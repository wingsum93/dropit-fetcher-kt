package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.SnapshotPayload
import com.ericho.dropit.model.entity.ProductEntity
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Instant

class SqliteStorage(private val dbPath: String) : Storage {
    private val json = Json { encodeDefaults = true }

    override fun upsert(detail: SingleProductPayload) {
        val snapshot = SnapshotPayload(
            key = detail.id,
            json = json.encodeToString(detail)
        )
        writeSnapshots(listOf(snapshot))
    }

    override fun upsert(product: ProductEntity) {
        writeProducts(listOf(product))
    }

    private fun writeSnapshots(snapshots: List<SnapshotPayload>) {
        if (snapshots.isEmpty()) {
            return
        }

        val jdbcUrl = "jdbc:sqlite:$dbPath"

        try {
            DriverManager.getConnection(jdbcUrl).use { connection ->
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
                ensureSnapshotsTable(ctx)

                val table = DSL.table("product_snapshots")
                val snapshotKey = DSL.field("snapshot_key", String::class.java)
                val payload = DSL.field("payload", String::class.java)
                val createdAt = DSL.field("created_at", Timestamp::class.java)
                val now = Timestamp.from(Instant.now())

                val queries = snapshots.map { snapshot ->
                    ctx.insertInto(table)
                        .columns(snapshotKey, payload, createdAt)
                        .values(snapshot.key, snapshot.json, now)
                }

                ctx.batch(queries).execute()
            }
        } catch (exception: Exception) {
            println("SQLite unavailable; skipping database persistence. (${exception::class.simpleName})")
        }
    }

    private fun writeProducts(products: List<ProductEntity>) {
        if (products.isEmpty()) {
            return
        }

        val jdbcUrl = "jdbc:sqlite:$dbPath"

        try {
            DriverManager.getConnection(jdbcUrl).use { connection ->
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
                ensureProductsTable(ctx)

                val table = DSL.table("products")
                val productId = DSL.field("product_id", Long::class.java)
                val createdAt = DSL.field("created_at", Timestamp::class.java)

                val queries = products.map { product ->
                    val timestamp = Timestamp.from(product.createdAt)
                    ctx.insertInto(table)
                        .columns(productId, createdAt)
                        .values(product.productId, timestamp)
                        .onConflict(productId)
                        .doUpdate()
                        .set(createdAt, timestamp)
                }

                ctx.batch(queries).execute()
            }
        } catch (exception: Exception) {
            println("SQLite unavailable; skipping product persistence. (${exception::class.simpleName})")
        }
    }

    private fun ensureSnapshotsTable(ctx: org.jooq.DSLContext) {
        val table = DSL.table("product_snapshots")
        val id = DSL.field("id", SQLDataType.INTEGER.nullable(false))
        val snapshotKey = DSL.field("snapshot_key", SQLDataType.VARCHAR(255).nullable(false))
        val payload = DSL.field("payload", SQLDataType.CLOB.nullable(false))
        val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMP.nullable(false))

        ctx.createTableIfNotExists(table)
            .column(id)
            .column(snapshotKey)
            .column(payload)
            .column(createdAt)
            .constraints(DSL.primaryKey(id))
            .execute()
    }

    private fun ensureProductsTable(ctx: org.jooq.DSLContext) {
        val table = DSL.table("products")
        val id = DSL.field("id", SQLDataType.INTEGER.nullable(false))
        val productId = DSL.field("product_id", SQLDataType.BIGINT.nullable(false))
        val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMP.nullable(false))

        ctx.createTableIfNotExists(table)
            .column(id)
            .column(productId)
            .column(createdAt)
            .constraints(DSL.primaryKey(id), DSL.unique(productId))
            .execute()
    }
}
