package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.DatabaseConfig
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.SnapshotPayload
import com.ericho.dropit.model.entity.ProductEntity
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jooq.JSONB
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Instant
import java.util.Properties

class PostgresqlStorage(private val config: DatabaseConfig) : Storage {
    private val json = Json { encodeDefaults = true }

    override fun upsert(detail: SingleProductPayload) {
        val snapshot = SnapshotPayload(
            key = detail.id,
            json = json.encodeToString(detail)
        )
        writeSnapshots(listOf(snapshot))
    }

    override fun upsert(product: ProductEntity) {
        // no-op for now
    }

    private fun writeSnapshots(snapshots: List<SnapshotPayload>) {
        if (snapshots.isEmpty()) {
            return
        }

        val jdbcUrl = "jdbc:postgresql://${config.host}:${config.port}/${config.database}"
        val props = Properties().apply {
            setProperty("user", config.user)
            setProperty("password", config.password)
        }

        try {
            DriverManager.getConnection(jdbcUrl, props).use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                ensureSnapshotsTable(ctx)

                val table = DSL.table("product_snapshots")
                val snapshotKey = DSL.field("snapshot_key", String::class.java)
                val payload = DSL.field("payload", SQLDataType.JSONB)
                val createdAt = DSL.field("created_at", Timestamp::class.java)
                val now = Timestamp.from(Instant.now())

                val queries = snapshots.map { snapshot ->
                    ctx.insertInto(table)
                        .columns(snapshotKey, payload, createdAt)
                        .values(snapshot.key, JSONB.jsonb(snapshot.json), now)
                }

                ctx.batch(queries).execute()
            }
        } catch (exception: Exception) {
            println("Postgres unavailable; skipping database persistence. (${exception::class.simpleName})")
        }
    }

    private fun ensureSnapshotsTable(ctx: org.jooq.DSLContext) {
        val table = DSL.table("product_snapshots")
        val id = DSL.field("id", SQLDataType.BIGINT.identity(true).nullable(false))
        val snapshotKey = DSL.field("snapshot_key", SQLDataType.VARCHAR(255).nullable(false))
        val payload = DSL.field("payload", SQLDataType.JSONB.nullable(false))
        val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

        ctx.createTableIfNotExists(table)
            .column(id)
            .column(snapshotKey)
            .column(payload)
            .column(createdAt)
            .constraints(DSL.primaryKey(id))
            .execute()
    }
}
