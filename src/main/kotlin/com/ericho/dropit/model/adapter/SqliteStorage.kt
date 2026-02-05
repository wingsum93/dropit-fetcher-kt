package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.SnapshotPayload
import com.ericho.dropit.model.entity.ProductEntity
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.sql.Timestamp
import java.time.Instant

class SqliteStorage(private val dbPath: String) : Storage {
    private val json = Json { encodeDefaults = true }
    private val snapshotsTable = DSL.table("product_snapshots")
    private val snapshotKeyField = DSL.field("snapshot_key", String::class.java)
    private val snapshotPayloadField = DSL.field("payload", String::class.java)
    private val snapshotCreatedAtField = DSL.field("created_at", Timestamp::class.java)

    private val productsTable = DSL.table("products")
    private val productRowIdField = DSL.field("id", Long::class.java)
    private val productIdField = DSL.field("product_id", Long::class.java)
    private val productStoreIdField = DSL.field("store_id", Int::class.java)
    private val productCategoryField = DSL.field("category", Int::class.java)
    private val productDepartmentIdField = DSL.field("department_id", Int::class.java)
    private val productUnitPriceField = DSL.field("unit_price", Double::class.java)
    private val productPopularityField = DSL.field("popularity", Int::class.java)
    private val productUpcField = DSL.field("upc", String::class.java)
    private val productNameField = DSL.field("name", String::class.java)
    private val productCanonicalUrlField = DSL.field("canonical_url", String::class.java)
    private val productRemoteLastUpdateAtField = DSL.field("remote_last_update_at", Timestamp::class.java)
    private val productCreatedAtField = DSL.field("created_at", Timestamp::class.java)

    private val dataSource: HikariDataSource = HikariDataSource(
        HikariConfig().apply {
            jdbcUrl = "jdbc:sqlite:$dbPath"
            driverClassName = "org.sqlite.JDBC"
            poolName = "dropit-sqlite-storage-pool"
            minimumIdle = 1
            maximumPoolSize = 1
            connectionTestQuery = "SELECT 1"
            initializationFailTimeout = 10_000
        }
    )

    init {
        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
                ensureSnapshotsSchema(ctx)
                ensureProductsSchema(ctx)
                reconcileProductsSchema(ctx)
            }
        } catch (exception: Exception) {
            fail(
                operation = "initSchema",
                key = "n/a",
                exception = exception
            )
        }
    }

    override fun upsertSnapshot(detail: SingleProductPayload) {
        val snapshot = SnapshotPayload(
            key = detail.id,
            json = json.encodeToString(detail)
        )
        writeSnapshot(snapshot)
    }

    override fun close() {
        dataSource.close()
    }

    private fun writeSnapshot(snapshot: SnapshotPayload) {
        val now = Timestamp.from(Instant.now())
        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
                ctx.insertInto(snapshotsTable)
                    .columns(snapshotKeyField, snapshotPayloadField, snapshotCreatedAtField)
                    .values(snapshot.key, snapshot.json, now)
                    .onConflict(snapshotKeyField)
                    .doUpdate()
                    .set(snapshotPayloadField, snapshot.json)
                    .set(snapshotCreatedAtField, now)
                    .execute()
            }
        } catch (exception: Exception) {
            fail(
                operation = "upsertSnapshot",
                key = snapshot.key,
                exception = exception
            )
        }
    }

    override fun findProductById(productId: Long): ProductEntity? {
        try {
            return dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
                ctx.selectFrom(productsTable)
                    .where(productIdField.eq(productId))
                    .fetchOne { mapProduct(it) }
            }
        } catch (exception: Exception) {
            fail(
                operation = "findProductById",
                key = productId.toString(),
                exception = exception
            )
        }
    }

    override fun findProductsNameIsEmpty(limit: Int): List<ProductEntity> {
        require(limit > 0) { "limit must be > 0" }

        try {
            return dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
                ctx.selectFrom(productsTable)
                    .where(
                        productNameField.isNull()
                            .or(DSL.trim(productNameField).eq(""))
                    )
                    .orderBy(productIdField.asc())
                    .limit(limit)
                    .fetch { mapProduct(it) }
            }
        } catch (exception: Exception) {
            fail(
                operation = "findProductsNameIsEmpty",
                key = limit.toString(),
                exception = exception
            )
        }
    }

    override fun findProductsSince(instant: Instant, limit: Int): List<ProductEntity> {
        require(limit > 0) { "limit must be > 0" }

        try {
            return dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
                ctx.selectFrom(productsTable)
                    .where(productRemoteLastUpdateAtField.ge(Timestamp.from(instant)))
                    .orderBy(
                        productRemoteLastUpdateAtField.desc(),
                        productIdField.desc()
                    )
                    .limit(limit)
                    .fetch { mapProduct(it) }
            }
        } catch (exception: Exception) {
            fail(
                operation = "findProductsSince",
                key = instant.toString(),
                exception = exception
            )
        }
    }

    override fun updateProduct(productId: Long, product: ProductEntity) {
        require(product.productId == productId) {
            "productId mismatch: arg=$productId payload=${product.productId}"
        }

        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
                val updated = ctx.update(productsTable)
                    .set(productStoreIdField, product.storeId)
                    .set(productCategoryField, product.category)
                    .set(productDepartmentIdField, product.departmentId)
                    .set(productUnitPriceField, product.unitPrice?.toDouble())
                    .set(productPopularityField, product.popularity)
                    .set(productUpcField, product.upc)
                    .set(productNameField, product.name)
                    .set(productCanonicalUrlField, product.canonicalUrl)
                    .set(productRemoteLastUpdateAtField, product.remoteLastUpdateAt?.let(Timestamp::from))
                    .where(productIdField.eq(productId))
                    .execute()

                if (updated == 0) {
                    throw StorageWriteException(
                        backend = "sqlite",
                        operation = "updateProduct",
                        key = productId.toString(),
                        cause = IllegalStateException("Product not found: $productId")
                    )
                }
            }
        } catch (exception: Exception) {
            if (exception is StorageWriteException) {
                throw exception
            }
            fail(
                operation = "updateProduct",
                key = productId.toString(),
                exception = exception
            )
        }
    }

    override fun createProductIfNotExist(list: List<Long>) {
        if (list.isEmpty()) return
        val productIds = list.distinct()
        val now = Timestamp.from(Instant.now())

        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
                val queries = productIds.map { productId ->
                    ctx.insertInto(productsTable)
                        .columns(productIdField, productCreatedAtField)
                        .values(productId, now)
                        .onConflict(productIdField)
                        .doNothing()
                }
                ctx.batch(queries).execute()
            }
        } catch (exception: Exception) {
            fail(
                operation = "createProductIfNotExist",
                key = productIds.size.toString(),
                exception = exception
            )
        }
    }

    private fun ensureSnapshotsSchema(ctx: DSLContext) {
        val id = DSL.field("id", SQLDataType.INTEGER.identity(true).nullable(false))
        val snapshotKey = DSL.field("snapshot_key", SQLDataType.VARCHAR(255).nullable(false))
        val payload = DSL.field("payload", SQLDataType.CLOB.nullable(false))
        val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMP.nullable(false))

        ctx.createTableIfNotExists(snapshotsTable)
            .column(id)
            .column(snapshotKey)
            .column(payload)
            .column(createdAt)
            .constraints(DSL.primaryKey(id))
            .execute()

        ctx.createUniqueIndexIfNotExists("ux_product_snapshots_snapshot_key")
            .on(snapshotsTable, snapshotKey)
            .execute()
    }

    private fun ensureProductsSchema(ctx: DSLContext) {
        val id = DSL.field("id", SQLDataType.INTEGER.identity(true).nullable(false))
        val productId = DSL.field("product_id", SQLDataType.BIGINT.nullable(false))
        val storeId = DSL.field("store_id", SQLDataType.INTEGER.nullable(true))
        val category = DSL.field("category", SQLDataType.INTEGER.nullable(true))
        val departmentId = DSL.field("department_id", SQLDataType.INTEGER.nullable(true))
        val unitPrice = DSL.field("unit_price", SQLDataType.DOUBLE.nullable(true))
        val popularity = DSL.field("popularity", SQLDataType.INTEGER.nullable(true))
        val upc = DSL.field("upc", SQLDataType.CLOB.nullable(true))
        val name = DSL.field("name", SQLDataType.CLOB.nullable(true))
        val canonicalUrl = DSL.field("canonical_url", SQLDataType.CLOB.nullable(true))
        val remoteLastUpdateAt = DSL.field("remote_last_update_at", SQLDataType.TIMESTAMP.nullable(true))
        val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMP.nullable(false))

        ctx.createTableIfNotExists(productsTable)
            .column(id)
            .column(productId)
            .column(storeId)
            .column(category)
            .column(departmentId)
            .column(unitPrice)
            .column(popularity)
            .column(upc)
            .column(name)
            .column(canonicalUrl)
            .column(remoteLastUpdateAt)
            .column(createdAt)
            .constraints(DSL.primaryKey(id))
            .execute()
    }

    private fun reconcileProductsSchema(ctx: DSLContext) {
        val existingColumns = ctx.fetch("PRAGMA table_info(products)")
            .mapNotNull { it.get("name", String::class.java)?.lowercase() }
            .toSet()

        addColumnIfMissing(ctx, existingColumns, "store_id", "INTEGER")
        addColumnIfMissing(ctx, existingColumns, "category", "INTEGER")
        addColumnIfMissing(ctx, existingColumns, "department_id", "INTEGER")
        addColumnIfMissing(ctx, existingColumns, "unit_price", "DOUBLE")
        addColumnIfMissing(ctx, existingColumns, "popularity", "INTEGER")
        addColumnIfMissing(ctx, existingColumns, "upc", "TEXT")
        addColumnIfMissing(ctx, existingColumns, "name", "TEXT")
        addColumnIfMissing(ctx, existingColumns, "canonical_url", "TEXT")
        addColumnIfMissing(ctx, existingColumns, "remote_last_update_at", "TIMESTAMP")
        addColumnIfMissing(ctx, existingColumns, "created_at", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP")

        ctx.createUniqueIndexIfNotExists("ux_products_product_id")
            .on(productsTable, productIdField)
            .execute()
        ctx.createIndexIfNotExists("idx_products_remote_last_update_at")
            .on(productsTable, productRemoteLastUpdateAtField)
            .execute()
    }

    private fun addColumnIfMissing(ctx: DSLContext, existingColumns: Set<String>, columnName: String, sqlType: String) {
        if (!existingColumns.contains(columnName.lowercase())) {
            ctx.execute("ALTER TABLE products ADD COLUMN $columnName $sqlType")
        }
    }

    private fun mapProduct(record: Record): ProductEntity {
        val createdAt = record.get(productCreatedAtField)?.toInstant() ?: Instant.EPOCH
        val rowId = record.get("id", Number::class.java)?.toLong()
        val productId = record.get("product_id", Number::class.java)?.toLong()
            ?: throw IllegalStateException("product_id is null")
        return ProductEntity(
            id = rowId,
            productId = productId,
            storeId = record.get(productStoreIdField),
            category = record.get(productCategoryField),
            departmentId = record.get(productDepartmentIdField),
            unitPrice = record.get(productUnitPriceField)?.toFloat(),
            popularity = record.get(productPopularityField),
            upc = record.get(productUpcField),
            name = record.get(productNameField),
            canonicalUrl = record.get(productCanonicalUrlField),
            remoteLastUpdateAt = record.get(productRemoteLastUpdateAtField)?.toInstant(),
            createdAt = createdAt
        )
    }

    private fun fail(operation: String, key: String, exception: Exception): Nothing {
        System.err.println(
            "event=storage_error backend=sqlite operation=$operation key=$key " +
                "error=${exception::class.simpleName} message=${exception.message}"
        )
        throw StorageWriteException(
            backend = "sqlite",
            operation = operation,
            key = key,
            cause = exception
        )
    }
}
