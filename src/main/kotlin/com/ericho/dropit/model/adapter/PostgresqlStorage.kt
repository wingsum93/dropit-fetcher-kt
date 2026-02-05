package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.DatabaseConfig
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.SnapshotPayload
import com.ericho.dropit.model.entity.DepartmentEntity
import com.ericho.dropit.model.entity.JobEntity
import com.ericho.dropit.model.entity.JobStatus
import com.ericho.dropit.model.entity.JobType
import com.ericho.dropit.model.entity.ProductEntity
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.sql.Timestamp
import java.time.Instant

class PostgresqlStorage(private val config: DatabaseConfig) : Storage {
    private val json = Json { encodeDefaults = true }

    private val snapshotsTable = DSL.table("product_snapshots")
    private val snapshotKeyField = DSL.field("snapshot_key", String::class.java)
    private val snapshotPayloadField = DSL.field("payload", SQLDataType.JSONB)
    private val snapshotCreatedAtField = DSL.field("created_at", Timestamp::class.java)

    private val productsTable = DSL.table("products")
    private val productIdField = DSL.field("id", Long::class.java)
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

    private val departmentsTable = DSL.table("departments")
    private val departmentIdField = DSL.field("id", Int::class.java)
    private val departmentParentIdField = DSL.field("parent_department_id", Int::class.java)
    private val departmentNameField = DSL.field("name", String::class.java)
    private val departmentPathField = DSL.field("path", String::class.java)
    private val departmentStoreIdField = DSL.field("store_id", Int::class.java)
    private val departmentCountField = DSL.field("count", Int::class.java)
    private val departmentCanonicalUrlField = DSL.field("canonical_url", String::class.java)
    private val departmentCreatedAtField = DSL.field("created_at", Timestamp::class.java)

    private val jobsTable = DSL.table("jobs")
    private val jobIdField = DSL.field("id", Int::class.java)
    private val jobSyncIdField = DSL.field("sync_id", Int::class.java)
    private val jobTypeField = DSL.field("job_type", String::class.java)
    private val jobStatusField = DSL.field("status", String::class.java)
    private val jobCreatedAtField = DSL.field("created_at", Timestamp::class.java)
    private val jobUpdatedAtField = DSL.field("updated_at", Timestamp::class.java)
    private val jobDedupeKeyField = DSL.field("dedupe_key", String::class.java)

    private val dataSource: HikariDataSource = HikariDataSource(
        HikariConfig().apply {
            jdbcUrl = "jdbc:postgresql://${config.host}:${config.port}/${config.database}"
            driverClassName = "org.postgresql.Driver"
            username = config.user
            password = config.password
            poolName = "dropit-postgres-storage-pool"
            minimumIdle = 1
            maximumPoolSize = 8
            connectionTestQuery = "SELECT 1"
            initializationFailTimeout = 10_000
        }
    )

    init {
        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                ensureSnapshotsSchema(ctx)
                ensureProductsSchema(ctx)
                reconcileProductsSchema(ctx)
                ensureDepartmentsSchema(ctx)
                reconcileDepartmentsSchema(ctx)
                ensureJobsSchema(ctx)
                reconcileJobsSchema(ctx)
                ensureJobsTrigger(ctx)
            }
        } catch (exception: Exception) {
            fail("initSchema", "n/a", exception)
        }
    }

    override fun upsertSnapshot(detail: SingleProductPayload) {
        val snapshot = SnapshotPayload(
            key = detail.id,
            json = json.encodeToString(detail)
        )
        writeSnapshot(snapshot)
    }

    override fun findProductById(productId: Long): ProductEntity? {
        try {
            return dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                ctx.selectFrom(productsTable)
                    .where(productIdField.eq(productId))
                    .fetchOne { mapProduct(it) }
            }
        } catch (exception: Exception) {
            fail("findProductById", productId.toString(), exception)
        }
    }

    override fun findProductsNameIsEmpty(limit: Int): List<ProductEntity> {
        require(limit > 0) { "limit must be > 0" }

        try {
            return dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                ctx.selectFrom(productsTable)
                    .where(
                        productNameField.isNull
                            .or(DSL.function("btrim", String::class.java, productNameField).eq(""))
                    )
                    .orderBy(productIdField.asc())
                    .limit(limit)
                    .fetch { mapProduct(it) }
            }
        } catch (exception: Exception) {
            fail("findProductsNameIsEmpty", limit.toString(), exception)
        }
    }

    override fun findProductsSince(instant: Instant, limit: Int): List<ProductEntity> {
        require(limit > 0) { "limit must be > 0" }

        try {
            return dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                ctx.selectFrom(productsTable)
                    .where(productRemoteLastUpdateAtField.ge(Timestamp.from(instant)))
                    .orderBy(productRemoteLastUpdateAtField.desc(), productIdField.desc())
                    .limit(limit)
                    .fetch { mapProduct(it) }
            }
        } catch (exception: Exception) {
            fail("findProductsSince", instant.toString(), exception)
        }
    }

    override fun updateProduct(productId: Long, product: ProductEntity) {
        require(product.id == productId) {
            "productId mismatch: arg=$productId payload=${product.id}"
        }

        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
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
                        backend = "postgres",
                        operation = "updateProduct",
                        key = productId.toString(),
                        cause = IllegalStateException("Product not found: $productId")
                    )
                }
            }
        } catch (exception: Exception) {
            if (exception is StorageWriteException) throw exception
            fail("updateProduct", productId.toString(), exception)
        }
    }

    override fun createProductIfNotExist(list: List<Long>) {
        if (list.isEmpty()) return

        val ids = list.distinct()
        val now = Timestamp.from(Instant.now())

        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                val queries = ids.map { id ->
                    ctx.insertInto(productsTable)
                        .columns(productIdField, productCreatedAtField)
                        .values(id, now)
                        .onConflict(productIdField)
                        .doNothing()
                }
                ctx.batch(queries).execute()
            }
        } catch (exception: Exception) {
            fail("createProductIfNotExist", ids.size.toString(), exception)
        }
    }

    override fun insertDepartmentEntity(list: List<DepartmentEntity>) {
        if (list.isEmpty()) return

        val chunks = list.chunked(300)
        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                chunks.forEach { chunk ->
                    val queries = chunk.map { department ->
                        ctx.insertInto(departmentsTable)
                            .columns(
                                departmentIdField,
                                departmentParentIdField,
                                departmentNameField,
                                departmentPathField,
                                departmentStoreIdField,
                                departmentCountField,
                                departmentCanonicalUrlField,
                                departmentCreatedAtField
                            )
                            .values(
                                department.id,
                                department.parentDepartmentId,
                                department.name,
                                department.path,
                                department.storeId,
                                department.count,
                                department.canonicalUrl,
                                Timestamp.from(department.createdAt)
                            )
                            .onConflict(departmentIdField)
                            .doNothing()
                    }
                    ctx.batch(queries).execute()
                }
            }
        } catch (exception: Exception) {
            fail("insertDepartmentEntity", list.size.toString(), exception)
        }
    }

    override fun findDepartmentById(id: Int): DepartmentEntity? {
        try {
            return dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                ctx.selectFrom(departmentsTable)
                    .where(departmentIdField.eq(id))
                    .fetchOne { mapDepartment(it) }
            }
        } catch (exception: Exception) {
            fail("findDepartmentById", id.toString(), exception)
        }
    }

    override fun findAllDepartments(): List<DepartmentEntity> {
        try {
            return dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                ctx.selectFrom(departmentsTable)
                    .orderBy(departmentIdField.asc())
                    .fetch { mapDepartment(it) }
            }
        } catch (exception: Exception) {
            fail("findAllDepartments", "all", exception)
        }
    }

    override fun insertJobsIfNotExist(jobs: List<JobEntity>) {
        if (jobs.isEmpty()) return
        jobs.forEach { validateDedupeKey(it.dedupeKey) }

        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                val queries = jobs.map { job ->
                    ctx.insertInto(jobsTable)
                        .columns(
                            jobSyncIdField,
                            jobTypeField,
                            jobStatusField,
                            jobCreatedAtField,
                            jobUpdatedAtField,
                            jobDedupeKeyField
                        )
                        .values(
                            job.syncId,
                            job.jobType.name,
                            job.status.name,
                            Timestamp.from(job.createdAt),
                            Timestamp.from(job.updatedAt),
                            job.dedupeKey
                        )
                        .onConflict(jobSyncIdField, jobDedupeKeyField)
                        .doNothing()
                }
                ctx.batch(queries).execute()
            }
        } catch (exception: Exception) {
            fail("insertJobsIfNotExist", jobs.size.toString(), exception)
        }
    }

    override fun findJobsByType(type: JobType, status: JobStatus): List<JobEntity> {
        try {
            return dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                ctx.selectFrom(jobsTable)
                    .where(jobTypeField.eq(type.name).and(jobStatusField.eq(status.name)))
                    .orderBy(jobIdField.asc())
                    .fetch { mapJob(it) }
            }
        } catch (exception: Exception) {
            fail("findJobsByType", "${type.name}:${status.name}", exception)
        }
    }

    override fun findJobsByType(syncId: Int, type: JobType, status: JobStatus): List<JobEntity> {
        try {
            return dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                ctx.selectFrom(jobsTable)
                    .where(
                        jobSyncIdField.eq(syncId)
                            .and(jobTypeField.eq(type.name))
                            .and(jobStatusField.eq(status.name))
                    )
                    .orderBy(jobIdField.asc())
                    .fetch { mapJob(it) }
            }
        } catch (exception: Exception) {
            fail("findJobsByType", "$syncId:${type.name}:${status.name}", exception)
        }
    }

    override fun findDepartmentJobsById(id: Int): List<JobEntity> {
        return listOfNotNull(findByDedupeKey("dept:$id"))
    }

    override fun findDepartmentJobsById(syncId: Int, id: Int): List<JobEntity> {
        return listOfNotNull(findByDedupeKey(syncId, "dept:$id"))
    }

    override fun findByDedupeKey(key: String): JobEntity? {
        validateDedupeKey(key)

        try {
            return dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                ctx.selectFrom(jobsTable)
                    .where(jobDedupeKeyField.eq(key))
                    .orderBy(jobIdField.desc())
                    .limit(1)
                    .fetchOne { mapJob(it) }
            }
        } catch (exception: Exception) {
            fail("findByDedupeKey", key, exception)
        }
    }

    override fun findByDedupeKey(syncId: Int, key: String): JobEntity? {
        validateDedupeKey(key)

        try {
            return dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                ctx.selectFrom(jobsTable)
                    .where(jobSyncIdField.eq(syncId).and(jobDedupeKeyField.eq(key)))
                    .orderBy(jobIdField.desc())
                    .limit(1)
                    .fetchOne { mapJob(it) }
            }
        } catch (exception: Exception) {
            fail("findByDedupeKey", "$syncId:$key", exception)
        }
    }

    override fun updateJobStatusById(id: Int, status: JobStatus) {
        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                val updated = ctx.update(jobsTable)
                    .set(jobStatusField, status.name)
                    .where(jobIdField.eq(id))
                    .execute()

                if (updated == 0) {
                    throw StorageWriteException(
                        backend = "postgres",
                        operation = "updateJobStatusById",
                        key = id.toString(),
                        cause = IllegalStateException("Job not found: $id")
                    )
                }
            }
        } catch (exception: Exception) {
            if (exception is StorageWriteException) throw exception
            fail("updateJobStatusById", id.toString(), exception)
        }
    }

    override fun updateJobStatusByIds(ids: List<Int>, status: JobStatus) {
        if (ids.isEmpty()) return
        val distinctIds = ids.distinct()

        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                val updated = ctx.update(jobsTable)
                    .set(jobStatusField, status.name)
                    .where(jobIdField.`in`(distinctIds))
                    .execute()

                if (updated != distinctIds.size) {
                    throw StorageWriteException(
                        backend = "postgres",
                        operation = "updateJobStatusByIds",
                        key = "expected=${distinctIds.size},updated=$updated",
                        cause = IllegalStateException("Not all jobs found for bulk status update")
                    )
                }
            }
        } catch (exception: Exception) {
            if (exception is StorageWriteException) throw exception
            fail("updateJobStatusByIds", distinctIds.size.toString(), exception)
        }
    }

    override fun close() {
        dataSource.close()
    }

    private fun writeSnapshot(snapshot: SnapshotPayload) {
        val now = Timestamp.from(Instant.now())
        try {
            dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.POSTGRES)
                ctx.insertInto(snapshotsTable)
                    .columns(snapshotKeyField, snapshotPayloadField, snapshotCreatedAtField)
                    .values(snapshot.key, JSONB.jsonb(snapshot.json), now)
                    .onConflict(snapshotKeyField)
                    .doUpdate()
                    .set(snapshotPayloadField, JSONB.jsonb(snapshot.json))
                    .set(snapshotCreatedAtField, now)
                    .execute()
            }
        } catch (exception: Exception) {
            fail("upsertSnapshot", snapshot.key, exception)
        }
    }

    private fun ensureSnapshotsSchema(ctx: DSLContext) {
        val id = DSL.field("id", SQLDataType.BIGINT.identity(true).nullable(false))
        val snapshotKey = DSL.field("snapshot_key", SQLDataType.VARCHAR(255).nullable(false))
        val payload = DSL.field("payload", SQLDataType.JSONB.nullable(false))
        val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

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
        val id = DSL.field("id", SQLDataType.BIGINT.nullable(false))
        val storeId = DSL.field("store_id", SQLDataType.INTEGER.nullable(true))
        val category = DSL.field("category", SQLDataType.INTEGER.nullable(true))
        val departmentId = DSL.field("department_id", SQLDataType.INTEGER.nullable(true))
        val unitPrice = DSL.field("unit_price", SQLDataType.DOUBLE.nullable(true))
        val popularity = DSL.field("popularity", SQLDataType.INTEGER.nullable(true))
        val upc = DSL.field("upc", SQLDataType.CLOB.nullable(true))
        val name = DSL.field("name", SQLDataType.CLOB.nullable(true))
        val canonicalUrl = DSL.field("canonical_url", SQLDataType.CLOB.nullable(true))
        val remoteLastUpdateAt = DSL.field("remote_last_update_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true))
        val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

        ctx.createTableIfNotExists(productsTable)
            .column(id)
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
        if (hasColumn(ctx, "products", "product_id")) {
            migrateLegacyProductsTable(ctx)
            return
        }

        ctx.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS store_id INTEGER")
        ctx.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS category INTEGER")
        ctx.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS department_id INTEGER")
        ctx.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS unit_price DOUBLE PRECISION")
        ctx.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS popularity INTEGER")
        ctx.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS upc TEXT")
        ctx.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS name TEXT")
        ctx.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS canonical_url TEXT")
        ctx.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS remote_last_update_at TIMESTAMPTZ")
        ctx.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")

        ctx.createIndexIfNotExists("idx_products_remote_last_update_at")
            .on(productsTable, productRemoteLastUpdateAtField)
            .execute()
    }

    private fun migrateLegacyProductsTable(ctx: DSLContext) {
        ctx.execute(
            """
            CREATE TABLE IF NOT EXISTS products_new (
                id BIGINT PRIMARY KEY,
                store_id INTEGER NULL,
                category INTEGER NULL,
                department_id INTEGER NULL,
                unit_price DOUBLE PRECISION NULL,
                popularity INTEGER NULL,
                upc TEXT NULL,
                name TEXT NULL,
                canonical_url TEXT NULL,
                remote_last_update_at TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """.trimIndent()
        )

        ctx.execute(
            """
            INSERT INTO products_new (
                id, store_id, category, department_id, unit_price, popularity, upc, name,
                canonical_url, remote_last_update_at, created_at
            )
            SELECT
                CAST(product_id AS BIGINT),
                store_id,
                category,
                department_id,
                unit_price,
                popularity,
                upc,
                name,
                canonical_url,
                remote_last_update_at,
                COALESCE(created_at, NOW())
            FROM products
            WHERE product_id IS NOT NULL
            ON CONFLICT (id) DO NOTHING
            """.trimIndent()
        )

        ctx.execute("DROP TABLE products")
        ctx.execute("ALTER TABLE products_new RENAME TO products")

        ctx.createIndexIfNotExists("idx_products_remote_last_update_at")
            .on(productsTable, productRemoteLastUpdateAtField)
            .execute()
    }

    private fun ensureDepartmentsSchema(ctx: DSLContext) {
        val id = DSL.field("id", SQLDataType.INTEGER.nullable(false))
        val parentDepartmentId = DSL.field("parent_department_id", SQLDataType.INTEGER.nullable(true))
        val name = DSL.field("name", SQLDataType.CLOB.nullable(false))
        val path = DSL.field("path", SQLDataType.CLOB.nullable(false))
        val storeId = DSL.field("store_id", SQLDataType.INTEGER.nullable(false))
        val count = DSL.field("count", SQLDataType.INTEGER.nullable(false))
        val canonicalUrl = DSL.field("canonical_url", SQLDataType.CLOB.nullable(false))
        val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))

        ctx.createTableIfNotExists(departmentsTable)
            .column(id)
            .column(parentDepartmentId)
            .column(name)
            .column(path)
            .column(storeId)
            .column(count)
            .column(canonicalUrl)
            .column(createdAt)
            .constraints(DSL.primaryKey(id))
            .execute()
    }

    private fun reconcileDepartmentsSchema(ctx: DSLContext) {
        if (hasColumn(ctx, "departments", "department_id")) {
            migrateLegacyDepartmentsTable(ctx)
            return
        }

        ctx.execute("ALTER TABLE departments ADD COLUMN IF NOT EXISTS parent_department_id INTEGER")
        ctx.execute("ALTER TABLE departments ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT ''")
        ctx.execute("ALTER TABLE departments ADD COLUMN IF NOT EXISTS path TEXT NOT NULL DEFAULT ''")
        ctx.execute("ALTER TABLE departments ADD COLUMN IF NOT EXISTS store_id INTEGER NOT NULL DEFAULT 0")
        ctx.execute("ALTER TABLE departments ADD COLUMN IF NOT EXISTS count INTEGER NOT NULL DEFAULT 0")
        ctx.execute("ALTER TABLE departments ADD COLUMN IF NOT EXISTS canonical_url TEXT NOT NULL DEFAULT ''")
        ctx.execute("ALTER TABLE departments ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
    }

    private fun migrateLegacyDepartmentsTable(ctx: DSLContext) {
        ctx.execute(
            """
            CREATE TABLE IF NOT EXISTS departments_new (
                id INTEGER PRIMARY KEY,
                parent_department_id INTEGER NULL,
                name TEXT NOT NULL,
                path TEXT NOT NULL,
                store_id INTEGER NOT NULL,
                count INTEGER NOT NULL,
                canonical_url TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """.trimIndent()
        )

        ctx.execute(
            """
            INSERT INTO departments_new (
                id, parent_department_id, name, path, store_id, count, canonical_url, created_at
            )
            SELECT
                CAST(department_id AS INTEGER),
                parent_department_id,
                COALESCE(name, ''),
                COALESCE(path, ''),
                COALESCE(store_id, 0),
                COALESCE(count, 0),
                COALESCE(canonical_url, ''),
                COALESCE(created_at, NOW())
            FROM departments
            WHERE department_id IS NOT NULL
            ON CONFLICT (id) DO NOTHING
            """.trimIndent()
        )

        ctx.execute("DROP TABLE departments")
        ctx.execute("ALTER TABLE departments_new RENAME TO departments")
    }

    private fun ensureJobsSchema(ctx: DSLContext) {
        val id = DSL.field("id", SQLDataType.INTEGER.identity(true).nullable(false))
        val syncId = DSL.field("sync_id", SQLDataType.INTEGER.nullable(false))
        val jobType = DSL.field("job_type", SQLDataType.VARCHAR(64).nullable(false))
        val status = DSL.field("status", SQLDataType.VARCHAR(32).nullable(false))
        val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
        val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false))
        val dedupeKey = DSL.field("dedupe_key", SQLDataType.VARCHAR(40).nullable(false))

        ctx.createTableIfNotExists(jobsTable)
            .column(id)
            .column(syncId)
            .column(jobType)
            .column(status)
            .column(createdAt)
            .column(updatedAt)
            .column(dedupeKey)
            .constraints(DSL.primaryKey(id))
            .execute()
    }

    private fun reconcileJobsSchema(ctx: DSLContext) {
        ctx.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS sync_id INTEGER NOT NULL DEFAULT 0")
        ctx.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS job_type TEXT NOT NULL DEFAULT 'FETCH_PRODUCT'")
        ctx.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'PENDING'")
        ctx.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
        ctx.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
        ctx.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS dedupe_key VARCHAR(40) NOT NULL DEFAULT ''")

        ctx.execute("UPDATE jobs SET status = 'IN_PROGRESS' WHERE status = 'INPROGRESS'")

        ctx.createUniqueIndexIfNotExists("ux_jobs_sync_id_dedupe_key")
            .on(jobsTable, jobSyncIdField, jobDedupeKeyField)
            .execute()
        ctx.createIndexIfNotExists("idx_jobs_type_status_id")
            .on(jobsTable, jobTypeField, jobStatusField, jobIdField)
            .execute()
        ctx.createIndexIfNotExists("idx_jobs_sync_type_status_id")
            .on(jobsTable, jobSyncIdField, jobTypeField, jobStatusField, jobIdField)
            .execute()
        ctx.createIndexIfNotExists("idx_jobs_dedupe_key_id")
            .on(jobsTable, jobDedupeKeyField, jobIdField)
            .execute()
        ctx.createIndexIfNotExists("idx_jobs_sync_dedupe_key_id")
            .on(jobsTable, jobSyncIdField, jobDedupeKeyField, jobIdField)
            .execute()
    }

    private fun ensureJobsTrigger(ctx: DSLContext) {
        ctx.execute(
            """
            CREATE OR REPLACE FUNCTION set_jobs_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """.trimIndent()
        )

        ctx.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_jobs_updated_at') THEN
                    CREATE TRIGGER trg_jobs_updated_at
                    BEFORE UPDATE ON jobs
                    FOR EACH ROW
                    EXECUTE FUNCTION set_jobs_updated_at();
                END IF;
            END $$;
            """.trimIndent()
        )
    }

    private fun hasColumn(ctx: DSLContext, tableName: String, columnName: String): Boolean {
        val count = ctx.fetchOne(
            """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ?
              AND column_name = ?
            """.trimIndent(),
            tableName,
            columnName
        )?.get(0, Int::class.java) ?: 0
        return count > 0
    }

    private fun mapProduct(record: Record): ProductEntity {
        val id = record.get("id", Number::class.java)?.toLong()
            ?: throw IllegalStateException("id is null")

        return ProductEntity(
            id = id,
            storeId = record.get(productStoreIdField),
            category = record.get(productCategoryField),
            departmentId = record.get(productDepartmentIdField),
            unitPrice = record.get(productUnitPriceField)?.toFloat(),
            popularity = record.get(productPopularityField),
            upc = record.get(productUpcField),
            name = record.get(productNameField),
            canonicalUrl = record.get(productCanonicalUrlField),
            remoteLastUpdateAt = record.get(productRemoteLastUpdateAtField)?.toInstant(),
            createdAt = record.get(productCreatedAtField)?.toInstant() ?: Instant.EPOCH
        )
    }

    private fun mapDepartment(record: Record): DepartmentEntity {
        val id = record.get("id", Number::class.java)?.toInt()
            ?: throw IllegalStateException("department id is null")

        return DepartmentEntity(
            id = id,
            parentDepartmentId = record.get(departmentParentIdField),
            name = record.get(departmentNameField) ?: "",
            path = record.get(departmentPathField) ?: "",
            storeId = record.get(departmentStoreIdField) ?: 0,
            count = record.get(departmentCountField) ?: 0,
            canonicalUrl = record.get(departmentCanonicalUrlField) ?: "",
            createdAt = record.get(departmentCreatedAtField)?.toInstant() ?: Instant.EPOCH
        )
    }

    private fun mapJob(record: Record): JobEntity {
        val rawStatus = (record.get(jobStatusField) ?: JobStatus.PENDING.name)
            .replace("INPROGRESS", "IN_PROGRESS")

        return JobEntity(
            id = record.get("id", Number::class.java)?.toInt(),
            syncId = record.get(jobSyncIdField) ?: 0,
            jobType = JobType.valueOf(record.get(jobTypeField) ?: JobType.FETCH_PRODUCT.name),
            status = JobStatus.valueOf(rawStatus),
            dedupeKey = record.get(jobDedupeKeyField) ?: "",
            createdAt = record.get(jobCreatedAtField)?.toInstant() ?: Instant.EPOCH,
            updatedAt = record.get(jobUpdatedAtField)?.toInstant() ?: Instant.EPOCH
        )
    }

    private fun validateDedupeKey(key: String) {
        require(key.length <= 40) { "dedupeKey length must be <= 40" }
    }

    private fun fail(operation: String, key: String, exception: Exception): Nothing {
        System.err.println(
            "event=storage_error backend=postgres operation=$operation key=$key " +
                "error=${exception::class.simpleName} message=${exception.message}"
        )
        throw StorageWriteException(
            backend = "postgres",
            operation = operation,
            key = key,
            cause = exception
        )
    }
}
