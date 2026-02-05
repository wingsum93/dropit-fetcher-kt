package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.entity.DepartmentEntity
import com.ericho.dropit.model.entity.JobEntity
import com.ericho.dropit.model.entity.JobStatus
import com.ericho.dropit.model.entity.JobType
import com.ericho.dropit.model.entity.ProductEntity
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.sql.Timestamp
import java.time.Instant

class SqliteStorage(private val dbPath: String) : Storage {
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

    private val managedTables = listOf("products", "departments", "jobs")

    private val expectedColumnsByTable: Map<String, Set<String>> = mapOf(
        "products" to setOf(
            "id",
            "store_id",
            "category",
            "department_id",
            "unit_price",
            "popularity",
            "upc",
            "name",
            "canonical_url",
            "remote_last_update_at",
            "created_at"
        ),
        "departments" to setOf(
            "id",
            "parent_department_id",
            "name",
            "path",
            "store_id",
            "count",
            "canonical_url",
            "created_at"
        ),
        "jobs" to setOf(
            "id",
            "sync_id",
            "job_type",
            "status",
            "created_at",
            "updated_at",
            "dedupe_key"
        )
    )

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
                resetAllManagedTablesIfSchemaStale(ctx)
                ensureProductsSchema(ctx)
                ensureDepartmentsSchema(ctx)
                ensureJobsSchema(ctx)
                ensureJobsTrigger(ctx)
            }
        } catch (exception: Exception) {
            fail(
                operation = "initSchema",
                key = "n/a",
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
            fail("findProductById", productId.toString(), exception)
        }
    }

    override fun findProductsNameIsEmpty(limit: Int): List<ProductEntity> {
        require(limit > 0) { "limit must be > 0" }
        try {
            return dataSource.connection.use { connection ->
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
                ctx.selectFrom(productsTable)
                    .where(productNameField.isNull.or(DSL.trim(productNameField).eq("")))
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
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
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
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
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
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
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
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
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
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
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
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
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
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
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
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
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
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
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
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
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
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
                val updated = ctx.update(jobsTable)
                    .set(jobStatusField, status.name)
                    .where(jobIdField.eq(id))
                    .execute()

                if (updated == 0) {
                    throw StorageWriteException(
                        backend = "sqlite",
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
                val ctx = DSL.using(connection, SQLDialect.SQLITE)
                val updated = ctx.update(jobsTable)
                    .set(jobStatusField, status.name)
                    .where(jobIdField.`in`(distinctIds))
                    .execute()

                if (updated != distinctIds.size) {
                    throw StorageWriteException(
                        backend = "sqlite",
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

    private fun resetAllManagedTablesIfSchemaStale(ctx: DSLContext) {
        if (!isSchemaStale(ctx)) return

        ctx.execute("DROP TRIGGER IF EXISTS trg_jobs_updated_at")
        ctx.execute("DROP TABLE IF EXISTS jobs")
        ctx.execute("DROP TABLE IF EXISTS departments")
        ctx.execute("DROP TABLE IF EXISTS products")
    }

    private fun isSchemaStale(ctx: DSLContext): Boolean {
        val existingTableCount = managedTables.count { tableExists(ctx, it) }
        if (existingTableCount == 0) return false
        if (existingTableCount != managedTables.size) return true

        return managedTables.any { tableName ->
            currentColumns(ctx, tableName) != expectedColumnsByTable.getValue(tableName)
        }
    }

    private fun tableExists(ctx: DSLContext, tableName: String): Boolean {
        val count = ctx.fetchOne(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
            tableName
        )?.get(0, Int::class.java) ?: 0
        return count > 0
    }

    private fun currentColumns(ctx: DSLContext, tableName: String): Set<String> {
        return ctx.fetch("PRAGMA table_info($tableName)")
            .mapNotNull { it.get("name", String::class.java)?.lowercase() }
            .toSet()
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
        val remoteLastUpdateAt = DSL.field("remote_last_update_at", SQLDataType.TIMESTAMP.nullable(true))
        val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMP.nullable(false))

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
        val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMP.nullable(false))

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

    private fun ensureJobsSchema(ctx: DSLContext) {
        val id = DSL.field("id", SQLDataType.INTEGER.identity(true).nullable(false))
        val syncId = DSL.field("sync_id", SQLDataType.INTEGER.nullable(false))
        val jobType = DSL.field("job_type", SQLDataType.VARCHAR(64).nullable(false))
        val status = DSL.field("status", SQLDataType.VARCHAR(32).nullable(false))
        val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMP.nullable(false))
        val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMP.nullable(false))
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
            CREATE TRIGGER IF NOT EXISTS trg_jobs_updated_at
            AFTER UPDATE ON jobs
            FOR EACH ROW
            WHEN NEW.updated_at = OLD.updated_at
            BEGIN
                UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
            END;
            """.trimIndent()
        )
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
        val id = record.get("id", Number::class.java)?.toInt()
        return JobEntity(
            id = id,
            syncId = record.get(jobSyncIdField) ?: 0,
            jobType = JobType.valueOf(record.get(jobTypeField) ?: JobType.FETCH_PRODUCT.name),
            status = JobStatus.valueOf(record.get(jobStatusField) ?: JobStatus.PENDING.name),
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
