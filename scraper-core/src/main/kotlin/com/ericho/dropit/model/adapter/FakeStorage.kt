package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.entity.DepartmentEntity
import com.ericho.dropit.model.entity.JobEntity
import com.ericho.dropit.model.entity.JobStatus
import com.ericho.dropit.model.entity.JobType
import com.ericho.dropit.model.entity.ProductEntity
import com.ericho.dropit.model.entity.SyncEntity
import com.ericho.dropit.model.entity.SyncStatus
import java.time.Instant

class FakeStorage : Storage {
    private val productsById: MutableMap<Long, ProductEntity> = mutableMapOf()
    private val departmentsById: MutableMap<Int, DepartmentEntity> = mutableMapOf()
    private val jobsById: MutableMap<Int, JobEntity> = mutableMapOf()
    private val syncsById: MutableMap<Int, SyncEntity> = mutableMapOf()
    private val rateLimitsByName: MutableMap<String, Instant> = mutableMapOf()
    private var nextJobId: Int = 1
    private var nextSyncId: Int = 1

    override fun findProductById(productId: Long): ProductEntity? {
        return productsById[productId]
    }

    override fun findProducts(limit: Int): List<ProductEntity> {
        require(limit > 0) { "limit must be > 0" }
        return productsById.values
            .sortedWith(compareByDescending<ProductEntity> { it.remoteLastUpdateAt ?: it.createdAt }.thenByDescending { it.id })
            .take(limit)
    }

    override fun findProductsNameIsEmpty(limit: Int): List<ProductEntity> {
        require(limit > 0) { "limit must be > 0" }
        return productsById.values
            .asSequence()
            .filter { it.name.isNullOrBlank() }
            .sortedBy { it.id }
            .take(limit)
            .toList()
    }

    override fun findProductsSince(instant: Instant, limit: Int): List<ProductEntity> {
        require(limit > 0) { "limit must be > 0" }
        return productsById.values
            .asSequence()
            .filter { it.remoteLastUpdateAt != null && !it.remoteLastUpdateAt.isBefore(instant) }
            .sortedWith(compareByDescending<ProductEntity> { it.remoteLastUpdateAt }.thenByDescending { it.id })
            .take(limit)
            .toList()
    }

    override fun updateProduct(productId: Long, product: ProductEntity) {
        require(product.id == productId) {
            "productId mismatch: arg=$productId payload=${product.id}"
        }

        if (!productsById.containsKey(productId)) {
            throw StorageWriteException(
                backend = "fake",
                operation = "updateProduct",
                key = productId.toString(),
                cause = IllegalStateException("Product not found: $productId")
            )
        }
        productsById[productId] = product
    }

    override fun createProductIfNotExist(list: List<Long>) {
        if (list.isEmpty()) return

        list.distinct().forEach { productId ->
            productsById.putIfAbsent(
                productId,
                ProductEntity(id = productId)
            )
        }
    }

    override fun insertDepartmentEntity(list: List<DepartmentEntity>) {
        if (list.isEmpty()) return

        list.forEach { department ->
            departmentsById.putIfAbsent(department.id, department)
        }
    }

    override fun findDepartmentById(id: Int): DepartmentEntity? {
        return departmentsById[id]
    }

    override fun findAllDepartments(): List<DepartmentEntity> {
        return departmentsById.values.sortedBy { it.id }
    }

    override fun insertJobsIfNotExist(jobs: List<JobEntity>) {
        if (jobs.isEmpty()) return

        jobs.forEach { validateDedupeKey(it.dedupeKey) }

        jobs.forEach { job ->
            val exists = jobsById.values.any { it.syncId == job.syncId && it.dedupeKey == job.dedupeKey }
            if (!exists) {
                val jobId = job.id ?: nextJobId++
                if (jobId >= nextJobId) {
                    nextJobId = jobId + 1
                }
                jobsById[jobId] = job.copy(id = jobId)
            }
        }
    }

    override fun findJobsByType(type: JobType, status: JobStatus): List<JobEntity> {
        return jobsById.values
            .asSequence()
            .filter { it.jobType == type && it.status == status }
            .sortedBy { it.id }
            .toList()
    }

    override fun findJobsByType(syncId: Int, type: JobType, status: JobStatus): List<JobEntity> {
        return jobsById.values
            .asSequence()
            .filter { it.syncId == syncId && it.jobType == type && it.status == status }
            .sortedBy { it.id }
            .toList()
    }

    override fun findJobsBySync(syncId: Int, status: JobStatus?, type: JobType?): List<JobEntity> {
        return jobsById.values
            .asSequence()
            .filter { it.syncId == syncId }
            .filter { status == null || it.status == status }
            .filter { type == null || it.jobType == type }
            .sortedBy { it.id }
            .toList()
    }

    override fun claimPendingJobs(limit: Int): List<JobEntity> {
        require(limit > 0) { "limit must be > 0" }
        val now = Instant.now()
        val claimedIds = jobsById.values
            .asSequence()
            .filter { it.status == JobStatus.PENDING }
            .sortedBy { it.id }
            .take(limit)
            .mapNotNull { it.id }
            .toList()

        return claimedIds.map { id ->
            val claimed = jobsById.getValue(id).copy(status = JobStatus.IN_PROGRESS, updatedAt = now)
            jobsById[id] = claimed
            claimed
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
        return jobsById.values
            .asSequence()
            .filter { it.dedupeKey == key }
            .maxByOrNull { it.id ?: Int.MIN_VALUE }
    }

    override fun findByDedupeKey(syncId: Int, key: String): JobEntity? {
        validateDedupeKey(key)
        return jobsById.values
            .asSequence()
            .filter { it.syncId == syncId && it.dedupeKey == key }
            .maxByOrNull { it.id ?: Int.MIN_VALUE }
    }

    override fun updateJobStatusById(id: Int, status: JobStatus) {
        val existing = jobsById[id] ?: throw StorageWriteException(
            backend = "fake",
            operation = "updateJobStatusById",
            key = id.toString(),
            cause = IllegalStateException("Job not found: $id")
        )

        jobsById[id] = existing.copy(
            status = status,
            updatedAt = Instant.now()
        )
    }

    override fun updateJobStatusByIds(ids: List<Int>, status: JobStatus) {
        if (ids.isEmpty()) return

        val distinct = ids.distinct()
        if (!jobsById.keys.containsAll(distinct)) {
            throw StorageWriteException(
                backend = "fake",
                operation = "updateJobStatusByIds",
                key = "expected=${distinct.size}",
                cause = IllegalStateException("Not all jobs found for bulk status update")
            )
        }

        val now = Instant.now()
        distinct.forEach { id ->
            val existing = jobsById.getValue(id)
            jobsById[id] = existing.copy(status = status, updatedAt = now)
        }
    }

    override fun findSyncEntityLeft(): SyncEntity? {
        return syncsById.values
            .asSequence()
            .filter { it.status == SyncStatus.RUNNING }
            .maxByOrNull { it.id ?: Int.MIN_VALUE }
    }

    override fun findSyncById(id: Int): SyncEntity? {
        return syncsById[id]
    }

    override fun createSyncEntity(): SyncEntity {
        val syncId = nextSyncId++
        val entity = SyncEntity(id = syncId)
        syncsById[syncId] = entity
        return entity
    }

    override fun updateSyncEntity(entity: SyncEntity): SyncEntity {
        val id = entity.id ?: throw IllegalArgumentException("sync id is required")
        if (!syncsById.containsKey(id)) {
            throw StorageWriteException(
                backend = "fake",
                operation = "updateSyncEntity",
                key = id.toString(),
                cause = IllegalStateException("Sync not found: $id")
            )
        }

        syncsById[id] = entity
        return entity
    }

    override fun updateSyncStatusFromJobs(syncId: Int): SyncEntity? {
        val sync = syncsById[syncId] ?: return null
        val jobs = jobsById.values.filter { it.syncId == syncId }
        if (jobs.isEmpty()) return sync

        val active = jobs.any { it.status == JobStatus.PENDING || it.status == JobStatus.IN_PROGRESS }
        if (active) return sync

        val updated = if (jobs.any { it.status == JobStatus.ERROR }) {
            sync.copy(status = SyncStatus.RETRY, finishedAt = null)
        } else {
            sync.copy(status = SyncStatus.DONE, finishedAt = Instant.now())
        }
        syncsById[syncId] = updated
        return updated
    }

    override fun reserveRateLimitSlot(name: String, spacingMs: Long): Instant {
        require(spacingMs >= 0) { "spacingMs must be >= 0" }
        val now = Instant.now()
        val reservedAt = rateLimitsByName[name]?.takeIf { it.isAfter(now) } ?: now
        rateLimitsByName[name] = reservedAt.plusMillis(spacingMs)
        return reservedAt
    }

    private fun validateDedupeKey(key: String) {
        require(key.length <= 40) { "dedupeKey length must be <= 40" }
    }
}
