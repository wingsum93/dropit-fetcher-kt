package com.ericho.dropit.scraper

import com.ericho.dropit.GroceryDataSource
import com.ericho.dropit.GroceryRepository
import com.ericho.dropit.model.FetchOptions
import com.ericho.dropit.model.adapter.Storage
import com.ericho.dropit.model.entity.JobEntity
import com.ericho.dropit.model.entity.JobStatus
import com.ericho.dropit.model.entity.JobType
import com.ericho.dropit.model.entity.SyncEntity
import com.ericho.dropit.model.entity.SyncStatus

class ProductScraper(
    private val repo: GroceryDataSource = GroceryRepository(),
    private val storage: Storage,
    private val parser: ProductParser = ProductParser(),
    private val rateLimiter: RateLimiter = StorageRateLimiter(storage)
) {
    fun submitScrape(options: FetchOptions = FetchOptions()): SyncEntity {
        val created = storage.createSyncEntity()
        val syncId = created.id ?: error("created sync id is null")
        val running = storage.updateSyncEntity(
            created.copy(
                attempts = created.attempts + 1,
                status = SyncStatus.RUNNING,
                finishedAt = null
            )
        )
        storage.insertJobsIfNotExist(
            listOf(
                JobEntity(
                    syncId = syncId,
                    jobType = JobType.FETCH_DEPARTMENTS,
                    status = JobStatus.PENDING,
                    dedupeKey = ALL_DEPARTMENTS_DEDUPE_KEY
                )
            )
        )
        return running
    }

    suspend fun executeClaimedJob(job: JobEntity, options: FetchOptions = FetchOptions()): ScrapeResult {
        val jobId = job.id ?: error("claimed job id is null")
        val start = System.currentTimeMillis()
        try {
            val result = when (job.jobType) {
                JobType.FETCH_DEPARTMENTS -> executeDepartmentsJob(job)
                JobType.FETCH_DEPARTMENT_PRODUCTS -> executeDepartmentProductsJob(job, options)
                JobType.FETCH_PRODUCT -> executeProductJob(job)
            }
            storage.updateJobStatusById(jobId, JobStatus.SUCCESS)
            storage.updateSyncStatusFromJobs(job.syncId)
            return result.copy(durationMs = System.currentTimeMillis() - start)
        } catch (exception: Throwable) {
            storage.updateJobStatusById(jobId, JobStatus.ERROR)
            storage.updateSyncStatusFromJobs(job.syncId)
            throw exception
        }
    }

    private suspend fun executeDepartmentsJob(job: JobEntity): ScrapeResult {
        rateLimiter.awaitPermit()
        val departments = repo.getAllDepartments()
        storage.insertDepartmentEntity(departments.map(parser::toDepartmentEntity))
        storage.insertJobsIfNotExist(
            departments.map { department ->
                JobEntity(
                    syncId = job.syncId,
                    jobType = JobType.FETCH_DEPARTMENT_PRODUCTS,
                    status = JobStatus.PENDING,
                    dedupeKey = "dept:${department.id}"
                )
            }
        )
        return ScrapeResult(
            syncId = job.syncId,
            jobId = job.id,
            departments = departments.size
        )
    }

    private suspend fun executeDepartmentProductsJob(job: JobEntity, options: FetchOptions): ScrapeResult {
        val departmentId = parseDepartmentId(job.dedupeKey)
            ?: throw IllegalArgumentException("Invalid department job dedupe key: ${job.dedupeKey}")

        rateLimiter.awaitPermit()
        val items = repo.getAllItemsInDepartment(departmentId, options)
        val productIds = items.mapNotNull { it.id.toLongOrNull() }
        storage.createProductIfNotExist(productIds)

        var details = 0
        var failed = 0
        for (productId in productIds) {
            try {
                rateLimiter.awaitPermit()
                val detail = repo.getItemDetail(productId)
                val entity = parser.toProductEntity(detail)
                storage.updateProduct(productId, entity)
                details += 1
            } catch (exception: Exception) {
                failed += 1
                System.err.println(
                    "event=scrape_product_failed syncId=${job.syncId} productId=$productId " +
                        "error=${exception::class.simpleName} message=${exception.message}"
                )
            }
        }

        if (failed > 0) {
            throw IllegalStateException("Failed to fetch $failed product details for department $departmentId")
        }

        return ScrapeResult(
            syncId = job.syncId,
            jobId = job.id,
            items = productIds.size,
            details = details,
            failed = failed
        )
    }

    private suspend fun executeProductJob(job: JobEntity): ScrapeResult {
        val productId = job.dedupeKey.removePrefix("product:").toLongOrNull()
            ?: throw IllegalArgumentException("Invalid product job dedupe key: ${job.dedupeKey}")
        storage.createProductIfNotExist(listOf(productId))
        rateLimiter.awaitPermit()
        val detail = repo.getItemDetail(productId)
        storage.updateProduct(productId, parser.toProductEntity(detail))
        return ScrapeResult(syncId = job.syncId, jobId = job.id, details = 1)
    }

    private fun parseDepartmentId(dedupeKey: String): Int? {
        if (!dedupeKey.startsWith("dept:")) return null
        return dedupeKey.removePrefix("dept:").toIntOrNull()
    }

    companion object {
        const val ALL_DEPARTMENTS_DEDUPE_KEY = "all_departments"
    }
}
