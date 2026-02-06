package com.ericho.dropit

import com.ericho.dropit.model.FetchReport
import com.ericho.dropit.model.adapter.Storage
import com.ericho.dropit.model.FetchOptions
import com.ericho.dropit.model.entity.JobEntity
import com.ericho.dropit.model.entity.JobStatus
import com.ericho.dropit.model.entity.JobType
import com.ericho.dropit.model.entity.SyncEntity
import com.ericho.dropit.model.entity.SyncStatus
import kotlinx.coroutines.delay
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

class DropitFetchService(
    private val repo: GroceryDataSource = GroceryRepository(),
    private val storage: Storage
) {
    companion object {
        private const val ALL_DEPARTMENTS_DEDUPE_KEY = "all_departments"
        private const val TARGET_TIME_MILL = 5_000L
    }

    private var lastApiCallStartMs: Long = 0L

    suspend fun run(options: FetchOptions): FetchReport {
        val start = System.currentTimeMillis()

        val deptCount = AtomicInteger(0)
        val itemCount = AtomicInteger(0)
        val detailCount = AtomicInteger(0)
        val failed = AtomicInteger(0)

        println("Starting fetch...")
        println("resume=${options.resume}, since=${options.since}, dryRun=${options.dryRun}")
        println("HTTP queue mode: manual delay (1 call per 5 seconds)")
        lastApiCallStartMs = 0L

        val sync = prepareSyncSession()
        val syncId = sync.id ?: error("sync id is null")

        try {
            val departmentIds = ensureDepartmentJobs(syncId)

            for (deptId in departmentIds) {
                deptCount.incrementAndGet()
                println("Dept: $deptId")

                manualDelay(TARGET_TIME_MILL, callName = "getAllItemsInDepartment deptId=$deptId")
                val items = repo.getAllItemsInDepartment(deptId, options)
                for (item in items) {
                    println("itemId = ${item.id}")
                    itemCount.incrementAndGet()

                    try {
                        manualDelay(TARGET_TIME_MILL, callName = "getItemDetail itemId=${item.id}")
                        repo.getItemDetail(item.id.toLong())
                        detailCount.incrementAndGet()
                    } catch (e: Exception) {
                        failed.incrementAndGet()
                        System.err.println(
                            "event=sync_item_failed itemId=${item.id} " +
                                "error=${e::class.simpleName} message=${e.message}"
                        )
                    }
                }
            }

            storage.updateSyncEntity(
                sync.copy(
                    status = SyncStatus.DONE,
                    finishedAt = Instant.now()
                )
            )
        } catch (exception: Throwable) {
            storage.updateSyncEntity(
                sync.copy(
                    status = SyncStatus.RETRY,
                    finishedAt = null
                )
            )
            throw exception
        }

        val duration = System.currentTimeMillis() - start

        return FetchReport(
            departments = deptCount.get(),
            items = itemCount.get(),
            details = detailCount.get(),
            failed = failed.get(),
            durationMs = duration
        )
    }

    private suspend fun manualDelay(targetTimeMill: Long, callName: String) {
        val now = System.currentTimeMillis()
        val remaining = (lastApiCallStartMs + targetTimeMill) - now
        if (remaining > 0) {
            println("event=manual_delay_wait waitMs=$remaining call=$callName")
            delay(remaining)
        }

        lastApiCallStartMs = System.currentTimeMillis()
        println("event=manual_delay_call_started at=${Instant.ofEpochMilli(lastApiCallStartMs)} call=$callName")
    }

    private fun prepareSyncSession(): SyncEntity {
        val current = storage.findSyncEntityLeft() ?: storage.createSyncEntity()
        return storage.updateSyncEntity(
            current.copy(
                attempts = current.attempts + 1,
                status = SyncStatus.RUNNING,
                finishedAt = null
            )
        )
    }

    private suspend fun ensureDepartmentJobs(syncId: Int): List<Int> {
        val marker = storage.findByDedupeKey(syncId, ALL_DEPARTMENTS_DEDUPE_KEY)
        if (marker != null) {
            return departmentIdsFromExistingJobs(syncId)
        }

        manualDelay(TARGET_TIME_MILL, callName = "getAllDepartments")
        val departmentIds = repo.getAllDepartments().map { it.id.toInt() }.distinct()
        val jobs = buildList {
            add(
                JobEntity(
                    syncId = syncId,
                    jobType = JobType.FETCH_DEPARTMENTS,
                    status = JobStatus.PENDING,
                    dedupeKey = ALL_DEPARTMENTS_DEDUPE_KEY
                )
            )
            addAll(
                departmentIds.map { departmentId ->
                    JobEntity(
                        syncId = syncId,
                        jobType = JobType.FETCH_DEPARTMENT_PRODUCTS,
                        status = JobStatus.PENDING,
                        dedupeKey = "dept:$departmentId"
                    )
                }
            )
        }
        storage.insertJobsIfNotExist(jobs)
        return departmentIds
    }

    private fun departmentIdsFromExistingJobs(syncId: Int): List<Int> {
        return JobStatus.entries
            .asSequence()
            .filter { it == JobStatus.PENDING || it == JobStatus.IN_PROGRESS }
            .flatMap { status ->
                storage.findJobsByType(syncId, JobType.FETCH_DEPARTMENT_PRODUCTS, status).asSequence()
            }
            .mapNotNull { parseDepartmentId(it.dedupeKey) }
            .distinct()
            .sorted()
            .toList()
    }

    private fun parseDepartmentId(dedupeKey: String): Int? {
        if (!dedupeKey.startsWith("dept:")) return null
        return dedupeKey.removePrefix("dept:").toIntOrNull()
    }
}
