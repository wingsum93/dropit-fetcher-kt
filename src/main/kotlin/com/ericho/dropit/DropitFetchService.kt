package com.ericho.dropit

import com.ericho.dropit.model.FetchReport
import com.ericho.dropit.model.adapter.Storage
import com.ericho.dropit.model.FetchOptions
import com.ericho.dropit.model.entity.JobEntity
import com.ericho.dropit.model.entity.JobStatus
import com.ericho.dropit.model.entity.JobType
import com.ericho.dropit.model.entity.SyncEntity
import com.ericho.dropit.model.entity.SyncStatus
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

@OptIn(ExperimentalCoroutinesApi::class)
class DropitFetchService(
    private val repo: GroceryDataSource = GroceryRepository(),
    private val storage: Storage
) {
    companion object {
        private const val ALL_DEPARTMENTS_DEDUPE_KEY = "all_departments"
    }

    suspend fun run(options: FetchOptions): FetchReport {
        val start = System.currentTimeMillis()

        val deptCount = AtomicInteger(0)
        val itemCount = AtomicInteger(0)
        val detailCount = AtomicInteger(0)
        val failed = AtomicInteger(0)

        println("Starting fetch...")
        println("resume=${options.resume}, since=${options.since}, dryRun=${options.dryRun}")

        val sync = prepareSyncSession()
        val syncId = sync.id ?: error("sync id is null")

        try {
            val departmentIds = ensureDepartmentJobs(syncId)
            val semaphore = Semaphore(options.detailConcurrency)

            departmentIds
                .asFlow()
                .onEach { deptId ->
                    deptCount.incrementAndGet()
                    println("Dept: $deptId")
                }
                .flatMapMerge(options.deptConcurrency) { deptId ->
                    repo.getAllItemsInDepartment(deptId, options)
                        .asFlow()
                        .onEach {
                            println("itemId = ${it.id}")
                            itemCount.incrementAndGet()
                        }
                }
                .buffer(200)
                .flatMapMerge(options.detailConcurrency) { item ->
                    flow {
                        semaphore.withPermit {
                            try {
                                val detail = repo.getItemDetail(item.id.toLong())
                                detailCount.incrementAndGet()
                                emit(detail)
                            } catch (e: Exception) {
                                failed.incrementAndGet()
                                System.err.println(
                                    "event=sync_item_failed itemId=${item.id} " +
                                        "error=${e::class.simpleName} message=${e.message}"
                                )
                            }
                        }
                    }
                }
                .collect()

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

        val departments = repo.getAllDepartments()
        val departmentIds = departments.map { it.id.toInt() }.distinct()
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
