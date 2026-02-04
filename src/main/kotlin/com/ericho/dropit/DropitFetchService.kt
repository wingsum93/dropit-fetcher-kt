package com.ericho.dropit

import com.ericho.dropit.model.FetchReport
import com.ericho.dropit.model.Storage
import com.ericho.dropit.model.FetchOptions
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import java.util.concurrent.atomic.AtomicInteger

@OptIn(ExperimentalCoroutinesApi::class)
class DropitFetchService(
    private val repo: GroceryRepository = GroceryRepository(),
    private val storage: Storage
) {

    suspend fun run(options: FetchOptions): FetchReport {
        val start = System.currentTimeMillis()

        val deptCount = AtomicInteger(0)
        val itemCount = AtomicInteger(0)
        val detailCount = AtomicInteger(0)
        val failed = AtomicInteger(0)

        println("Starting fetch...")
        println("resume=${options.resume}, since=${options.since}, dryRun=${options.dryRun}")

        val semaphore = Semaphore(options.detailConcurrency)

        repo.getAllDepartments()
            .asFlow()
            .onEach { dept ->
                deptCount.incrementAndGet()
                println("Dept: ${dept.name}")
            }
            .flatMapMerge(options.deptConcurrency) { dept ->
                repo.getAllItemsInDepartment(dept.id.toInt(), options)
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

                            if (!options.dryRun) {
                                storage.upsert(detail)
                            }

                            detailCount.incrementAndGet()
                            emit(detail)

                        } catch (e: Exception) {
                            failed.incrementAndGet()
                            println("Failed item ${item.id}: ${e.message}")
                        }
                    }
                }
            }
            .collect() // triggers pipeline

        val duration = System.currentTimeMillis() - start

        return FetchReport(
            departments = deptCount.get(),
            items = itemCount.get(),
            details = detailCount.get(),
            failed = failed.get(),
            durationMs = duration
        )
    }
}
