package com.ericho.dropit.worker

import com.ericho.dropit.model.FetchOptions
import com.ericho.dropit.model.adapter.Storage
import com.ericho.dropit.scraper.ProductScraper
import kotlinx.coroutines.delay
import java.util.concurrent.atomic.AtomicBoolean

class ScraperWorker(
    private val storage: Storage,
    private val scraper: ProductScraper,
    private val pollIntervalMs: Long = 2_000L,
    private val batchSize: Int = 1,
    private val fetchOptions: FetchOptions = FetchOptions()
) {
    suspend fun runOnce(): Int {
        val jobs = storage.claimPendingJobs(batchSize)
        jobs.forEach { job ->
            try {
                val result = scraper.executeClaimedJob(job, fetchOptions)
                println("event=worker_job_success syncId=${result.syncId} jobId=${result.jobId}")
            } catch (exception: Throwable) {
                System.err.println(
                    "event=worker_job_error syncId=${job.syncId} jobId=${job.id} " +
                        "error=${exception::class.simpleName} message=${exception.message}"
                )
            }
        }
        return jobs.size
    }

    suspend fun runUntilStopped(running: AtomicBoolean) {
        while (running.get()) {
            val count = runOnce()
            if (count == 0) {
                delay(pollIntervalMs)
            }
        }
    }
}
