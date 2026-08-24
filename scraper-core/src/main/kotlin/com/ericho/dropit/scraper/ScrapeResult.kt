package com.ericho.dropit.scraper

data class ScrapeResult(
    val syncId: Int,
    val jobId: Int?,
    val departments: Int = 0,
    val items: Int = 0,
    val details: Int = 0,
    val failed: Int = 0,
    val durationMs: Long = 0
)
