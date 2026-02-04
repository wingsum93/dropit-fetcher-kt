package com.ericho.dropit.model

data class FetchReport(
    val departments: Int,
    val items: Int,
    val details: Int,
    val failed: Int,
    val durationMs: Long
)