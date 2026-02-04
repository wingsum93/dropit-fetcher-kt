package com.ericho.dropit.model

import java.time.LocalDate

data class FetchOptions(
    val deptConcurrency: Int,
    val detailConcurrency: Int,
    val resume: Boolean,
    val since: LocalDate?,
    val dryRun: Boolean
)
