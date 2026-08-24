package com.ericho.dropit.model

import java.time.LocalDate

data class FetchOptions(
    val deptConcurrency: Int = 1,
    val detailConcurrency: Int = 1,
    val resume: Boolean = false,
    val since: LocalDate? = null,
    val dryRun: Boolean = false
)
