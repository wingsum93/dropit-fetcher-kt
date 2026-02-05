package com.ericho.dropit.model.entity

import java.time.Instant

data class SyncEntity(
    val id: Int? = null,
    val attempts: Int = 0,
    val status: SyncStatus = SyncStatus.PENDING,
    val finishedAt: Instant? = null
)

enum class SyncStatus {
    PENDING,
    RUNNING,
    DONE,
    RETRY,
    DEAD
}
