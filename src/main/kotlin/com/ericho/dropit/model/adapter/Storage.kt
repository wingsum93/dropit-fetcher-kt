package com.ericho.dropit.model.adapter
import com.ericho.dropit.model.SingleProductPayload

interface Storage : AutoCloseable {
    fun upsertSnapshot(detail: SingleProductPayload)

    override fun close() {
        // default no-op
    }
}
