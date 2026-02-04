package com.ericho.dropit.model.adapter
import com.ericho.dropit.model.SingleProductPayload

interface Storage {
    fun upsert(detail: SingleProductPayload)
}