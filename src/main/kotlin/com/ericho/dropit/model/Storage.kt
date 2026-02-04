package com.ericho.dropit.model

interface Storage {
    fun upsert(detail: SingleProductPayload)
}