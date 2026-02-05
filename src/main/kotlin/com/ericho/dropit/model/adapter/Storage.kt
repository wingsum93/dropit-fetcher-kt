package com.ericho.dropit.model.adapter
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.entity.ProductEntity

interface Storage {
    fun upsert(detail: SingleProductPayload)
    fun upsert(product: ProductEntity)
}
