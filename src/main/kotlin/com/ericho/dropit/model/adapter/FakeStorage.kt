package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.entity.ProductEntity

class FakeStorage : Storage {

    override fun upsert(detail: SingleProductPayload) {
        //
    }

    override fun upsert(product: ProductEntity) {
        //
    }
}
