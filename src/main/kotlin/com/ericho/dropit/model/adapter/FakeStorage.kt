package com.ericho.dropit.model.adapter

import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.adapter.Storage

class FakeStorage : Storage {

    override fun upsert(detail: SingleProductPayload) {
        //
    }
}