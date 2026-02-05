package com.ericho.dropit.model.adapter

class StorageWriteException(
    backend: String,
    operation: String,
    key: String,
    cause: Throwable
) : RuntimeException(
    "Storage write failed. backend=$backend operation=$operation key=$key cause=${cause.message}",
    cause
)
