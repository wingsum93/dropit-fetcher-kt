package com.ericho.dropit.scraper

import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.DepartmentDto
import com.ericho.dropit.model.entity.DepartmentEntity
import com.ericho.dropit.model.entity.ProductEntity
import java.time.Instant
import java.time.OffsetDateTime

class ProductParser {
    fun toDepartmentEntity(dto: DepartmentDto): DepartmentEntity {
        return DepartmentEntity(
            id = dto.id.toInt(),
            parentDepartmentId = dto.parentId?.toIntOrNull(),
            name = dto.name,
            path = dto.path,
            storeId = dto.storeId.toInt(),
            count = dto.count,
            canonicalUrl = dto.canonicalUrl
        )
    }

    fun toProductEntity(payload: SingleProductPayload): ProductEntity {
        return ProductEntity(
            id = payload.id.toLong(),
            storeId = payload.storeId.toIntOrNull(),
            category = payload.category.toIntOrNull(),
            departmentId = payload.departmentId.firstOrNull()?.toIntOrNull(),
            unitPrice = payload.unitPrice.toFloat(),
            popularity = payload.popularity,
            upc = payload.upc,
            name = payload.name,
            canonicalUrl = payload.canonicalUrl,
            remoteLastUpdateAt = parseInstant(payload.lastUpdatedAt)
        )
    }

    private fun parseInstant(value: String): Instant? {
        return runCatching { Instant.parse(value) }
            .getOrElse {
                runCatching { OffsetDateTime.parse(value).toInstant() }.getOrNull()
            }
    }
}
