package com.ericho.dropit.server

import com.ericho.dropit.model.adapter.Storage
import com.ericho.dropit.model.entity.DepartmentEntity
import com.ericho.dropit.model.entity.JobEntity
import com.ericho.dropit.model.entity.JobStatus
import com.ericho.dropit.model.entity.JobType
import com.ericho.dropit.model.entity.ProductEntity
import com.ericho.dropit.model.entity.SyncEntity
import com.ericho.dropit.scraper.ProductScraper
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.time.Instant

fun Application.configureScrapeApi(
    storage: Storage,
    scraper: ProductScraper
) {
    install(ContentNegotiation) {
        json(
            Json {
                ignoreUnknownKeys = true
                isLenient = true
            }
        )
    }
    install(StatusPages) {
        exception<IllegalArgumentException> { call, cause ->
            call.respond(HttpStatusCode.BadRequest, ErrorResponse(cause.message ?: "Bad request"))
        }
        exception<Throwable> { call, cause ->
            call.respond(HttpStatusCode.InternalServerError, ErrorResponse(cause.message ?: "Internal server error"))
        }
    }

    routing {
        get("/health") {
            call.respond(HealthResponse(status = "ok"))
        }

        route("/scrapes") {
            post {
                val sync = scraper.submitScrape()
                call.respond(HttpStatusCode.Accepted, SyncResponse.from(sync))
            }

            get("/{syncId}") {
                val syncId = call.parameters["syncId"]?.toIntOrNull()
                    ?: throw IllegalArgumentException("syncId must be an integer")
                val sync = storage.findSyncById(syncId)
                if (sync == null) {
                    call.respond(HttpStatusCode.NotFound, ErrorResponse("Sync not found: $syncId"))
                    return@get
                }
                call.respond(SyncResponse.from(sync))
            }

            get("/{syncId}/jobs") {
                val syncId = call.parameters["syncId"]?.toIntOrNull()
                    ?: throw IllegalArgumentException("syncId must be an integer")
                val status = call.request.queryParameters["status"]?.let { JobStatus.valueOf(it.uppercase()) }
                val type = call.request.queryParameters["type"]?.let { JobType.valueOf(it.uppercase()) }
                call.respond(storage.findJobsBySync(syncId, status, type).map(JobResponse::from))
            }
        }

        get("/products") {
            val limit = call.request.queryParameters["limit"]?.toIntOrNull() ?: 100
            val since = call.request.queryParameters["since"]?.let { Instant.parse(it) }
            val products = if (since == null) {
                storage.findProducts(limit)
            } else {
                storage.findProductsSince(since, limit)
            }
            call.respond(products.map(ProductResponse::from))
        }

        get("/departments") {
            call.respond(storage.findAllDepartments().map(DepartmentResponse::from))
        }
    }
}

@Serializable
data class HealthResponse(val status: String)

@Serializable
data class ErrorResponse(val error: String)

@Serializable
data class SyncResponse(
    val id: Int,
    val attempts: Int,
    val status: String,
    val finishedAt: String?
) {
    companion object {
        fun from(entity: SyncEntity): SyncResponse {
            return SyncResponse(
                id = entity.id ?: 0,
                attempts = entity.attempts,
                status = entity.status.name,
                finishedAt = entity.finishedAt?.toString()
            )
        }
    }
}

@Serializable
data class JobResponse(
    val id: Int?,
    val syncId: Int,
    val type: String,
    val status: String,
    val dedupeKey: String,
    val createdAt: String,
    val updatedAt: String
) {
    companion object {
        fun from(entity: JobEntity): JobResponse {
            return JobResponse(
                id = entity.id,
                syncId = entity.syncId,
                type = entity.jobType.name,
                status = entity.status.name,
                dedupeKey = entity.dedupeKey,
                createdAt = entity.createdAt.toString(),
                updatedAt = entity.updatedAt.toString()
            )
        }
    }
}

@Serializable
data class ProductResponse(
    val id: Long,
    val storeId: Int?,
    val category: Int?,
    val departmentId: Int?,
    val unitPrice: Float?,
    val popularity: Int?,
    val upc: String?,
    val name: String?,
    val canonicalUrl: String?,
    val remoteLastUpdateAt: String?,
    val createdAt: String
) {
    companion object {
        fun from(entity: ProductEntity): ProductResponse {
            return ProductResponse(
                id = entity.id,
                storeId = entity.storeId,
                category = entity.category,
                departmentId = entity.departmentId,
                unitPrice = entity.unitPrice,
                popularity = entity.popularity,
                upc = entity.upc,
                name = entity.name,
                canonicalUrl = entity.canonicalUrl,
                remoteLastUpdateAt = entity.remoteLastUpdateAt?.toString(),
                createdAt = entity.createdAt.toString()
            )
        }
    }
}

@Serializable
data class DepartmentResponse(
    val id: Int,
    val parentDepartmentId: Int?,
    val name: String,
    val path: String,
    val storeId: Int,
    val count: Int,
    val canonicalUrl: String,
    val createdAt: String
) {
    companion object {
        fun from(entity: DepartmentEntity): DepartmentResponse {
            return DepartmentResponse(
                id = entity.id,
                parentDepartmentId = entity.parentDepartmentId,
                name = entity.name,
                path = entity.path,
                storeId = entity.storeId,
                count = entity.count,
                canonicalUrl = entity.canonicalUrl,
                createdAt = entity.createdAt.toString()
            )
        }
    }
}
