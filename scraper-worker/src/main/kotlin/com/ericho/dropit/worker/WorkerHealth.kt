package com.ericho.dropit.worker

import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.cio.CIO
import io.ktor.server.engine.ApplicationEngine
import io.ktor.server.engine.embeddedServer
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import java.util.concurrent.atomic.AtomicBoolean

class WorkerReadiness {
    private val ready = AtomicBoolean(false)

    fun markReady() {
        ready.set(true)
    }

    fun markNotReady() {
        ready.set(false)
    }

    fun isReady(): Boolean = ready.get()
}

fun Application.configureWorkerHealth(readiness: WorkerReadiness) {
    routing {
        get("/health") {
            if (readiness.isReady()) {
                call.respondText(
                    text = "ready",
                    contentType = ContentType.Text.Plain,
                    status = HttpStatusCode.OK
                )
            } else {
                call.respondText(
                    text = "initializing",
                    contentType = ContentType.Text.Plain,
                    status = HttpStatusCode.ServiceUnavailable
                )
            }
        }
    }
}

fun startWorkerHealthServer(
    readiness: WorkerReadiness,
    host: String,
    port: Int
): ApplicationEngine {
    return embeddedServer(CIO, host = host, port = port) {
        configureWorkerHealth(readiness)
    }.start(wait = false)
}
