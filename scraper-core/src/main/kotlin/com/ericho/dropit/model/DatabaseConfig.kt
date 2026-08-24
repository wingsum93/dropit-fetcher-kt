package com.ericho.dropit.model

import io.github.cdimascio.dotenv.Dotenv

data class DatabaseConfig(
    val host: String,
    val port: Int,
    val database: String,
    val user: String,
    val password: String
) {
    companion object {
        fun fromEnv(dotenv: Dotenv): DatabaseConfig? {
            val host = dotenv["POSTGRES_HOST"] ?: System.getenv("POSTGRES_HOST")
            val portValue = dotenv["POSTGRES_PORT"] ?: System.getenv("POSTGRES_PORT")
            val database = dotenv["POSTGRES_DB"] ?: System.getenv("POSTGRES_DB")
            val user = dotenv["POSTGRES_USER"] ?: System.getenv("POSTGRES_USER")
            val password = dotenv["POSTGRES_PASSWORD"] ?: System.getenv("POSTGRES_PASSWORD")

            if (host.isNullOrBlank() || portValue.isNullOrBlank() || database.isNullOrBlank() ||
                user.isNullOrBlank() || password.isNullOrBlank()
            ) {
                return null
            }

            val port = portValue.toIntOrNull() ?: return null
            return DatabaseConfig(
                host = host,
                port = port,
                database = database,
                user = user,
                password = password
            )
        }
    }
}
