package com.ericho.dropit

import com.ericho.dropit.model.DatabaseConfig
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class StorageBackendSelectionTest {
    @Test
    fun `selectStorageBackend chooses SQLITE when postgres config is missing`() {
        val selected = selectStorageBackend(postgresConfig = null)
        assertEquals(StorageBackend.SQLITE, selected)
    }

    @Test
    fun `selectStorageBackend chooses POSTGRES when postgres config is present`() {
        val selected = selectStorageBackend(
            postgresConfig = DatabaseConfig(
                host = "localhost",
                port = 5432,
                database = "dropit",
                user = "dropit",
                password = "dropit"
            )
        )
        assertEquals(StorageBackend.POSTGRES, selected)
    }
}
