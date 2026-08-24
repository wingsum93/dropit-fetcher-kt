package com.ericho.dropit

import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.DepartmentPayload
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class ApiConversionTest {
    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
    }

    @Test
    fun testSingleApiJson(){
        val rawJson = readResourceText("/single_product_1564405684712095895.json")

        val payload = json.decodeFromString<SingleProductPayload>(rawJson)

        assertEquals("1564405684712095895", payload.id)
        assertEquals("Febreze Car Vent Lush", payload.name)
        assertEquals(12.59, payload.unitPrice)
        assertNotNull(payload.noteConfiguration)
    }

    @Test
    fun testDepartmentApiJson() {
        val rawJson = readResourceText("/all_department.json")

        val payload = json.decodeFromString<DepartmentPayload>(rawJson)

        assertEquals(14120, payload.total)
        assertNotNull(payload.departments.firstOrNull())
        assertEquals("22886614", payload.departments.first().id)
        assertEquals("Shop", payload.departments.first().name)
    }

    private fun readResourceText(resourceName: String): String {
        val url = checkNotNull(this::class.java.getResource(resourceName)) {
            "Missing test resource: $resourceName"
        }
        return url.readText()
    }
}