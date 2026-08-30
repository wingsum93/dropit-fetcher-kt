package com.ericho.dropit

import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.DepartmentPayload
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertContentEquals
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
        val payload = decodeDepartmentPayload()

        assertEquals(14120, payload.total)
        assertNotNull(payload.departments.firstOrNull())
        assertEquals("22886614", payload.departments.first().id)
        assertEquals("Shop", payload.departments.first().name)
    }

    @Test
    fun testDiscoverDepartmentsApiJson() {
        val payload = decodeDepartmentPayload()
        val departmentsById = payload.departments.associateBy { it.id }
        val childCountsByParentId = payload.departments.groupingBy { it.parentId }.eachCount()
        val lineageDepthCounts = payload.departments.groupingBy { it.lineage.size }.eachCount()

        assertEquals(1101, payload.departments.size)
        assertEquals(1, childCountsByParentId[null])
        assertEquals(1100, payload.departments.count { it.parentId != null })
        assertEquals(
            mapOf(
                1 to 1,
                2 to 10,
                3 to 101,
                4 to 436,
                5 to 457,
                6 to 96
            ),
            lineageDepthCounts
        )

        val shop = departmentsById.getValue("22886614")
        assertEquals("Shop", shop.name)
        assertEquals(14121, shop.count)
        assertEquals(10, childCountsByParentId[shop.id])
        assertContentEquals(listOf("22886614"), shop.lineage)

        val pantry = departmentsById.getValue("22886630")
        assertEquals("Pantry", pantry.name)
        assertEquals(9441, pantry.count)
        assertEquals("22886614", pantry.parentId)
        assertEquals(17, childCountsByParentId[pantry.id])
        assertContentEquals(listOf("22886614", "22886630"), pantry.lineage)

        val cookingAndBakingNeeds = departmentsById.getValue("22886790")
        assertEquals("Cooking & Baking Needs", cookingAndBakingNeeds.name)
        assertEquals(762, cookingAndBakingNeeds.count)
        assertEquals("22886630", cookingAndBakingNeeds.parentId)
        assertEquals(20, childCountsByParentId[cookingAndBakingNeeds.id])
        assertContentEquals(listOf("22886614", "22886630", "22886790"), cookingAndBakingNeeds.lineage)

        val cannedVegetables = departmentsById.getValue("22887594")
        assertEquals("Canned Vegetables", cannedVegetables.name)
        assertEquals(250, cannedVegetables.count)
        assertEquals("22886784", cannedVegetables.parentId)
        assertEquals(20, childCountsByParentId[cannedVegetables.id])
        assertContentEquals(
            listOf("22886614", "22886630", "22886784", "22887594"),
            cannedVegetables.lineage
        )

        val cottonBallsAndSwabs = departmentsById.getValue("22889108")
        assertEquals("Cotton Balls & Swabs", cottonBallsAndSwabs.name)
        assertEquals(11, cottonBallsAndSwabs.count)
        assertEquals("22888602", cottonBallsAndSwabs.parentId)
        assertEquals(0, childCountsByParentId[cottonBallsAndSwabs.id] ?: 0)
        assertContentEquals(
            listOf("22886614", "22886630", "22886792", "22887674", "22888602", "22889108"),
            cottonBallsAndSwabs.lineage
        )
    }

    private fun decodeDepartmentPayload(): DepartmentPayload {
        val rawJson = readResourceText("/all_department.json")
        return json.decodeFromString(rawJson)
    }

    private fun readResourceText(resourceName: String): String {
        val url = checkNotNull(this::class.java.getResource(resourceName)) {
            "Missing test resource: $resourceName"
        }
        return url.readText()
    }
}
