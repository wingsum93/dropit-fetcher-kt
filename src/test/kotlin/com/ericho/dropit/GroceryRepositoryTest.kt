package com.ericho.dropit

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking

class GroceryRepositoryTest {

    @Test
    fun testGetDepartment(){
        runBlocking {
            val repo = GroceryRepository()
            val departments = repo.getAllDepartments()

            assertTrue(departments.isNotEmpty())
            assertTrue(departments.all { it.id.isNotBlank() })
            assertTrue(departments.all { it.name.isNotBlank() })
            assertTrue(departments.all { it.storeId == AppSetting.storeId7442.toString() })
        }
    }

    @Test
    fun testGetProductOfDepartment_22888702() = runBlocking {
        val repo = GroceryRepository()
        val departmentIds = listOf(22888702, 22888712, 22888714, 22887698)

        departmentIds.forEach { departmentId ->
            val payload = repo.getProductsFromDepartment(departmentId)

            assertTrue(payload.items.isNotEmpty())
            assertTrue(payload.items.all { it.id.isNotBlank() })
            assertTrue(payload.total >= payload.items.size)
            assertTrue(payload.departments.isNotEmpty())
            assertTrue(payload.departments.any { it.id == departmentId.toString() })
        }
    }

    @Test
    fun testGetSingleProductDetail() = runBlocking {
        // item id 1564405684712095895L
        val repo = GroceryRepository()
        val payload = repo.getItemDetail(1564405684712095895L)

        assertEquals("1564405684712095895", payload.id)
        assertEquals(AppSetting.storeId7442.toString(), payload.storeId)
        assertTrue(payload.name.isNotBlank())
        assertTrue(payload.status.isNotBlank())
        assertTrue(payload.category.isNotBlank())
        assertTrue(payload.identifier.isNotBlank())
        assertTrue(payload.departmentId.isNotEmpty())
        assertTrue(payload.departmentIds.isNotEmpty())
        assertTrue(payload.unitPrice >= 0.0)
        assertTrue(payload.quantityInitial >= 0.0)
        assertTrue(payload.quantityMaximum >= 0.0)
        assertTrue(payload.effectiveQuantityOnHand >= 0)
        assertTrue(payload.canonicalUrl.isNotBlank())
    }
}
