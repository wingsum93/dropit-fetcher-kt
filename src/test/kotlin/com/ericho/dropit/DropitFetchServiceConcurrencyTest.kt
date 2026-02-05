package com.ericho.dropit

import com.ericho.dropit.model.FetchOptions
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.adapter.SqliteStorage
import com.ericho.dropit.model.api.DepartmentDto
import com.ericho.dropit.model.api.ProductDto
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.sql.DriverManager
import kotlin.test.assertEquals

class DropitFetchServiceConcurrencyTest {
    private val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
    }

    @Test
    fun `parallel writes through service do not drop snapshots`() = runBlocking {
        val itemsPerDepartment = 30
        val departmentCount = 2
        val expectedSnapshots = itemsPerDepartment * departmentCount
        val dbPath = Files.createTempFile("dropit-concurrency-", ".sqlite")
        val storage = SqliteStorage(dbPath.toString())
        val repo = FakeGroceryDataSource(
            basePayload = baseDetail(),
            itemsPerDepartment = itemsPerDepartment,
            departmentCount = departmentCount
        )
        val service = DropitFetchService(repo = repo, storage = storage)

        try {
            val report = service.run(
                FetchOptions(
                    deptConcurrency = 2,
                    detailConcurrency = 8,
                    resume = false,
                    since = null,
                    dryRun = false
                )
            )

            assertEquals(expectedSnapshots, report.details)
            assertEquals(0, report.failed)

            DriverManager.getConnection("jdbc:sqlite:${dbPath}").use { connection ->
                val rowCount = connection.prepareStatement(
                    "SELECT COUNT(*) FROM product_snapshots"
                ).use { stmt ->
                    stmt.executeQuery().use { rs ->
                        rs.next()
                        rs.getInt(1)
                    }
                }
                assertEquals(expectedSnapshots, rowCount)
            }
        } finally {
            storage.close()
            Files.deleteIfExists(dbPath)
        }
    }

    private fun baseDetail(): SingleProductPayload {
        val rawJson = readResourceText("/single_product_1564405684712095895.json")
        return json.decodeFromString(rawJson)
    }

    private fun readResourceText(resourceName: String): String {
        val url = checkNotNull(this::class.java.getResource(resourceName)) {
            "Missing test resource: $resourceName"
        }
        return url.readText()
    }

    private class FakeGroceryDataSource(
        private val basePayload: SingleProductPayload,
        private val itemsPerDepartment: Int,
        private val departmentCount: Int
    ) : GroceryDataSource {
        override suspend fun getAllDepartments(storeId: Int): List<DepartmentDto> {
            return (1..departmentCount).map { dept ->
                DepartmentDto(
                    id = dept.toString(),
                    count = itemsPerDepartment,
                    sequence = dept,
                    name = "Department $dept",
                    parentId = null,
                    identifier = "dept-$dept",
                    internalSequence = dept,
                    storeId = storeId.toString(),
                    storeDepth = 1,
                    typeId = "type-$dept",
                    path = "Department/$dept",
                    lineage = listOf("Department"),
                    canonicalUrl = "/department/$dept",
                    masterTaxonomy = null,
                    isRedDepartment = false
                )
            }
        }

        override suspend fun getAllItemsInDepartment(departmentId: Int, fetchOptions: FetchOptions): List<ProductDto> {
            return (1..itemsPerDepartment).map { i ->
                val id = (departmentId.toLong() * 100_000L) + i
                ProductDto(id = id.toString())
            }
        }

        override suspend fun getItemDetail(itemId: Long): SingleProductPayload {
            return basePayload.copy(
                id = itemId.toString(),
                name = "Item $itemId"
            )
        }
    }
}
