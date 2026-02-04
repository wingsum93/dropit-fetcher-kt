package com.ericho.dropit

import com.ericho.dropit.model.DatabaseConfig
import com.ericho.dropit.model.PostgresWriter
import com.ericho.dropit.model.SnapshotPayload
import com.ericho.dropit.util.PrettyJson
import io.github.cdimascio.dotenv.dotenv
import kotlinx.coroutines.runBlocking
import java.io.File


fun main() = runBlocking {
    val dotenv = dotenv()
    val tempDir = dotenv["TEMP_FOLDER"] ?: System.getenv("TEMP_FOLDER") ?: "temp"
    File(tempDir).mkdirs()
    val client = GroceryClient()

    val allDepartmentJson = client.fetchUrlAsJson("https://api.freshop.ncrcloud.com/1/products?app_key=lindos&department_id_cascade=true&include_departments=true&limit=0&render_id=1770110706171&store_id=7446&token=55538cf8dc7e26f2b8e6ff150c07acad")
    val productsOfDepartmentA = client.fetchProductsFromDepartment(22888702)
    val productsOfDepartmentB = client.fetchProductsFromDepartment(22888712)
    val productsOfDepartmentC = client.fetchProductsFromDepartment(22888714)
    val productsOfDepartmentD = client.fetchProductsFromDepartment(22887698)

    val productJson = client.fetchProductDetailAsJson(1564405684712095895L)

    // Write to file
    val allDepartmentPretty = allDepartmentJson.toPrettyJson()
    val productsOfDepartmentAPretty = productsOfDepartmentA.toString().toPrettyJson()
    val productsOfDepartmentBPretty = productsOfDepartmentB.toString().toPrettyJson()
    val productsOfDepartmentCPretty = productsOfDepartmentC.toString().toPrettyJson()
    val productsOfDepartmentDPretty = productsOfDepartmentD.toString().toPrettyJson()
    val productPretty = productJson.toPrettyJson()

    File("$tempDir/allDepartment.json").writeText(allDepartmentPretty)
    File("$tempDir/productsOfDepartmentA.json").writeText(productsOfDepartmentAPretty)
    File("$tempDir/productsOfDepartmentB.json").writeText(productsOfDepartmentBPretty)
    File("$tempDir/productsOfDepartmentC.json").writeText(productsOfDepartmentCPretty)
    File("$tempDir/productsOfDepartmentD.json").writeText(productsOfDepartmentDPretty)
    File("$tempDir/singleProduct.json").writeText(productPretty)

    val databaseConfig = DatabaseConfig.fromEnv(dotenv)
    if (databaseConfig == null) {
        println("Postgres env vars missing. Skipping database persistence.")
    } else {
        PostgresWriter.writeSnapshots(
            databaseConfig,
            listOf(
                SnapshotPayload("allDepartment", allDepartmentPretty),
                SnapshotPayload("productsOfDepartmentA", productsOfDepartmentAPretty),
                SnapshotPayload("productsOfDepartmentB", productsOfDepartmentBPretty),
                SnapshotPayload("productsOfDepartmentC", productsOfDepartmentCPretty),
                SnapshotPayload("productsOfDepartmentD", productsOfDepartmentDPretty),
                SnapshotPayload("singleProduct", productPretty)
            )
        )
    }

    println("Saved to $tempDir/api1.json")
}

private fun String.toPrettyJson():String{
    return PrettyJson.process(this)
}
