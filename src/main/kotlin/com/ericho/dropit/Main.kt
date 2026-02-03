package com.ericho.dropit

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
    val productsOfDepartmentD = client.fetchProductsFromDepartment(22888716)

    val productJson = client.fetchProductDetailAsJson(1564405684712095895L)

    // Write to file
    File("$tempDir/allDepartment.json").writeText(allDepartmentJson.toPrettyJson())
    File("$tempDir/productsOfDepartmentA.json").writeText(productsOfDepartmentA.toPrettyJson())
    File("$tempDir/productsOfDepartmentB.json").writeText(productsOfDepartmentB.toPrettyJson())
    File("$tempDir/productsOfDepartmentC.json").writeText(productsOfDepartmentC.toPrettyJson())
    File("$tempDir/productsOfDepartmentD.json").writeText(productsOfDepartmentD.toPrettyJson())
    File("$tempDir/singleProduct.json").writeText(productJson.toPrettyJson())

    println("Saved to $tempDir/api1.json")
}

private fun String.toPrettyJson():String{
    return PrettyJson.process(this)
}
