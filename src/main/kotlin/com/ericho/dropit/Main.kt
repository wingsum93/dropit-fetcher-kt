package com.ericho.dropit

import com.ericho.dropit.util.PrettyJson
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import java.io.File


fun main() = runBlocking {
    val client = GroceryClient()

    val aJson = client.fetchUrlAsJson("https://api.freshop.ncrcloud.com/1/products?app_key=lindos&department_id_cascade=true&include_departments=true&limit=0&render_id=1770110706171&store_id=7446&token=55538cf8dc7e26f2b8e6ff150c07acad")


    val prettyJson = PrettyJson.process(aJson)
    // Write to file
    File("api1.json").writeText(prettyJson)

    println("Saved to api1.json")
}