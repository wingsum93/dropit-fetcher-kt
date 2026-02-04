package com.ericho.dropit

import kotlinx.coroutines.runBlocking

class DropitFetchService {

    fun start(isResume: Boolean = false)= runBlocking{
        val client = GroceryClient()

        val allDepartmentJson = client.fetchUrlAsJson("https://api.freshop.ncrcloud.com/1/products?app_key=lindos&department_id_cascade=true&include_departments=true&limit=0&render_id=1770110706171&store_id=7446&token=55538cf8dc7e26f2b8e6ff150c07acad")
        val dePayload = client.fetchDepartments()
        val productsOfDepartmentA = client.fetchProductsFromDepartment(22888702)
        val productsOfDepartmentB = client.fetchProductsFromDepartment(22888712)
        val productsOfDepartmentC = client.fetchProductsFromDepartment(22888714)
        val productsOfDepartmentD = client.fetchProductsFromDepartment(22887698)

        val productJson = client.fetchProductDetailAsJson(1564405684712095895L)

        // Write to file

    }
}