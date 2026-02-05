package com.ericho.dropit

import com.ericho.dropit.model.FetchOptions
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.api.DepartmentDto
import com.ericho.dropit.model.api.ProductDto

interface GroceryDataSource {
    suspend fun getAllDepartments(storeId: Int = AppSetting.storeId7442): List<DepartmentDto>
    suspend fun getAllItemsInDepartment(departmentId: Int, fetchOptions: FetchOptions): List<ProductDto>
    suspend fun getItemDetail(itemId: Long): SingleProductPayload
}
