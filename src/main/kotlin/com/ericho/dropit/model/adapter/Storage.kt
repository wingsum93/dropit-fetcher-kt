package com.ericho.dropit.model.adapter
import com.ericho.dropit.model.SingleProductPayload
import com.ericho.dropit.model.entity.DepartmentEntity
import com.ericho.dropit.model.entity.JobEntity
import com.ericho.dropit.model.entity.JobStatus
import com.ericho.dropit.model.entity.JobType
import com.ericho.dropit.model.entity.ProductEntity
import java.time.Instant

interface Storage : AutoCloseable {
    fun upsertSnapshot(detail: SingleProductPayload)
    fun findProductById(productId: Long): ProductEntity?
    fun findProductsNameIsEmpty(limit: Int): List<ProductEntity>
    fun findProductsSince(instant: Instant, limit: Int = 10): List<ProductEntity>
    fun updateProduct(productId: Long, product: ProductEntity)
    fun createProductIfNotExist(list: List<Long>)
    fun insertDepartmentEntity(list: List<DepartmentEntity>)
    fun findDepartmentById(id: Int): DepartmentEntity?
    fun findAllDepartments(): List<DepartmentEntity>
    fun insertJobsIfNotExist(jobs: List<JobEntity>)
    fun findJobsByType(type: JobType, status: JobStatus): List<JobEntity>
    fun findJobsByType(syncId: Int, type: JobType, status: JobStatus): List<JobEntity>
    fun findDepartmentJobsById(id: Int): List<JobEntity>
    fun findDepartmentJobsById(syncId: Int, id: Int): List<JobEntity>
    fun findByDedupeKey(key: String): JobEntity?
    fun findByDedupeKey(syncId: Int, key: String): JobEntity?
    fun updateJobStatusById(id: Int, status: JobStatus)
    fun updateJobStatusByIds(ids: List<Int>, status: JobStatus)

    override fun close() {
        // default no-op
    }
}
