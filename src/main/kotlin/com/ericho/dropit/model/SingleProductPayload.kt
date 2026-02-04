package com.ericho.dropit.model

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class SingleProductPayload(
    val id: String,
    @SerialName("store_id")
    val storeId: String,
    val category: String,
    @SerialName("department_id")
    val departmentId: List<String>,
    @SerialName("unit_price")
    val unitPrice: Double,
    val popularity: Int,
    @SerialName("pos_department")
    val posDepartment: String,
    @SerialName("quantity_maximum")
    val quantityMaximum: Double,
    val upc: String,
    @SerialName("status_id")
    val statusId: String,
    @SerialName("tag_ids")
    val tagIds: List<String>,
    @SerialName("fulfillment_type_ids")
    val fulfillmentTypeIds: List<String>,
    @SerialName("last_updated_at")
    val lastUpdatedAt: String,
    @SerialName("reference_id")
    val referenceId: String,
    @SerialName("reference_ids")
    val referenceIds: List<String>,
    val name: String,
    @SerialName("is_weight_required")
    val isWeightRequired: Boolean,
    val images: List<SingleProductImage>?,
    @SerialName("substitution_type_ids")
    val substitutionTypeIds: List<String>?,
    @SerialName("note_configuration")
    val noteConfiguration: NoteConfiguration?,
    @SerialName("upc_configuration")
    val upcConfiguration: UpcConfiguration?,
    @SerialName("cover_image")
    val coverImage: String?,
    @SerialName("enforce_product_inventory")
    val enforceProductInventory: Boolean,
    @SerialName("effective_quantity_on_hand")
    val effectiveQuantityOnHand: Int,
    @SerialName("has_featured_offer")
    val hasFeaturedOffer: Boolean,
    val status: String,
    val identifier: String,
    @SerialName("quantity_initial")
    val quantityInitial: Double,
    @SerialName("department_ids")
    val departmentIds: List<String>,
    @SerialName("fulfillment_type_id")
    val fulfillmentTypeId: String?,
    @SerialName("allow_user_product_notes")
    val allowUserProductNotes: Boolean,
    @SerialName("barcode_type")
    val barcodeType: String?,
    val barcode: String?,
    @SerialName("barcode_upc_a")
    val barcodeUpcA: String?,
    @SerialName("barcode_ean13")
    val barcodeEan13: String?,
    @SerialName("barcode_ean8")
    val barcodeEan8: String?,
    @SerialName("canonical_url")
    val canonicalUrl: String
)

@Serializable
data class SingleProductImage(
    val sequence: Int,
    val identifier: String
)

@Serializable
data class NoteConfiguration(
    @SerialName("is_required")
    val isRequired: Boolean,
    @SerialName("is_allowed")
    val isAllowed: Boolean
)

@Serializable
data class UpcConfiguration(
    val type: String
)
