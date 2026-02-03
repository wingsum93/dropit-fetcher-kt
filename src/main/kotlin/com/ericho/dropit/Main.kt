package com.ericho.dropit

import com.ericho.dropit.util.PrettyJson
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import java.io.File


fun main() = runBlocking {
    val client = GroceryClient()

    val aJson = client.fetchUrlAsJson("https://api.freshop.ncrcloud.com/1/products?app_key=lindos&department_id_cascade=true&include_departments=true&limit=0&render_id=1770110706171&store_id=7446&token=55538cf8dc7e26f2b8e6ff150c07acad")
    val tr2Json = client.fetchUrlAsJson("https://api.freshop.ncrcloud.com/1/products?app_key=lindos&department_id=22886618&department_id_cascade=true&fields=id%2Cidentifier%2Cattribution_token%2Creference_id%2Creference_ids%2Cupc%2Cname%2Cstore_id%2Cdepartment_id%2Csize%2Ccover_image%2Cprice%2Csale_price%2Csale_price_md%2Csale_start_date%2Csale_finish_date%2Cprice_disclaimer%2Csale_price_disclaimer%2Cis_favorite%2Crelevance%2Cpopularity%2Cshopper_walkpath%2Cfulfillment_walkpath%2Cquantity_step%2Cquantity_minimum%2Cquantity_initial%2Cquantity_label%2Cquantity_label_singular%2Cvarieties%2Cquantity_size_ratio_description%2Cstatus%2Cstatus_id%2Csale_configuration_type_id%2Cfulfillment_type_id%2Cfulfillment_type_ids%2Cother_attributes%2Cclippable_offer%2Cslot_message%2Ccall_out%2Chas_featured_offer%2Ctax_class_label%2Cpromotion_text%2Csale_offer%2Cstore_card_required%2Caverage_rating%2Creview_count%2Clike_code%2Cshelf_tag_ids%2Coffers%2Cis_place_holder_cover_image%2Cvideo_config%2Cenforce_product_inventory%2Cdisallow_adding_to_cart%2Csubstitution_type_ids%2Cunit_price%2Coffer_sale_price%2Ccanonical_url%2Coffered_together%2Csequence&include_offered_together=true&limit=96&popularity_sort=asc&render_id=1769356302366&sort=popularity&store_id=7442")

    // Write to file
    File("api1.json").writeText(aJson.toPrettyJson())
    File("api2.json").writeText(tr2Json.toPrettyJson())

    println("Saved to api1.json")
}

private fun String.toPrettyJson():String{
    return PrettyJson.process(this)
}