package com.ericho.dropit.util

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement

object PrettyJson {
    private val formatter = Json {
        prettyPrint = true
        prettyPrintIndent = "  "
    }

    fun process(rawJson: String): String {
        // Parse generic JSON tree
        val element: JsonElement = Json.parseToJsonElement(rawJson)

        // Pretty formatter
        return formatter.encodeToString(JsonElement.serializer(), element)
    }
}