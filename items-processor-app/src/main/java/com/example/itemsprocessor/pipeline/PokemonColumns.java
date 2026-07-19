package com.example.itemsprocessor.pipeline;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.Map;

final class PokemonColumns {
    private PokemonColumns() { }

    static Map<String, String> from(JsonNode pokemon) {
        Map<String, String> columns = new LinkedHashMap<>();
        scalar(pokemon, "species", "info:species", columns);
        scalar(pokemon, "description", "info:description", columns);
        object(pokemon.path("image"), "info:image_", columns);
        object(pokemon.path("name"), "name:", columns);
        array(pokemon.path("type"), "type:", columns);
        object(pokemon.path("base"), "base:", columns);
        JsonNode profile = pokemon.path("profile");
        if (profile.isObject()) profile.fields().forEachRemaining(entry -> {
            if (entry.getValue().isArray()) array(entry.getValue(), "profile:" + entry.getKey() + "_", columns);
            else columns.put("profile:" + entry.getKey(), entry.getValue().asText());
        });
        return columns;
    }

    private static void scalar(JsonNode source, String field, String column, Map<String, String> target) {
        if (source.has(field)) target.put(column, source.get(field).asText());
    }
    private static void object(JsonNode source, String prefix, Map<String, String> target) {
        if (source.isObject()) source.fields().forEachRemaining(entry -> target.put(prefix + entry.getKey(), entry.getValue().asText()));
    }
    private static void array(JsonNode source, String prefix, Map<String, String> target) {
        if (source.isArray()) for (int i = 0; i < source.size(); i++) target.put(prefix + i, source.get(i).asText());
    }
}
