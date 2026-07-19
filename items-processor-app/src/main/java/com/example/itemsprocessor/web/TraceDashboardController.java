package com.example.itemsprocessor.web;

import com.example.itemsprocessor.config.AppProperties;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Controller
public class TraceDashboardController {
    private final WebClient webClient;
    private final AppProperties properties;

    public TraceDashboardController(WebClient webClient, AppProperties properties) {
        this.webClient = webClient; this.properties = properties;
    }

    @GetMapping("/api/traces/pokemon/{id}")
    @ResponseBody
    public Mono<Map<String, Object>> pokemon(@PathVariable String id,
                                              @RequestParam(defaultValue = "21600") long lookbackSeconds) {
        long now = Instant.now().getEpochSecond();
        return webClient.get().uri(properties.tempoUrl()
                        + "/api/search?tags={tags}&start={start}&end={end}&limit=100",
                        "pokemon.id=" + id, now - lookbackSeconds, now)
                .retrieve().bodyToMono(JsonNode.class)
                .flatMapMany(result -> Flux.fromIterable(result.path("traces")))
                .map(trace -> trace.path("traceID").asText()).filter(traceId -> !traceId.isBlank())
                .flatMap(this::fetchTrace).flatMapIterable(entry -> flatten(entry.getKey(), entry.getValue(), id))
                .sort((left, right) -> Long.compare((Long) left.get("startTimeMs"), (Long) right.get("startTimeMs")))
                .collectList().map(spans -> Map.of("pokemonId", id,
                        "traceCount", spans.stream().map(span -> span.get("traceId")).distinct().count(),
                        "spanCount", spans.size(), "spans", spans));
    }

    @GetMapping(value = "/traces", produces = MediaType.TEXT_HTML_VALUE)
    @ResponseBody
    public String dashboard() {
        return """
                <!doctype html><html><head><meta charset="utf-8"><title>Pokemon traces</title>
                <style>body{font:14px sans-serif;background:#0f172a;color:#e5e7eb;padding:24px}input,button{padding:8px}button{background:#2563eb;color:white;border:0}table{width:100%;margin-top:20px;border-collapse:collapse}td,th{padding:8px;border-bottom:1px solid #334155;text-align:left}</style></head>
                <body><h1>Pokemon Trace Dashboard</h1><input id="id" value="25"><button onclick="load()">Search</button>
                <table><thead><tr><th>Time</th><th>Service</th><th>Span</th><th>Duration ms</th><th>Trace</th></tr></thead><tbody id="rows"></tbody></table>
                <script>async function load(){const d=await(await fetch('/api/traces/pokemon/'+encodeURIComponent(id.value))).json();rows.innerHTML=d.spans.map(s=>`<tr><td>${s.startTime}</td><td>${s.serviceName}</td><td>${s.name}</td><td>${s.durationMs}</td><td>${s.traceId}</td></tr>`).join('')}load()</script></body></html>
                """;
    }

    private Mono<Map.Entry<String, JsonNode>> fetchTrace(String traceId) {
        return webClient.get().uri(properties.tempoUrl() + "/api/traces/" + traceId).retrieve()
                .bodyToMono(JsonNode.class).map(trace -> Map.entry(traceId, trace));
    }

    private List<Map<String, Object>> flatten(String traceId, JsonNode trace, String pokemonId) {
        List<Map<String, Object>> rows = new ArrayList<>();
        trace.path("batches").forEach(batch -> {
            String service = attributes(batch.path("resource").path("attributes")).getOrDefault("service.name", "");
            batch.path("scopeSpans").forEach(scope -> scope.path("spans").forEach(span -> {
                Map<String, String> attrs = attributes(span.path("attributes"));
                String ids = attrs.getOrDefault("pokemon.ids", "");
                if (!pokemonId.equals(attrs.get("pokemon.id")) && !List.of(ids.split(",")).contains(pokemonId)) return;
                long startNanos = span.path("startTimeUnixNano").asLong();
                long endNanos = span.path("endTimeUnixNano").asLong();
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("traceId", traceId); row.put("spanId", span.path("spanId").asText());
                row.put("serviceName", service); row.put("name", span.path("name").asText());
                row.put("startTimeMs", startNanos / 1_000_000);
                row.put("startTime", startNanos == 0 ? "" : Instant.ofEpochMilli(startNanos / 1_000_000).toString());
                row.put("durationMs", (endNanos - startNanos) / 1_000_000.0);
                row.put("attributes", attrs); rows.add(row);
            }));
        });
        return rows;
    }

    private Map<String, String> attributes(JsonNode values) {
        Map<String, String> result = new LinkedHashMap<>();
        values.forEach(attribute -> {
            JsonNode value = attribute.path("value");
            for (String name : List.of("stringValue", "intValue", "doubleValue", "boolValue"))
                if (value.has(name)) { result.put(attribute.path("key").asText(), value.path(name).asText()); break; }
        });
        return result;
    }
}
