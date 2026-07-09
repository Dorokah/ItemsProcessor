import os
from datetime import datetime, timezone

import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse


TEMPO_URL = os.environ.get("TEMPO_URL", "http://localhost:3200").rstrip("/")
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000").rstrip("/")
DEFAULT_LOOKBACK_SECONDS = int(os.environ.get("TRACE_DASHBOARD_LOOKBACK_SECONDS", "21600"))

app = FastAPI(title="Pokemon Trace Dashboard")


def _attr_value(value):
    for key in ("stringValue", "intValue", "doubleValue", "boolValue"):
        if key in value:
            return str(value[key])
    nested = value.get("Value")
    if isinstance(nested, dict):
        for key in ("string_value", "int_value", "double_value", "bool_value"):
            if key in nested:
                return str(nested[key])
    return ""


def _attrs_to_dict(attributes):
    return {attr.get("key", ""): _attr_value(attr.get("value", {})) for attr in attributes or []}


def _timestamp_ms(nano):
    if not nano:
        return 0
    return int(int(nano) / 1_000_000)


def _format_time(nano):
    timestamp_ms = _timestamp_ms(nano)
    if not timestamp_ms:
        return ""
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def _span_matches_pokemon(attrs, pokemon_id):
    if attrs.get("pokemon.id") == pokemon_id:
        return True
    pokemon_ids = attrs.get("pokemon.ids", "")
    return pokemon_id in [item.strip() for item in pokemon_ids.split(",") if item.strip()]


def _search_traces(pokemon_id, lookback_seconds):
    now = int(datetime.now(timezone.utc).timestamp())
    params = {
        "tags": f"pokemon.id={pokemon_id}",
        "start": now - lookback_seconds,
        "end": now,
        "limit": 100,
    }
    response = requests.get(f"{TEMPO_URL}/api/search", params=params, timeout=20)
    response.raise_for_status()
    return response.json().get("traces", [])


def _fetch_trace(trace_id):
    response = requests.get(f"{TEMPO_URL}/api/traces/{trace_id}", timeout=20)
    response.raise_for_status()
    return response.json()


def _flatten_trace(trace_id, trace):
    rows = []
    for batch in trace.get("batches", []):
        resource_attrs = _attrs_to_dict(batch.get("resource", {}).get("attributes", []))
        service_name = resource_attrs.get("service.name", "")
        for scope_span in batch.get("scopeSpans", []):
            for span in scope_span.get("spans", []):
                attrs = _attrs_to_dict(span.get("attributes", []))
                rows.append(
                    {
                        "traceId": trace_id,
                        "spanId": span.get("spanId", ""),
                        "parentSpanId": span.get("parentSpanId", ""),
                        "serviceName": service_name,
                        "name": span.get("name", ""),
                        "startTime": _format_time(span.get("startTimeUnixNano")),
                        "startTimeMs": _timestamp_ms(span.get("startTimeUnixNano")),
                        "durationMs": round(
                            (_timestamp_ms(span.get("endTimeUnixNano")) - _timestamp_ms(span.get("startTimeUnixNano"))),
                            3,
                        ),
                        "pokemonId": attrs.get("pokemon.id", ""),
                        "pokemonIds": attrs.get("pokemon.ids", ""),
                        "splitterId": attrs.get("splitter.id") or attrs.get("spillerid", ""),
                        "attributes": attrs,
                    }
                )
    return rows


def _trace_link(trace_id):
    return (
        f"{GRAFANA_URL}/explore?schemaVersion=1&panes="
        f'{{"pokemon":{{"datasource":"tempo-ds","queries":[{{"refId":"A","query":"{trace_id}",'
        f'"queryType":"traceql","datasource":{{"type":"tempo","uid":"tempo-ds"}}}}],'
        f'"range":{{"from":"now-6h","to":"now"}}}}}}&orgId=1'
    )


@app.get("/api/pokemon/{pokemon_id}")
def get_pokemon_spans(pokemon_id: str, lookback_seconds: int = DEFAULT_LOOKBACK_SECONDS):
    try:
        traces = _search_traces(pokemon_id, lookback_seconds)
        rows = []
        for trace in traces:
            trace_id = trace.get("traceID")
            if not trace_id:
                continue
            rows.extend(_flatten_trace(trace_id, _fetch_trace(trace_id)))

        matching_rows = [
            row for row in rows if _span_matches_pokemon(row["attributes"], pokemon_id)
        ]
        matching_rows.sort(key=lambda row: row["startTimeMs"])
        return {
            "pokemonId": pokemon_id,
            "traceCount": len(traces),
            "spanCount": len(matching_rows),
            "spans": matching_rows,
        }
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(
        """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Pokemon Trace Dashboard</title>
  <style>
    body { margin: 0; font-family: Inter, Arial, sans-serif; background: #0f172a; color: #e5e7eb; }
    main { padding: 24px; }
    .toolbar { display: flex; gap: 10px; align-items: center; margin-bottom: 18px; }
    input, button { height: 36px; border-radius: 6px; border: 1px solid #334155; background: #111827; color: #e5e7eb; padding: 0 10px; }
    button { cursor: pointer; background: #2563eb; border-color: #2563eb; font-weight: 600; }
    .summary { margin-bottom: 16px; color: #cbd5e1; }
    table { width: 100%; border-collapse: collapse; background: #111827; }
    th, td { border-bottom: 1px solid #1f2937; padding: 8px 10px; text-align: left; font-size: 13px; vertical-align: top; }
    th { position: sticky; top: 0; background: #0b1220; color: #93c5fd; }
    code { color: #bfdbfe; }
    a { color: #93c5fd; }
    .muted { color: #94a3b8; }
  </style>
</head>
<body>
  <main>
    <div class="toolbar">
      <label for="pokemonId">Pokemon ID</label>
      <input id="pokemonId" value="25" />
      <button onclick="loadSpans()">Search</button>
    </div>
    <div id="summary" class="summary"></div>
    <table>
      <thead>
        <tr>
          <th>Time</th><th>Service</th><th>Span</th><th>Duration ms</th>
          <th>Pokemon</th><th>Splitter ID</th><th>Trace</th>
        </tr>
      </thead>
      <tbody id="rows"></tbody>
    </table>
  </main>
  <script>
    async function loadSpans() {
      const pokemonId = document.getElementById('pokemonId').value.trim();
      document.getElementById('summary').textContent = 'Loading...';
      const response = await fetch(`/api/pokemon/${encodeURIComponent(pokemonId)}`);
      const data = await response.json();
      if (!response.ok) {
        document.getElementById('summary').textContent = data.detail || 'Failed to load spans';
        return;
      }
      document.getElementById('summary').textContent =
        `Pokemon ${data.pokemonId}: ${data.spanCount} spans across ${data.traceCount} trace(s)`;
      document.getElementById('rows').innerHTML = data.spans.map((span) => `
        <tr>
          <td>${span.startTime}</td>
          <td>${span.serviceName}</td>
          <td><code>${span.name}</code><div class="muted">${span.spanId}</div></td>
          <td>${span.durationMs}</td>
          <td>${span.pokemonId || span.pokemonIds}</td>
          <td><code>${span.splitterId || ''}</code></td>
          <td><a target="_blank" href="/trace/${span.traceId}">${span.traceId}</a></td>
        </tr>
      `).join('');
    }
    loadSpans();
  </script>
</body>
</html>
        """
    )


@app.get("/trace/{trace_id}")
def redirect_trace(trace_id: str):
    return HTMLResponse(
        f'<script>window.location.href = "{_trace_link(trace_id)}";</script>'
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8090"))
    uvicorn.run(app, host="0.0.0.0", port=port)
