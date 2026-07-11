import os
import time

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request

from src.utils.service_logger import ServiceLogger
from src.utils.tracer import (
    HTTP_ITEM_ID_HEADER,
    ITEM_ID_HEADER,
    JSON_TRACE_ID_HEADER,
    JSON_TRACE_ALIAS_HEADER,
    extract_text_map_context,
    init_tracer,
    set_active_span_item_tags,
    set_active_span_json_trace_tags,
    set_current_span_error,
    start_span,
)


SURNAME_FRENCH_NAMES = {
    "oak": "Chene",
    "elm": "Orme",
    "birch": "Bouleau",
    "rowan": "Sorbier",
    "juniper": "Genevrier",
    "sycamore": "Sycomore",
    "kukui": "Bancoulier",
    "magnolia": "Magnolia",
    "willow": "Saule",
    "maple": "Erable",
}


app = FastAPI(title="Pokemon Surname French API")
service_logger = None


def get_service_logger():
    global service_logger
    if service_logger is None:
        init_tracer()
        service_logger = ServiceLogger()
        service_logger.log_service_config()
        service_logger.set_logstash_handler()
    return service_logger


def translate_surname(surname):
    french_name = SURNAME_FRENCH_NAMES.get(surname.strip().lower())
    if french_name is None:
        raise HTTPException(
            status_code=404,
            detail=f"No French name configured for surname '{surname}'",
        )
    return french_name


def get_parent_context(request):
    return extract_text_map_context({key: value for key, value in request.headers.items()})


def get_request_item_id(request, context):
    return request.headers.get(HTTP_ITEM_ID_HEADER) or request.headers.get(ITEM_ID_HEADER)


def get_request_json_trace_id(request, context):
    return request.headers.get(JSON_TRACE_ID_HEADER) or request.headers.get(JSON_TRACE_ALIAS_HEADER)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/surname/{surname}/french")
def get_surname_french_name(surname: str, request: Request):
    return translate(request, surname)


@app.get("/translate")
def translate(request: Request, surname: str = Query(..., min_length=1)):
    start_timestamp = time.time()
    logger = get_service_logger()
    parent_context = get_parent_context(request)
    pokemon_id = get_request_item_id(request, parent_context)
    json_trace_id = get_request_json_trace_id(request, parent_context)

    with start_span(
        "surname_api_translate",
        parent_context=parent_context,
    ):
        if pokemon_id:
            set_active_span_item_tags([pokemon_id])
        if json_trace_id:
            set_active_span_json_trace_tags([json_trace_id])

        logger.reset_aggregated_log()
        logger.log_trace_id()
        logger.add_field("requestId", f"surname-api-{surname.lower()}-{int(start_timestamp)}")
        logger.add_field("surname", surname)
        logger.add_field("httpMethod", request.method)
        logger.add_field("httpPath", request.url.path)
        if pokemon_id:
            logger.add_field("entityId", pokemon_id)
        if json_trace_id:
            logger.add_field("jsonTraceId", json_trace_id)

        try:
            french_name = translate_surname(surname)
            logger.add_field("frenchName", french_name)
            logger.info(f"Translated surname {surname} to French name {french_name}")
            logger.log_success_logstash(start_timestamp)
            return {
                "surname": surname,
                "frenchName": french_name,
            }
        except HTTPException as exc:
            set_current_span_error(str(exc.detail))
            logger.log_error(str(exc.detail), start_timestamp)
            raise


if __name__ == "__main__":
    get_service_logger()
    port = int(os.environ.get("PORT", "8082"))
    uvicorn.run(app, host="0.0.0.0", port=port)
