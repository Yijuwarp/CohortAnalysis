from fastapi import APIRouter, Depends, HTTPException, Query
import duckdb
from typing import Any
from datetime import datetime
import logging
from app.db.connection import get_connection
from app.domains.analytics.retention_service import get_retention
from app.domains.analytics.usage_service import (
    list_events,
    get_event_properties,
    get_event_property_values,
    get_usage,
    get_usage_frequency,
)
from app.domains.analytics.monetization_service import get_monetization
from app.domains.analytics.flow_service import get_flow_graph, get_l1_flows, get_l2_flows
from app.domains.analytics.user_explorer_service import get_user_explorer, search_users
from app.utils.parsing import parse_max_day

router = APIRouter()
logger = logging.getLogger(__name__)
MAX_DEPTH = 20

@router.get("/retention")
async def retention_endpoint(
    max_day: Any = Query(7),
    retention_event: str | None = Query(None),
    include_ci: bool = Query(False),
    confidence: float = Query(0.95),
    retention_type: str = Query("classic"),
    granularity: str = Query("day"),
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    parsed_max_day = parse_max_day(max_day)
    return get_retention(conn, parsed_max_day, retention_event, include_ci, confidence, granularity=granularity, retention_type=retention_type)

@router.get("/events")
async def list_events_endpoint(
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return list_events(conn)

@router.get("/events/{event_name}/properties")
async def event_properties_endpoint(
    event_name: str,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return get_event_properties(conn, event_name)

@router.get("/events/{event_name}/properties/{property}/values")
async def event_property_values_endpoint(
    event_name: str,
    property: str,
    limit: int = 25,
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return get_event_property_values(conn, event_name, property, limit)

@router.get("/usage")
async def usage_endpoint(
    event: str,
    max_day: Any = Query(7),
    retention_event: str | None = Query(None),
    property: str | None = Query(None),
    operator: str = Query("="),
    value: str | None = Query(None),
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    parsed_max_day = parse_max_day(max_day)
    return get_usage(conn, event, parsed_max_day, retention_event, property, operator, value)

@router.get("/usage-frequency")
async def usage_frequency_endpoint(
    event: str,
    property: str | None = Query(None),
    operator: str = Query("="),
    value: str | None = Query(None),
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return get_usage_frequency(conn, event, property, operator, value)

@router.get("/monetization")
async def monetization_endpoint(
    max_day: Any = Query(7),
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    parsed_max_day = parse_max_day(max_day)
    return get_monetization(conn, parsed_max_day)

@router.get("/flow/l1")
async def flow_l1_endpoint(
    start_event: str = Query(...),
    direction: str = Query("forward"),
    depth: int = Query(2),
    property_column: str | None = Query(None),
    property_operator: str = Query("="),
    property_values: list[str] | None = Query(None),
    include_top_k: bool = Query(True),
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    depth = min(max(2, depth), MAX_DEPTH)
    if not property_column or not property_values:
        property_values = None
    try:
        return get_l1_flows(conn, start_event, direction, depth, property_column, property_operator, property_values, include_top_k)
    except Exception as e:
        logger.exception("flow_l1 failed")
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/flow/l2")
async def flow_l2_endpoint(
    start_event: str = Query(...),
    parent_path: list[str] | None = Query(None),
    parent_event: str | None = Query(None),
    direction: str = Query("forward"),
    depth: int = Query(2),
    property_column: str | None = Query(None),
    property_operator: str = Query("="),
    property_values: list[str] | None = Query(None),
    include_top_k: bool = Query(True),
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    if not property_column or not property_values:
        property_values = None
    if parent_path:
        resolved_parent_path = parent_path
    elif parent_event:
        resolved_parent_path = [start_event, parent_event]
    else:
        resolved_parent_path = [start_event]
    depth = min(max(max(2, depth), len(resolved_parent_path) + 1), MAX_DEPTH)
    try:
        return get_l2_flows(
            conn,
            start_event,
            resolved_parent_path,
            direction,
            depth,
            property_column,
            property_operator,
            property_values,
            include_top_k,
        )
    except Exception as e:
        logger.exception("flow_l2 failed")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/flow/graph")
async def flow_graph_endpoint(
    start_event: str = Query(...),
    direction: str = Query("forward"),
    depth: int = Query(3),
    property_column: str | None = Query(None),
    property_operator: str = Query("="),
    property_values: list[str] | None = Query(None),
    include_top_k: bool = Query(True),
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    graph_depth = max(1, min(depth, 10))
    if not property_column or not property_values:
        property_values = None
    try:
        return get_flow_graph(
            conn,
            start_event,
            direction,
            graph_depth,
            property_column,
            property_operator,
            property_values,
            include_top_k,
        )
    except Exception as e:
        logger.exception("flow_graph failed")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/users/search")
async def users_search_endpoint(
    query: str = Query(""),
    limit: int = Query(20),
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    return search_users(conn, query=query, limit=limit)


@router.get("/user-explorer")
async def user_explorer_endpoint(
    user_id: str = Query(...),
    page: int = Query(1),
    page_size: int = Query(50),
    event_search: str | None = Query(None),
    direction: str | None = Query(None),
    from_event_time: datetime | None = Query(None),
    jump_datetime: str | None = Query(None),
    conn: duckdb.DuckDBPyConnection = Depends(get_connection),
):
    if direction not in {None, "next", "prev"}:
        raise HTTPException(status_code=400, detail="direction must be one of: next, prev")

    return get_user_explorer(
        conn,
        user_id=user_id,
        page=page,
        page_size=page_size,
        event_search=event_search,
        direction=direction,
        from_event_time=from_event_time,
        jump_datetime_raw=jump_datetime,
    )
