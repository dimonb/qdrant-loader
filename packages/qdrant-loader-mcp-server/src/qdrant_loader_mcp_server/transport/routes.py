"""FastAPI router for MCP HTTP transport endpoints."""

import asyncio
import json
import time
import uuid
from typing import Any

from fastapi import APIRouter, Depends, Request, Response
from fastapi.responses import StreamingResponse

from ..utils.logging import LoggingConfig
from .dependencies import get_mcp_handler, validate_origin

logger = LoggingConfig.get_logger(__name__)

mcp_router = APIRouter()

# Session store for SSE transport: session_id -> asyncio.Queue
# Each queue carries JSON-serialisable dicts to be forwarded as SSE data events.
_sse_sessions: dict[str, asyncio.Queue] = {}
_SSE_HEARTBEAT_INTERVAL = 15.0  # seconds


@mcp_router.post("/mcp", dependencies=[Depends(validate_origin)])
async def handle_mcp_post(
    request: Request,
    mcp_handler=Depends(get_mcp_handler),
):
    """Handle client-to-server messages via HTTP POST (streamable-HTTP transport)."""
    try:
        body = await request.json()
    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body")
        return {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -32700, "message": "Invalid JSON in request"},
        }

    try:
        if not isinstance(body, dict):
            return {
                "jsonrpc": "2.0",
                "id": None,
                "error": {"code": -32600, "message": "Invalid Request"},
            }
        logger.debug("Processing MCP request: %s", body.get("method", "unknown"))
        response = await mcp_handler.handle_request(body, headers=dict(request.headers))
        logger.debug("Successfully processed MCP request")
        return response
    except Exception:
        logger.error("Error processing MCP request", exc_info=True)
        return {
            "jsonrpc": "2.0",
            "id": body.get("id") if isinstance(body, dict) else None,
            "error": {"code": -32603, "message": "Internal server error"},
        }


@mcp_router.get("/mcp", dependencies=[Depends(validate_origin)])
async def handle_mcp_get():
    """SSE stub -- heartbeat-only stream for keep-alive (streamable-HTTP transport)."""

    async def heartbeat():
        try:
            while True:
                yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': time.time()})}\n\n"
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            logger.debug("SSE heartbeat stream cancelled")
            raise

    return StreamingResponse(
        heartbeat(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# Legacy SSE transport (MCP 2024-11-05 spec) — required by context-forge /
# any client using mcp.client.sse.sse_client
# ---------------------------------------------------------------------------

@mcp_router.get("/sse")
async def handle_sse_connect(request: Request):
    """Open an SSE channel and advertise the per-session POST endpoint.

    Implements the legacy MCP HTTP+SSE transport:
      1. Client opens GET /sse → receives ``event: endpoint`` with the session URL.
      2. Client POSTs JSON-RPC messages to ``/messages/{session_id}``.
      3. Responses arrive back through this SSE stream.
    """
    session_id = str(uuid.uuid4())
    queue: asyncio.Queue[Any] = asyncio.Queue()
    _sse_sessions[session_id] = queue

    # Build absolute URL for the messages endpoint so it works behind proxies.
    base = str(request.base_url).rstrip("/")
    endpoint_url = f"{base}/messages/{session_id}"
    logger.info("New SSE session", session_id=session_id, endpoint=endpoint_url)

    async def event_stream():
        try:
            # Step 1: tell the client where to POST messages.
            yield f"event: endpoint\ndata: {endpoint_url}\n\n"

            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=_SSE_HEARTBEAT_INTERVAL)
                    if item is None:
                        # Sentinel: session closed
                        break
                    yield f"data: {json.dumps(item)}\n\n"
                except asyncio.TimeoutError:
                    # Keep-alive heartbeat
                    yield f": heartbeat {time.time()}\n\n"
        except asyncio.CancelledError:
            logger.debug("SSE session cancelled", session_id=session_id)
        finally:
            _sse_sessions.pop(session_id, None)
            logger.info("SSE session closed", session_id=session_id)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@mcp_router.post("/messages/{session_id}")
async def handle_sse_message(
    session_id: str,
    request: Request,
    mcp_handler=Depends(get_mcp_handler),
):
    """Receive a JSON-RPC message for an active SSE session and push the response back."""
    queue = _sse_sessions.get(session_id)
    if queue is None:
        logger.warning("Message for unknown SSE session", session_id=session_id)
        return Response(status_code=404, content="Session not found")

    try:
        body = await request.json()
    except json.JSONDecodeError:
        error = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Invalid JSON"}}
        await queue.put(error)
        return Response(status_code=202)

    try:
        logger.debug("SSE session message", session_id=session_id, method=body.get("method"))
        response = await mcp_handler.handle_request(body, headers=dict(request.headers))
        await queue.put(response)
    except Exception:
        logger.error("Error handling SSE message", session_id=session_id, exc_info=True)
        error = {
            "jsonrpc": "2.0",
            "id": body.get("id") if isinstance(body, dict) else None,
            "error": {"code": -32603, "message": "Internal server error"},
        }
        await queue.put(error)

    # Return 202 Accepted — the actual response comes through the SSE stream.
    return Response(status_code=202)
