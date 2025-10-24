from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi import Request
from fastapi.responses import JSONResponse
from src.schemas import Event
from src.db import DB
from src.consumer import Consumer
from src.utils import logger
import asyncio
import time
import json
from typing import List, Dict, Any, Optional

app = FastAPI(title="Pub-Sub Log Aggregator (UTS)")

# Initialize application state
app.state.queue = asyncio.Queue()
app.state.db = DB()  # persistent sqlite file ./data/aggregator.db
app.state.start_time = time.time()
app.state.stats = {
    "received": 0,
    "unique_processed": 0,
    "duplicate_dropped": 0,
    "topics": set()
}
app.state.consumer = Consumer(app.state.queue, app.state.db, app.state.stats)

@app.on_event("startup")
async def startup_event():
    logger.info("Starting app, starting consumer")
    await app.state.consumer.start()

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down: waiting consumer stop")
    await app.state.consumer.stop()
    app.state.db.close()

@app.post("/publish")
async def publish(payload: Optional[List[dict]] = None, request: Request = None):
    """
    Accept single event (object) or batch (list of objects).
    """
    body = await request.json()
    items = body if isinstance(body, list) else [body]
    inserted_count = 0
    for raw in items:
        # Validate via pydantic
        try:
            evt = Event(**raw)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
        app.state.stats["received"] += 1
        app.state.stats["topics"].add(evt.topic)
        # At-least-once: push to internal queue, processing will check dedup
        await app.state.queue.put(evt.dict())
        inserted_count += 1
    return {"accepted": inserted_count}

@app.get("/events")
def get_events(topic: str = Query(..., min_length=1)):
    rows = app.state.db.list_events(topic)
    items = []
    for row in rows:
        topic_r, event_id, timestamp, source, payload, processed_at = row
        try:
            payload_obj = json.loads(payload) if payload else {}
        except:
            payload_obj = {}
        items.append({
            "topic": topic_r,
            "event_id": event_id,
            "timestamp": timestamp,
            "source": source,
            "payload": payload_obj,
            "processed_at": processed_at
        })
    return {"count": len(items), "events": items}

@app.get("/stats")
def get_stats():
    topics_count = len(app.state.stats["topics"])
    uptime = time.time() - app.state.start_time
    return {
        "received": app.state.stats["received"],
        "unique_processed": app.state.stats["unique_processed"],
        "duplicate_dropped": app.state.stats["duplicate_dropped"],
        "topics_count": topics_count,
        "topics": list(app.state.stats["topics"]),
        "uptime_seconds": uptime
    }
