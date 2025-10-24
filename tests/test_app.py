import sys, os
import pytest
import asyncio
from fastapi.testclient import TestClient
from asyncio import TimeoutError

# Tambahkan src ke Python path
sys.path.append(os.path.join(os.getcwd()))

async def ensure_queue_processed(timeout=1.0):
    """Helper function to ensure queue is processed with timeout"""
    try:
        await asyncio.wait_for(app.state.consumer.wait_processed(), timeout=timeout)
        # Give a small delay for DB operations to complete
        await asyncio.sleep(0.1)
    except TimeoutError:
        pytest.fail(f"Queue processing timed out after {timeout} seconds")
from src.app import app
from src.db import DB
from src.consumer import Consumer

client = TestClient(app)


@pytest.fixture(autouse=True)
async def cleanup_db():
    """Setup and cleanup for each test"""
    # Initialize app state first
    # Stop existing consumer if running
    if hasattr(app.state, 'consumer'):
        await app.state.consumer.stop()

    # Ensure data directory exists
    db_path = os.getenv("DB_PATH", "./data/aggregator.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Reset state with fresh objects
    app.state.queue = asyncio.Queue()
    app.state.db = DB(db_path)  # Explicitly pass db_path
    app.state.stats = {
        "received": 0,
        "unique_processed": 0,
        "duplicate_dropped": 0,
        "topics": set()
    }
    app.state.consumer = Consumer(app.state.queue, app.state.db, app.state.stats)
    
    # Remove old database if exists
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except PermissionError:
            await asyncio.sleep(0.1)  # Use async sleep
            os.remove(db_path)
    
    # Start consumer and wait a bit to ensure it's ready
    await app.state.consumer.start()
    await asyncio.sleep(0.1)  # Small delay to ensure consumer is running
    
    try:
        yield
    finally:
        # Cleanup after test
        await app.state.consumer.stop()
        app.state.db.close()


@pytest.mark.asyncio
async def test_publish_valid_event():
    """Test: publish satu event valid"""
    body = [{
        "topic": "unit-topic",
        "event_id": "u001",
        "timestamp": "2025-10-24T13:30:00",
        "source": "pytest",
        "payload": {"msg": "test satu"}
    }]
    response = client.post("/publish", json=body)
    assert response.status_code == 200
    assert response.json()["accepted"] == 1
    
    # Add timeout to avoid hanging
    try:
        await asyncio.wait_for(app.state.consumer.wait_processed(), timeout=2.0)
    except asyncio.TimeoutError:
        pytest.fail("Test timed out waiting for consumer to process events")


@pytest.mark.asyncio
async def test_deduplication_effect():
    """Test: kirim event duplikat → hanya satu yang unik"""
    # Reset stats untuk test ini
    app.state.stats["received"] = 0
    app.state.stats["unique_processed"] = 0
    app.state.stats["duplicate_dropped"] = 0
    app.state.stats["topics"] = set()

    body = [{
        "topic": "unit-topic",
        "event_id": "u002",
        "timestamp": "2025-10-24T13:31:00",
        "source": "pytest",
        "payload": {"msg": "duplikat"}
    }]
    
    # Kirim event pertama dan tunggu diproses
    client.post("/publish", json=body)
    await ensure_queue_processed()
    
    # Kirim event kedua (duplikat) dan tunggu diproses
    client.post("/publish", json=body)
    await ensure_queue_processed()

    stats = client.get("/stats").json()
    assert stats["received"] == 2, f"Expected 2 events received, got {stats['received']}"
    assert stats["unique_processed"] == 1, f"Expected 1 unique event, got {stats['unique_processed']}"
    assert stats["duplicate_dropped"] == 1, f"Expected 1 duplicate dropped, got {stats['duplicate_dropped']}"
@pytest.mark.asyncio
async def test_get_events_by_topic():
    """Test: ambil event dari topic tertentu"""
    # Reset stats untuk test ini
    app.state.stats["received"] = 0
    app.state.stats["unique_processed"] = 0
    app.state.stats["duplicate_dropped"] = 0
    app.state.stats["topics"] = set()

    body = [{
        "topic": "unit-topic",
        "event_id": "u003",
        "timestamp": "2025-10-24T13:32:00",
        "source": "pytest",
        "payload": {"msg": "fetch test"}
    }]
    
    response = client.post("/publish", json=body)
    assert response.status_code == 200, "Failed to publish event"
    
    # Pastikan event diproses
    await ensure_queue_processed()

    # Verifikasi event tersimpan
    response = client.get("/events", params={"topic": "unit-topic"})
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1, f"Expected 1 event, got {data['count']}"
    assert len(data["events"]) == 1
    
    # Verifikasi konten event
    event = data["events"][0]
    assert event["event_id"] == "u003"
    assert event["topic"] == "unit-topic"
@pytest.mark.asyncio
async def test_persisted_dedup_store(tmp_path):
    """Test: simulasi restart dengan dedup store persist"""
    # Reset stats untuk test ini
    app.state.stats["received"] = 0
    app.state.stats["unique_processed"] = 0
    app.state.stats["duplicate_dropped"] = 0
    app.state.stats["topics"] = set()

    store_file = tmp_path / "dedup_test.db"
    if store_file.exists():
        os.remove(store_file)

    # Set DB path untuk test ini
    os.environ["DB_PATH"] = str(store_file)

    body = [{
        "topic": "persist-topic",
        "event_id": "p001",
        "timestamp": "2025-10-24T13:33:00",
        "source": "pytest",
        "payload": {"msg": "persist test"}
    }]
    
    # Kirim event pertama
    response = client.post("/publish", json=body)
    assert response.status_code == 200, "Failed to publish first event"
    await ensure_queue_processed()

    # Verifikasi event pertama diproses
    stats = client.get("/stats").json()
    assert stats["unique_processed"] == 1, "First event should be processed"

    # Kirim event yang sama (duplikat)
    response = client.post("/publish", json=body)
    assert response.status_code == 200, "Failed to publish duplicate event"
    await ensure_queue_processed()

    # Verifikasi duplikat terdeteksi
    stats = client.get("/stats").json()
    assert stats["duplicate_dropped"] == 1, "Duplicate event should be dropped"
    assert stats["unique_processed"] == 1, "Should still have only one unique event"
@pytest.mark.asyncio
async def test_batch_publish():
    """Test: kirim batch 5 event"""
    batch = []
    for i in range(5):
        batch.append({
            "topic": "batch-topic",
            "event_id": f"b{i}",
            "timestamp": "2025-10-24T13:34:00",
            "source": "pytest",
            "payload": {"msg": f"batch {i}"}
        })
    response = client.post("/publish", json=batch)
    assert response.status_code == 200
    assert response.json()["accepted"] == 5
    await app.state.consumer.wait_processed()
