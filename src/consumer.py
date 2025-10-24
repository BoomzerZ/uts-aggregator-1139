import asyncio
import json
import logging
from .db import DB
from typing import Dict, Any
import time

logger = logging.getLogger("aggregator.consumer")

class Consumer:
    def __init__(self, queue: asyncio.Queue, db: DB, stats: Dict[str, Any]):
        self.queue = queue
        self.db = db
        self.stats = stats
        self._task = None
        self._running = False

    async def start(self):
        """Start the consumer"""
        if self._running:
            logger.debug("Consumer already running")
            return
            
        logger.info("Starting consumer")
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Consumer started successfully")

    async def stop(self):
        """Stop the consumer gracefully"""
        logger.info("Stopping consumer")
        self._running = False
        
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
                logger.info("Consumer stopped successfully")
            except asyncio.TimeoutError:
                logger.error("Consumer stop timed out")
            except Exception as e:
                logger.error(f"Error stopping consumer: {e}")
        else:
            logger.debug("No consumer task to stop")
            
    async def wait_processed(self):
        """Wait until all items in queue are processed"""
        if not self._running:
            return
        try:
            await self.queue.join()
        except Exception as e:
            logger.error(f"Error waiting for queue to process: {e}")
            raise

    async def _run_loop(self):
        logger.info("Consumer run loop started")
        while self._running:
            try:
                logger.debug("Waiting for event...")
                evt = await self.queue.get()
                logger.info(f"Processing event: {evt}")
                
                # evt is dict with keys: topic, event_id, timestamp, source, payload
                topic = evt["topic"]
                event_id = evt["event_id"]
                timestamp = evt["timestamp"]
                source = evt["source"]
                payload = evt.get("payload", {})
                payload_json = json.dumps(payload, ensure_ascii=False)
                
                # check dedup + mark
                inserted = self.db.mark_processed(topic, event_id, timestamp, source, payload_json)
                if inserted:
                    self.stats["unique_processed"] += 1
                    logger.info(f"Event processed: topic={topic} event_id={event_id}")
                else:
                    self.stats["duplicate_dropped"] += 1
                    logger.info(f"Duplicate dropped: topic={topic} event_id={event_id}")
                
                # always mark task done
                self.queue.task_done()
                logger.debug(f"Queue size: {self.queue.qsize()}, Stats: {self.stats}")
                await asyncio.sleep(0.01)  # Give other tasks a chance to run
            except Exception as e:
                logger.exception("Error in consumer loop: %s", e)
                self.queue.task_done()  # Ensure queue doesn't get stuck
                await asyncio.sleep(0.1)
