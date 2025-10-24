# UTS — Pub-Sub Log Aggregator with Idempotent Consumer & Deduplication

Instruksi singkat:

## Build & Run (tanpa Docker)
1. python -m venv .venv
2. .venv\Scripts\activate
3. pip install -r requirements.txt
4. python -m src.main
API: http://127.0.0.1:8080

## Build & Run (Docker)
1. docker build -t uts-aggregator .
2. docker run --rm -p 8080:8080 -v "%cd%/data:/app/data" uts-aggregator

## Endpoints
- POST /publish  (single object atau array)
- GET /events?topic=...
- GET /stats

## Tests
pytest -q

## Stress test
python scripts/stress_test.py --url http://127.0.0.1:8080 --count 5000 --dup-rate 0.2
