# Laporan UTS — Pub-Sub Log Aggregator

## Ringkasan sistem dan arsitektur
(isi: diagram sederhana, keputusan desain: FastAPI, asyncio.Queue, SQLite dedup store)

## Keputusan desain
- Idempotency: consumer menulis ke table dedup (topic,event_id) PK
- Dedup store: SQLite (persisten pada `./data/aggregator.db`)
- Ordering: (jelaskan kebutuhan/ketidakperluan total ordering untuk log aggregator)

## Analisis performa dan metrik
Metrik: throughput (events/sec), latency (publish→processed), duplicate rate.

## Kaitan ke Bab 1–7 (Tanenbaum & Van Steen)
(T1..T8 diisi sesuai instruksi, sertakan sitasi APA ed. ke-7)

## Referensi
Tanenbaum, A. S., & Van Steen, M. (tahun). *Distributed Systems: Principles and Paradigms*. Penerbit.
