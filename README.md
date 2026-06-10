## streamvault E-Commerce Data Platform

End-to-end streaming + lakehouse pipeline on the Olist Brazil dataset.

## Phase 1 — Kafka / Redpanda

### Project structure
```
olist-platform/
├── docker-compose.redpanda.yml   # Redpanda broker + Console
├── console-config.yaml           # Console config (auto-used by compose)
├── requirements.txt
├── data/
│   └── raw/                      # ← put all Olist CSVs here
└── kafka/
    ├── create_topics.py          # run once after Redpanda starts
    └── producer.py               # replay producer
```

### Step 1 — Put the CSVs in place
```
data/raw/
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_customers_dataset.csv
└── olist_products_dataset.csv
```
(geolocation, sellers, reviews not needed for Phase 1)

### Step 2 — Start Redpanda
```bash
docker compose -f docker-compose.redpanda.yml up -d
```
This runs on its own isolated network (redpanda_net) — will not affect
any existing Docker projects on your machine.

Wait ~15 seconds for the healthcheck to pass, then open:
- **Redpanda Console UI**: http://localhost:8080

### Step 3 — Install Python dependencies
```bash
pip install -r requirements.txt
```

### Step 4 — Create Kafka topics
```bash
python kafka/create_topics.py
```
Expected output:
```
Creating Olist Kafka topics...
  ✓ Created topic: olist.orders
  ✓ Created topic: olist.order_items
  ✓ Created topic: olist.order_payments
  ✓ Created topic: olist.customers
  ✓ Created topic: olist.products
Done.
```
You can also verify in the Console UI under Topics.

### Step 5 — Run the replay producer
```bash
# Fast mode (default — 0.05s delay = ~20 msg/sec per topic)
python kafka/producer.py --data-dir data/raw

# Slow mode (watch messages arrive in real time in the UI)
python kafka/producer.py --data-dir data/raw --delay 1.0
```

Open http://localhost:8080 → Topics → olist.orders → Messages
to watch messages arriving live.

### Stop Redpanda
```bash
docker compose -f docker-compose.redpanda.yml down
```
Data is persisted in the `redpanda_data` Docker volume — topics and
messages survive restarts.

To wipe everything:
```bash
docker compose -f docker-compose.redpanda.yml down -v
```

## Topics

| Topic | Source | Key field | Partitions |
|---|---|---|---|
| olist.orders | olist_orders_dataset.csv | order_id | 3 |
| olist.order_items | olist_order_items_dataset.csv | order_id | 3 |
| olist.order_payments | olist_order_payments_dataset.csv | order_id | 3 |
| olist.customers | olist_customers_dataset.csv | customer_id | 3 |
| olist.products | olist_products_dataset.csv | product_id | 3 |

## Message format
Every message is JSON with the original CSV fields plus audit metadata:
```json
{
  "order_id": "e481f51cbdc54678b7cc49136f2d6af7",
  "customer_id": "9ef432eb6251297304e76186b10a928d",
  "order_status": "delivered",
  "order_purchase_timestamp": "2017-10-02 10:56:33",
  "_topic": "olist.orders",
  "_ingested_at": "2024-01-15T08:23:11.432Z",
  "_event_id": "a3f2c1d4-8e7b-4a2f-9c1d-3e5f7a8b9c0d"
}
```
The `_ingested_at` and `_event_id` fields are added by the producer —
they're how the bronze layer tracks when data entered the pipeline.
