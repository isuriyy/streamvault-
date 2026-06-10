"""
create_topics.py
Run once after Redpanda starts to create all StreamVault pipeline topics.

Usage (via Docker):
    docker compose -f docker-compose.redpanda.yml run --rm producer create_topics.py
"""

from confluent_kafka.admin import AdminClient, NewTopic

# Inside Docker — talk to Redpanda via the internal Docker network
BROKER = "redpanda:9092"

# 3 partitions per topic gives PySpark Structured Streaming
# one task per partition when we get to Phase 2.
# replication_factor=1 because we run a single broker locally.
TOPICS = [
    NewTopic("olist.orders",         num_partitions=3, replication_factor=1),
    NewTopic("olist.order_items",    num_partitions=3, replication_factor=1),
    NewTopic("olist.order_payments", num_partitions=3, replication_factor=1),
    NewTopic("olist.customers",      num_partitions=3, replication_factor=1),
    NewTopic("olist.products",       num_partitions=3, replication_factor=1),
]

def main():
    admin = AdminClient({"bootstrap.servers": BROKER})
    futures = admin.create_topics(TOPICS)

    for topic, future in futures.items():
        try:
            future.result()
            print(f"  ✓ Created  : {topic}")
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"  ~ Exists   : {topic}")
            else:
                print(f"  ✗ Failed   : {topic} — {e}")

if __name__ == "__main__":
    print("StreamVault — creating Kafka topics...")
    print(f"Broker: {BROKER}\n")
    main()
    print("\nDone. Verify at http://localhost:8081 → Topics")
