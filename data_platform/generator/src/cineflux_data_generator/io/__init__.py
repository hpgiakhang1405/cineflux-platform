"""Group external I/O gateways for storage and messaging systems.

Reads: serialized records, runtime endpoints, and destination metadata.
Writes: Parquet objects to MinIO and Avro messages to Kafka.
Runs: dependency composition from generator modes and runners.
"""
