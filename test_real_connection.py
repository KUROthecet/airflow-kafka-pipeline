import os
import sys

# Ensure utf-8 encoding for stdout
sys.stdout.reconfigure(encoding='utf-8')

from kafka import KafkaAdminClient
from kafka.errors import KafkaError

# Real Kafka Connection Details
bootstrap_servers = '46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294'
security_protocol = 'SASL_PLAINTEXT'
sasl_mechanism = 'PLAIN'
sasl_plain_username = 'kafka'
sasl_plain_password = 'UnigapKafka@2024'

print("Checking connection to REAL Kafka cluster...")
print(f"Servers: {bootstrap_servers}")

try:
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password,
        request_timeout_ms=10000
    )
    
    topics = admin.list_topics()
    print("\n[SUCCESS] Connected to REAL Kafka cluster!")
    print(f"Found {len(topics)} topics on the server.")
    
    if "product_view" in topics:
        print("[SUCCESS] Topic 'product_view' exists and is ready.")
    else:
        print("[ERROR] Topic 'product_view' does NOT exist.")
        
    admin.close()
    
except KafkaError as e:
    print(f"\n[ERROR] Kafka Connection Error: {e}")
except Exception as e:
    print(f"\n[ERROR] Other Exception: {e}")
