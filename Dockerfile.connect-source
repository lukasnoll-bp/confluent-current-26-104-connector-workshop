FROM confluentinc/cp-kafka-connect:8.2.0

# ── IEC 104 Source Connector ──────────────────────────────────────────────
# Drop the shaded JAR into the default Kafka Connect plugin path.
# Replace this file whenever a new connector release is built.
COPY connectors/Iec104SourceConnector-1.0-SNAPSHOT-shaded.jar \
     /usr/share/java/Iec104SourceConnector.jar

# ── JMX Prometheus exporter agent ────────────────────────────────────────
# Baked into the image so no host volume is required for the JAR itself.
# The scrape config (config.yml) is still volume-mounted so it can be
# changed without rebuilding the image.
COPY jmx-exporter/jmx_prometheus_javaagent-1.0.1.jar \
     /opt/jmx-exporter/jmx_prometheus_javaagent-1.0.1.jar
