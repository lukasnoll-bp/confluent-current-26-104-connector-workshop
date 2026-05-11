FROM confluentinc/cp-kafka-connect:8.0.0

COPY "./target/Iec104SourceConnector.jar"  "/usr/share/java/Iec104SourceConnector.jar"
