init:
	mvn archetype:generate                \
		-DarchetypeGroupId=org.apache.flink   \
		-DarchetypeArtifactId=flink-quickstart-java \
		-DarchetypeVersion=1.16.0

build:
	mvn clean package

run: 
	/home/annd2/Downloads/flink-1.16.0/bin/flink run ./target/jaeger-analytics-1.0-SNAPSHOT.jar --config-file ./src/main/resources/config.properties

all: build run

client:
	/home/annd2/Downloads/kafka_2.13-3.3.1/bin/kafka-console-consumer.sh \
		--bootstrap-server 103.245.251.90:9093,103.245.251.90:9094,103.245.251.90:9095 \
		--topic tracing-dev-stg \
		--group dev-stg-consumer \
		--consumer.config client-ssl.properties
		# --new-consumer \

client2:
	/home/annd2/Downloads/kafka_2.13-2.8.1/bin/kafka-console-consumer.sh \
		--bootstrap-server 103.245.251.90:9093,103.245.251.90:9094,103.245.251.90:9095 \
		--topic tracing-dev-stg \
		--group dev-stg-consumer \
		--consumer.config client-ssl.properties
		# --new-consumer \

tail:
	tail -n 1 /home/annd2/Downloads/flink-1.16.0/log/flink-annd2-taskexecutor-*.out -f

example:
	cd /home/annd2/Downloads/flink-1.16.0 && ./bin/flink run examples/streaming/WordCount.jar


up:
	docker compose up -d

down:
	docker compose down

copy:
	scp -r -P 234 /home/annd2/Documents/SPM/jaeger-analytics annd2@58.84.2.245:/home/annd2/flink

start:
	cd /home/annd2/flink/flink-1.16.0 && ./bin/start-cluster.sh
stop:
	cd /home/annd2/flink/flink-1.16.0 && ./bin/stop-cluster.sh
