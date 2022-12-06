init:
	mvn archetype:generate                \
		-DarchetypeGroupId=org.apache.flink   \
		-DarchetypeArtifactId=flink-quickstart-java \
		-DarchetypeVersion=1.16.0

build:
	mvn clean package

run: 
	# /home/annd2/Downloads/flink-1.16.0/bin/flink run ./target/jaeger-analytics-1.0-SNAPSHOT.jar

	/home/annd2/Downloads/flink-1.16.0/bin/flink run ./target/jaeger-analytics-1.0-SNAPSHOT.jar --input /home/annd2/Documents/SPM/jaeger-analytics/text.txt --output /home/annd2/Documents/SPM/jaeger-analytics/output