.PHONY: kafka.create-topic kafka.describe-topic kafka.list-topics kafkacat.run-producer kafkacat.run-consumer spark.run

help:
	@echo "Usage: make [COMMAND]... [OPTIONS]...\n"
	@echo "Commands for kafka:"
	@echo "  create-topic topic-name=<str>				Builds the docker images for the docker-compose setup"
	@echo "  describe-topic topic-name=<str>			Stops and removes all docker containers"
	@echo "  list-topics						Compile dependencies from 'requirements.in' into 'requirements.txt'"
	@echo "\nCommands for kafkacat utility:"
	@echo "  run-producer topic-name=<str>				Run kafkacat in producer mode"
	@echo "  run-consumeir topic-name=<str> 	 		Run kafkacat in consumer mode"
	@echo "\nCommands for Spark:"
	@echo "  run-job job-name=<str>				Run one of the Spark jobs listed in ./jobs directory"
	@echo "\nCommands for Generator:"
	@echo " generate-adv-campaigns					Generate advertisment campaings data for today's date"

create-topic:
	docker exec -it kafka \
		/opt/bitnami/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--topic $(topic-name) --create \
		--partitions 3 --replication-factor 1

describe-topic:
	docker exec -it kafka \
		/opt/bitnami/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--describe --topic $(topic-name)

list-topics:
	docker exec -it kafka \
		/opt/bitnami/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--list

run-producer:
	kafkacat -b $(curl -s ipinfo.io/ip):9092 \
		-t $(topic-name) -P -K:

run-consumer:
	kafkacat -b $(curl -s ipinfo.io/ip):9092 \
		-t $(topic-name) -C -o begining \
		-f 'Message Key: %k\nMessage Value: %s\nPartition: %p\nOffset: %o\nTimestamp: %T\n'

run-job:
	docker exec -it spark-master \
		/app/spark/run.sh $(job-name)

generate-adv-campaigns:
	docker build -t generator -f ./docker/generator/Dockerfile .
	docker run --rm -it \
		--name generator \
		--volume ./src:/app/src \
		--volume ./.env:/app/.env \
      	--volume ./generator:/app/generator \
    	--volume ./config.yaml:/app/config.yaml \
		generator  /app/generator/run.sh
