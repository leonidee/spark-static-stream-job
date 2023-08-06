help:
	@echo "Usage: make [COMMAND]... [OPTIONS]...\n"
	@echo "Comands for kafka running in container:"
	@echo "    create-topic topic=<str>			Builds the docker images for the docker-compose setup"
	@echo "    describe-topic topic=<str>			Stops and removes all docker containers"
	@echo "    list-topics					Compile dependencies from 'requirements.in' into 'requirements.txt'"
	@echo "\nkafkacat commands:"
	@echo "    run-producer topic=<str> 			Run kafkacat in producer mode"
	@echo "    run-consumer topic=<str> 			Run kafkacat in consumer mode"
	@echo "\nCommands for Spark:"
	@echo "    run-job job=<str>				Run one of the Spark jobs listed in ./jobs folder"
	@echo "\nDocker compose commands:"
	@echo "    up-generator 				Generate advertisment campaings data for today's date"
	@echo "    up-producer 				Up data producer to generate user moving locations data"
	@echo "    up-kafka 					Up kafka in container"
	@echo "    up-spark 					Up Spark cluster with 5 workers"

# kafka commands
create-topic:
	docker exec -it kafka \
		/opt/bitnami/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--topic $(topic) --create \
		--partitions 3 --replication-factor 1

describe-topic:
	docker exec -it kafka \
		/opt/bitnami/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--describe --topic $(topic)

list-topics:
	docker exec -it kafka \
		/opt/bitnami/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--list

# kafkacat commands
run-producer:
	kafkacat -b $$(curl -s ipinfo.io/ip):9092 \
		-t $(topic) -P -K:

run-consumer:
	kafkacat -b $$(curl -s ipinfo.io/ip):9092 \
		-t $(topic) -C -o end \
		-f 'Message Key: %k\nMessage Value: %s\nPartition: %p\nOffset: %o\nTimestamp: %T\n'

# spark commands 
run-job:
	docker exec -it spark-master \
		/app/spark/run.sh $(job)

# docker commands
up-spark:
	docker compose up --build spark-master spark-worker-1 spark-worker-2 spark-worker-3 spark-worker-4 spark-worker-5

up-kafka:
	docker compose up kafka

up-producer:
	docker compose up producer

up-generator:
	docker compose up generator
