help:
	@echo "Usage: make [COMMAND]... [OPTIONS]...\n"
	@echo "Comands for kafka running in container:"
	@echo "    make create-topic topic=<str>			Create new topic in kafka cluster"
	@echo "    make describe-topic topic=<str>			Show descripiton of given topic"
	@echo "    make list-topics					Show list of all created topics in kafka cluster"
	@echo "\nkafkacat commands:"
	@echo "    make run-producer topic=<str> 			Run kafkacat in producer mode"
	@echo "    make run-consumer topic=<str> 			Run kafkacat in consumer mode"
	@echo "\nCommands for Spark:"
	@echo "    make run-job job=<str>				Run one of the Spark jobs listed in ./jobs folder"
	@echo "\nDocker compose commands:"
	@echo "    make up-generator 					Generate advertisment campaings data for today's date"
	@echo "    make up-producer 					Up data producer to generate client's moving locations"
	@echo "    make up-kafka 					Up kafka in container"
	@echo "    make up-spark 					Up Spark cluster with 5 workers in containers"

# kafka commands
create-topic:
	docker exec -it kafka \
		/opt/bitnami/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--topic $(topic) --create \
		--partitions 5 --replication-factor 1

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
