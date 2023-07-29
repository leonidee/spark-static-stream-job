kafka.create-topic:
	docker exec -it kafka \
		/opt/bitnami/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--topic $(topic-name) --create \
		--partitions 3 --replication-factor 1

kafka.describe-topic:
	docker exec -it kafka \
		/opt/bitnami/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--describe --topic $(topic-name)

kafka.list-topics:
	docker exec -it kafka \
		/opt/bitnami/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--list

kafkacat.run-producer:
	kafkacat -b $(curl -s ipinfo.io/ip):9092 \
		-t $(topic-name) -P -K:

kafkacat.run-consumer:
	kafkacat -b $(curl -s ipinfo.io/ip):9092 \
		-t $(topic-name) -C -o begining \
		-f 'Message Key: %k\nMessage Value: %s\nPartition: %p\nOffset: %o\nTimestamp: %T\n'

spark.run-job:
	docker exec -it spark-master \
		/app/spark/run.sh $(job-name)