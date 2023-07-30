
To show project's `make` help page just type:

```shell
make help
```
You will see something like that:

```
Usage: make [COMMAND]... [OPTIONS]...

Commands for kafka:
  create-topic topic-name=<str>                         Builds the docker images for the docker-compose setup
  describe-topic topic-name=<str>                       Stops and removes all docker containers
  list-topics                                           Compile dependencies from 'requirements.in' into 'requirements.txt'

Commands for kafkacat utility:
  run-producer topic-name=<str>                         Run kafkacat in producer mode
  run-consumeir topic-name=<str>                        Run kafkacat in consumer mode

Commands for Spark:
  run-job job-name=<str>                                Run one of the Spark jobs listed in ./jobs directory
```

To run all containers listed in `docker-compose.yml` type:

```shell
docker compose up
```

Or you can run only specific continers, for example:

```shell
docker compouse up spark-master spark-worker-1 spark-worker-2
```