
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


If you want to customize kafka or Spark configuration you can edit file in ./config directory or put your file. Make sure to 



# How to deploy

Create `.env` in root directory and set required invironment variables listed in `.env.tempalte`.

Generate actual advertisment campaings data for todays date:

```shell
make up-generator
```
This command will up docker container with generator, thats will creates some data and store on s3.

Up container with kafka:

```shell
make up-kafka
```
Create topics for input and output:

```shell
make create-topic topic=test.in \
  make create-topic topic=test.out
```

Up containers with Spark master and 5 workers:

```shell
make up-spark 
```

Submit Spark streaming job:

```shell
make run-job job=some.py
```

