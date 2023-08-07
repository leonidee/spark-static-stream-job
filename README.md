
To show project's `make` help page just type:

```shell
make help
```


You will see something like that:


```shell
Usage: make [COMMAND]... [OPTIONS]...

Comands for kafka running in container:
    make create-topic topic=<str>                    Create new topic in kafka cluster
    make describe-topic topic=<str>                  Show descripiton of given topic
    make list-topics                                 Show list of all created topics in kafka cluster

kafkacat commands:
    make run-producer topic=<str>                    Run kafkacat in producer mode
    make run-consumer topic=<str>                    Run kafkacat in consumer mode

Commands for Spark:
    make run-job job=<str>                           Run one of the Spark jobs listed in ./jobs folder

Docker compose commands:
    make up-generator                                Generate advertisment campaings data for today's date
    make up-producer                                 Up data producer to generate client's moving locations
    make up-kafka                                    Up kafka in container
    make up-spark                                    Up Spark cluster with 5 workers in containers

```


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
Create topics for clients location data producer:

```shell
make create-topic topic=clients.client-routes-locations.export
```
And Streaming application output:

```shell
make create-topic topic=adv-push.actual-adv.export
```
Up containers with Spark master and 5 workers:

```shell
make up-spark 
```

Submit Spark streaming job:

```shell
make run-job job=collect-streaming-query.py
```

Now you can run kafkacat in producer mode to see messages producer buy streaming application:

```shell
make run-consumer topic=adv-push.actual-adv.export
```


For debugging purposes specify flag in `config.yaml` file `is_debug` to `true`

```yaml
# Apps configurations
apps:
  spark:
    is_debug: true # <----- here
    clients-locations-topic: *clients-locations-topic
```

Now all result producing by Streaming Query will be printed to console.