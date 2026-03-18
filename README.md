# Azul Dispatcher

This service acts as the event/messaging hub for Azul processing.

It is responsible for:

- Converting between the messaging/event transport (currently Kafka) and HTTP
- Abstracting the data store implementation via HTTP API
  - Provides common set of metadata generation for stored content
- Providing common workflow for event processing/plugin architecture in
  a fault tolerant and scalable manner

## API Description

These endpoints are currently under development.

| Method | Endpoint                              | Description                                                  | Response                      |
| ------ | ------------------------------------- | ------------------------------------------------------------ | ----------------------------- |
| POST   | /api/v2/event                         | Publish json event                                           |                               |
| GET    | /api/v2/event/:entity-type/passive    | Retrieve events with no side-effects                         | json containing pulled events |
| GET    | /api/v2/event/:entity-type/active     | Retrieve events and expect processing status to be published | json containing pulled events |
| POST   | /api/v3/stream/:source/:label         | Upload a data blob raw in http body                          | json metadata                 |
| HEAD   | /api/v3/stream/:source/:label/:sha256 | Check if a hash exists in the datastore                      | 200 or 404                    |
| GET    | /api/v3/stream/:source/:label/:sha256 | Download the data for the given hash                         | raw data                      |
| DELETE | /api/v3/stream/:source/:label/:sha256 | Delete the data for the given hash                           | 200 or 404                    |
| GET    | /metrics                              | Prometheus metrics                                           | Prometheus metrics            |
| GET    | /debug/pprof/:item                    | pprof debugging                                              | pprof debugging               |

### GET Events extra flags

Both `GET /api/v2/event/_entity-type_/passive` and `GET /api/v2/event/_entity-type_/active` accept a number of
parameters that alter behaviour.

| Parameter          | Description                                                                                  |
| ------------------ | -------------------------------------------------------------------------------------------- |
| name               | Plugin name                                                                                  |
| version            | Plugin version                                                                               |
| ignore-historic    | Ignore historic events before plugin existed                                                 |
| count              | Number of events to retrieve                                                                 |
| timeout            | Seconds to wait to try collecting events up to count                                         |
| requires-data      | require events to have underlying binary data                                                |
| filter             | Apply gjson filter to retrieved events. Discouraged due to difficulty getting these correct. |
| f-max-content-size | only keep events that have 'content' stream below this size (must be 0 or greater than min)  |
| f-min-content-size | only keep events that have 'content' stream above this size (must be 0 or less than max)     |
| f-allow-event-type | allow only specified event types                                                             |
| f-deny-event-type  | deny specified event types                                                                   |
| f-self             | filter out events published by this plugin                                                   |
| f-data-type        | filter data types. e.g. f-data-type=content,executable/windows/pe32,executable/windows/dll32 |

The difference between `active` and `passive` is that `active` will resend requested `binary` events to the
requestor if 'finalised' (completed/errored/optout) status messages are not published by the requestor.

### POST Events extra flags

`POST /api/v2/event` accepts parameters that modify it's behaviour:

| Parameter     | Description                                                                               |
| ------------- | ----------------------------------------------------------------------------------------- |
| sync          | Wait for confirmation that Kafka has received the published messages before returning.    |
| include_ok    | Include the message that was sent to Kafka after any transformations or enrichments.      |
| pause_plugins | Pause allowing plugins to get events for the next 10minutes after receiving this message. |

## Requirements

### golang

Install golang from https://go.dev/doc/install

```bash
curl -o go1.20.linux-amd64.tar.gz  -L https://go.dev/dl/go1.20.linux-amd64.tar.gz
rm -rf /usr/local/go && tar -C /usr/local -xzf go1.20.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

Remember to add `export PATH=$PATH:/usr/local/go/bin` to .bashrc

### debian.txt apt requirements

```bash
sudo apt install $(grep -vE "^\s*(#|$)" ./debian.txt | tr "\n" " ")
```

### libmagic

# default libmagic for debian can get out of date
contains a number of bugs for office and archive file types

```bash
git clone --depth 1 --branch FILE5_46 https://github.com/file/file
cd file/
autoreconf -f -i
./configure --disable-silent-rules
make -j4
sudo make install
sudo cp ./magic/magic.mgc /etc/magic
cd -
# check it worked
file --version
```

### yara

```bash
mkdir -p ./yara
git clone --branch v4.3.2 https://github.com/VirusTotal/yara ./yara
cd ./yara
./bootstrap.sh
./configure
make
sudo make install
cd -
## check it worked
yara -v
```

### Linting

https://github.com/golangci/golangci-lint

`golangci-lint run --timeout 5m`

## Bare Metal

Recommended for use during local development.

It is not necessary to run `go get` or `go install`.

### Build

```bash
export CGO_LDFLAGS_ALLOW="^-[Il].*$"

go build -o bin/dispatcher -tags static_all main.go
```

### Run

A docker compose configuration is provided for running all dependencies.

`docker compose up`

Dispatcher must be started with awareness of Kafka and Minio

```bash
DISPATCHER_KAFKA_ENDPOINT=localhost:9092 \
DISPATCHER_STORE_S3_ENDPOINT=localhost:9000 \
DISPATCHER_STORE_S3_ACCESS_KEY=minio-root-user \
DISPATCHER_STORE_S3_SECRET_KEY=minio-root-password \
DISPATCHER_STORE_S3_SECURE=false \
./bin/dispatcher
```

Additional commands for dispatcher (such as for reprocessing) can be
discovered using the help command:

```bash
./bin/dispatcher --help
```

#### Topic creation

Dispatcher creates a range of topics required by Azul on launch.
The default list of topics is specified in `events/default_topics.go`.

Dispatcher uses this list of topics as the base topic configuration.
The topic configuration can be overwritten using the following methods:

1. Overwrite Partition/Replica counts using `DISPATCHER_GLOBAL_PARTITION_COUNT` & `DISPATCHER_GLOBAL_REPLICA_COUNT` environment variables.
2. Overwrite topics to create using `DISPATCHER_SOURCES_HELM` environment variable.
   This environment variable should contain a list of sources for Azul, resembling the Helm sources configuration for Metastore.
   Dispatcher will automatically create a Kafka topic for each source if it has a `kafka` specification block.
3. Overwrite specific topics using a `DISPATCHER_TOPICS_YAML` env variable.

Each method specified in the list above, can be overwritten by the method specified in by the higher number.
e.g. The base config hard-coded into dispatcher can be overwritten by `DISPATCHER_GLOBAL_PARTITION_COUNT` & `DISPATCHER_GLOBAL_REPLICA_COUNT` environment variables.
Topics specified by `DISPATCHER_SOURCES_HELM` environment variable can be overwritten by `DISPATCHER_TOPICS_YAML` and so on.

```shell
# Set Topics.yaml env variable
export DISPATCHER_TOPICS_YAML="$(cat topics.yaml)"
```

### Testing

To run all available unit tests:

`go test ./...`

A bare bones integration test exists via `./test_integration.sh`.
Kafka and minio must be available, as provided by `docker-compose.yaml`.

There is a known issue where the integration test will fail on the first run,
and succeed on subsequent runs. I believe this to be a bug somewhere in dispatcher that
requires further investigation. This issue has been encountered before, usually
on first registration of a plugin.

In the future, this integration test would be best as a golang test.

#### Integration

You need to be running the docker-compose.yaml in the background as kafka and minio are needed.
Make sure they are both running properly.

To run the integration tests use the script `test_integration.sh` in the root of the project.
This sets all the necessary environment variables.
And run's the actual test command `go test ./... -count=1 -tags=integration`

NOTE: The count setting side-effect means that golang will not cache results of test.

# Docker Builds

An example dockerfile is provided for building images.
To use the container for a build run the following (or similar if your ssh private and public key for accessing Azure is in a non-standard file):

Example Build:

    docker build --build-arg SSH_PRIVATE_KEY="$(cat ~/.ssh/id_rsa)" --build-arg SSH_PUBLIC_KEY="$(cat ~/.ssh/id_rsa.pub)" .

This provides your public and private ssh keys as build arguments to the docker container.
As the built go binary is extracted from a separate stage, the final docker image will not contain your private key.

## Run

A docker compose file is provided which will start dispatcher and all required dependencies.
Uploaded binaries are stored in `./minio_data`.

`docker compose -f docker-compose-all.yaml up`

You can connect to the Dispatcher api via `http://localhost:8111`.

Test connectivity:

```bash
curl -XPOST -d @Dockerfile http://localhost:8111/api/v1/data
curl 'http://localhost:8111/api/v1/events/binary?name=mytest&version=1.0'
```

## Configuration

The dispatcher service can be tuned in multiple ways for different deployments.

This can be achieved by passing environment variables through to the process on start up.
In docker this is via the `-e` command option or in k8s via a container's `env` section.

Configuration values that describe sizes in bytes support common SI or IEC suffixes eg. "Mi", "GB", "K"

Check `./settings/settings.go` for configuration options and valid values.

# Code review

https://github.com/golang/go/wiki/CodeReviewComments

# Debugging

## Grabbing pprof metrics

You need to install graphviz to allow you to convert heap.pprof into an image.
To install it use the following command:

```bash
sudo apt install graphviz
```

To grab metrics from a pod simply put the pod name and namespace into the following bash code.
And then paste it into the terminal and you will have the pprof heap and goroutine metrics

```bash
#Grabbing metrics from pprof with:
export DP_POD_NAME=<insert-the-name-of-the-dp-pod-here>
export NAMESPACE=<insert-current-namespace>
kubectl exec -n=$NAMESPACE $DP_POD_NAME -- curl localhost:8111/debug/pprof/heap > ./heap.pprof
kubectl exec -n=$NAMESPACE $DP_POD_NAME -- curl localhost:8111/debug/pprof/goroutine > ./goroutine.pprof
kubectl exec -n=$NAMESPACE $DP_POD_NAME -- curl localhost:8111/debug/pprof/trace?seconds=30 > ./trace.out
kubectl exec -n=$NAMESPACE $DP_POD_NAME -- curl localhost:8111/debug/pprof/profile?seconds=30 > ./cpu.pprof

#Then running top and traces
#Also getting graph representation of memory usage with
go tool pprof -png heap.pprof > heap-diagram.png
go tool pprof -sample_index=alloc_space -png heap.pprof > heap-sample-diagram.png
go tool pprof -png cpu.pprof > cpu-usage-diagram.png
```

You can then do further analysis using the go tool with the following commands:

```bash
# When using go tool pprof the recommended commands are `top 10` and `traces` other commands via help.
go tool pprof heap.pprof
# Go routine usage.
go tool pprof goroutine.pprof
# View CPU usage over 30 seconds
go tool pprof cpu.pprof
# view trace in web browser
go tool trace trace.out
```

# Benchmarks

Benchmark results can be hard to read due to stdout printing random stuff between the name of the
benchmark and the actual results.

Running against an actual kafka instance is much better for comparing performance of changes.

Running various benchmarks:

```bash
# run restapi benchmarks against both mocked and docker-compose kafka
./benchmark-restapi.sh
# run restapi benchmarks against mocked kafka only
./benchmark-restapi.sh Mock
# run restapi benchmarks against docker-compose kafka only
./benchmark-restapi.sh Int

# All consumer/producer implementations
./benchmark-events.sh
# Memory provider for consumer and producer
./benchmark-events.sh Memory
# Sarama provider for consumer and producer
./benchmark-events.sh Sarama
```

# Performance Analysis

For analysing performance of specific benchmarks in Azul.

```bash
# run GetEvents benchmark with performance profiling
go test ./restapi -bench=GetEvents -run=xx  -benchmem -cpuprofile profile.out
# triage performance of api route on the restapi
go tool pprof -png -focus GetEventsActiveImplicit profile.out
# triage performance of pipelines on the restapi
go tool pprof -png -focus RunConsumeActions profile.out
```
