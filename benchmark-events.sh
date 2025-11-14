#/bin/bash
# run benchmarks (For integration benchmarks make sure to run `docker compose up` first to bring up kafka)
# probably want to run `./benchmark-events.sh Sarama` to only target integration benchmarks
# first argument is prefix of bench to run, if empty runs all
# running benchmarks prints stuff to console which makes the results real hard to read

BED__LOG_LEVEL="WARN" \
BED__LOG_PRETTY=TRUE \
DP__EVENTS__KAFKA__ENDPOINT=localhost:9092 \
DP__STREAMS__BACKEND=s3 \
DP__STREAMS__S3__ENDPOINT=localhost:9000 \
DP__STREAMS__S3__ACCESS_KEY=minio-root-user \
DP__STREAMS__S3__SECRET_KEY=minio-root-password \
DP__STREAMS__S3__SECURE="false" \
DP__STREAMS__API_ALLOW_DELETE=TRUE \
DP__EVENTS__REDIS__ENDPOINT=localhost:6379 \
DP__EVENTS__REDIS__USERNAME=default \
DP__EVENTS__REDIS__PASSWORD=password \
go test ./events/benchmark -bench=${1:-.} -run=xx -tags=integration -p 1 -benchtime=10000x

# Note - for consistent results benchtime should be at least 10,000x
