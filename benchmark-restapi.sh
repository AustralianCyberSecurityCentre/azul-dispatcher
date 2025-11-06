#/bin/bash
# run benchmarks (For integration benchmarks make sure to run `docker compose up` first to bring up kafka)
# probably want to run `./benchmark-restapi.sh Int` to only target integration benchmarks
# first argument is prefix of bench to run, if empty runs all
# running benchmarks prints stuff to console which makes the results real hard to read

DP__LOG_LEVEL="WARN" \
DP__LOG_PRETTY=TRUE \
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
DP__EVENTS__KAFKA__TOPIC_PREFIX=test02 \
go test ./restapi -bench=${1:-.} -run=xx -tags=integration -p 1 
