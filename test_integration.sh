#/bin/bash
# local testing of integration
# first argument is prefix of tests to run, if empty runs all

BED__LOG_LEVEL=TRACE \
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
go test ./... -count=1 -tags=integration -run=$1 -p 1 -failfast

# The following runs integration tests on Azure Blob, ensure to set the <set-value> env's
#DISPATCHER_INIT_KAFKA_RETRIES=1 \
#DISPATCHER_LOG=TRACE \
#DISPATCHER_LOG_PRETTY=TRUE \
#DISPATCHER_CHECK_VALID_EVENTS=TRUE \
#DISPATCHER_KAFKA_ENDPOINT=localhost:9092 \
#DP__STREAMS__BACKEND=azure \
#DP__STREAMS__AZURE__ACCESS_KEY=<set-value> \
#DP__STREAMS__AZURE__CONTAINER=<set-value> \
#DP__STREAMS__AZURE__STORAGE_ACCOUNT=<set-value> \
#DP__STREAMS__AZURE__ENDPOINT=<set-value> \
#DISPATCHER_ALLOW_STREAM_DELETE=TRUE \
#go test ./streams/store/... -count=1 -tags=integration_azure -run=$1