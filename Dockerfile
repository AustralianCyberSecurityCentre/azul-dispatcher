# Start from a Debian image with the latest version of Go installed
# and a workspace (GOPATH) configured at /go.
ARG REGISTRY="docker.io/library"
ARG BASE_IMAGE=golang
ARG BASE_TAG=1.25-trixie

FROM $REGISTRY/$BASE_IMAGE:$BASE_TAG AS builder
ENV DEBIAN_FRONTEND=noninteractive
# important not to disable cgo here as kafka requires it
ENV GOOS=linux GOARCH=amd64 GO111MODULE=on GOPATH=/tmp/go
# flags necessary for gossdeep
ENV CGO_LDFLAGS_ALLOW="^-[Il].*$"
ENV GOPRIVATE=github.com/AustralianCyberSecurityCentre/*

ARG XDG_CONFIG_HOME

COPY debian.txt /tmp/src/
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
    $(grep -vE "^\s*(#|$)" /tmp/src/debian.txt | tr "\n" " ") && \
    rm -rf /tmp/src/debian.txt /var/lib/apt/lists/*
RUN git config --global url."git@github.com:AustralianCyberSecurityCentre/".insteadOf "https://github.com/AustralianCyberSecurityCentre/"

# Install yara, needed for identify.
ARG YARA_GIT=https://github.com/VirusTotal/yara
ARG YARA_TAG=v4.3.2
RUN mkdir -p /go/yara && \
    git clone --branch $YARA_TAG $YARA_GIT /go/yara && \
    cd /go/yara && \
    ./bootstrap.sh && \
    ./configure && \
    rm /go/yara/tests -rf && \
    make && \
    make install

# default libmagic is updated slowly for debian distros and
# contains a number of bugs for office and archive file types
# Install updated libmagic
ARG FILE_GIT=https://github.com/file/file
ARG FILE_TAG=FILE5_46
RUN git clone --branch $FILE_TAG $FILE_GIT /go/file && \
    cd /go/file/ && \
    autoreconf -f -i && \
    ./configure --disable-silent-rules && \
    make -j4 && \
    make install && \
    ldconfig -v && file --version

# note - this also copies in .ssh folder, so be careful
COPY . /src

# if BEDROCK_REPLACE, bedrock is in a different place
# you must include a version such as thing@latest
ARG BEDROCK_REPLACE=""
RUN if [ "$BEDROCK_REPLACE" != "" ] ; then \
    cd /src && go mod edit -replace github.com/AustralianCyberSecurityCentre/azul-bedrock/v10=$BEDROCK_REPLACE && go mod tidy ;fi

# rakyll/magicmime requires static compilation ldflags (ie. -ldflags '-extldflags "-static"')
RUN --mount=type=ssh,id=id --mount=type=secret,id=testSecret export $(cat /run/secrets/testSecret) && \
    cd /src && go test ./...
RUN --mount=type=ssh,id=id cd /src && \
    go build -v -a -tags static_all -o /go/bin/dispatcher main.go

##
# Main Image
##
FROM $REGISTRY/$BASE_IMAGE:$BASE_TAG
ENV DEBIAN_FRONTEND=noninteractive
# required for yara to find .so libraries
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/local/lib"

COPY debian.txt /tmp/src/
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install flex bison -y && \
    apt-get install -y --no-install-recommends \
    $(grep -vE "^\s*(#|$)" /tmp/src/debian.txt | tr "\n" " ") && \
    rm -rf /tmp/src/debian.txt /var/lib/apt/lists/*


# # default libmagic for debian can get out of date 
# contains a number of bugs for office and archive file types
# Install updated libmagic
COPY --from=builder /go/file /go/file
RUN cd /go/file && \
    make install && \
    ldconfig -v && \
    cd /go && \
    rm /go/file -rf && \
    file --version

# Install Yara
COPY --from=builder /go/yara /go/yara
RUN cd /go/yara && \
    make install && \
    cd /go && \
    rm /go/yara -rf && \
    yara --version

COPY --from=builder /go/bin/dispatcher /go/bin/
ARG UID=21000
ARG GID=21000
RUN groupadd -g $GID azul && useradd --create-home --shell /bin/bash -u $UID -g $GID azul
USER azul
EXPOSE 8111
ENTRYPOINT ["/go/bin/dispatcher"]
CMD ["serve"]
