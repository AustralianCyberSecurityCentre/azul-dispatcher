# Start from a Debian image with the latest version of Go installed
# and a workspace (GOPATH) configured at /go.
ARG REGISTRY="docker.io/library"
ARG BASE_IMAGE=golang
ARG BASE_TAG=1.26-trixie@sha256:a085df697019cb63b40a70f6a92b948f7dc9df96dfcb2c20ba6eed25ce28f5b3

FROM $REGISTRY/$BASE_IMAGE:$BASE_TAG AS builder
ENV DEBIAN_FRONTEND=noninteractive
# important not to disable cgo here as kafka requires it
ENV GOOS=linux GOARCH=amd64 GO111MODULE=on GOPATH=/tmp/go
# flags necessary for gossdeep
ENV CGO_LDFLAGS_ALLOW="^-[Il].*$"

ARG XDG_CONFIG_HOME
# llvm installed as a lower RAM usage linker for cargo (rust) build of yara
COPY debian.txt /tmp/src/
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install llvm -y && \
    apt-get install libssl-dev -y && \
    apt-get install -y --no-install-recommends \
    $(grep -vE "^\s*(#|$)" /tmp/src/debian.txt | tr "\n" " ") && \
    rm -rf /tmp/src/debian.txt /var/lib/apt/lists/*
RUN git config --global url."git@github.com:AustralianCyberSecurityCentre/".insteadOf "https://github.com/AustralianCyberSecurityCentre/"

# Install yara-x for identify - needed for golang bedrock
# Install Rust and yara-x
ENV RUST_VERSION=1.95.0
RUN gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys 85AB96E6FA1BE5FE
# Download Rust tarball + signature
RUN curl -O https://static.rust-lang.org/dist/rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz \
    && curl -O https://static.rust-lang.org/dist/rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz.asc
# Verify signature
RUN gpg --verify rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz.asc

# perform rust install
RUN tar xzf rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz \
    && rust-${RUST_VERSION}-x86_64-unknown-linux-gnu/install.sh \
         --prefix=/usr/local \
         --without=rust-docs \
    && rm -rf rust-${RUST_VERSION}-*

# perform yara-x install
# Attempts to limit cargo RAM usage during builds.
# ENV CARGO_INCREMENTAL=0
# ENV RUSTFLAGS="-C debuginfo=0 -C codegen-units=1 -C link-arg=-fuse-ld=lld"
ENV RUSTFLAGS="-C link-arg=-fuse-ld=lld"

RUN cargo install cargo-c
RUN git clone -b v1.16.0 https://github.com/VirusTotal/yara-x.git && \
    cd yara-x && \
    cargo cinstall -p yara-x-capi --release --libdir /usr/local/lib/
RUN rm -rf yara-x

# Verify that yara-x install was successfull
RUN cat <<'EOF' > test.c
#include <yara_x.h>
int main() {
    YRX_RULES* rules;
    yrx_compile("rule dummy { condition: true }", &rules);
    yrx_rules_destroy(rules);
}
EOF
RUN gcc `pkg-config --cflags yara_x_capi` test.c `pkg-config --libs yara_x_capi`
RUN rm test.c
# End of yara-x/rust install

# default libmagic is updated slowly for debian distros and
# contains a number of bugs for office and archive file types
# Install updated libmagic
ARG FILE_GIT=https://github.com/file/file
ARG FILE_TAG=FILE5_47
RUN git clone --branch $FILE_TAG $FILE_GIT /go/file && \
    cd /go/file/ && \
    autoreconf -f -i && \
    ./configure --disable-silent-rules && \
    make -j4 && \
    make install && \
    ldconfig -v && file --version

COPY . /src

# if BEDROCK_REPLACE, bedrock is in a different place
# you must include a version such as thing@latest
ARG BEDROCK_REPLACE=""
RUN if [ "$BEDROCK_REPLACE" != "" ] ; then \
    cd /src && go mod edit -replace github.com/AustralianCyberSecurityCentre/azul-bedrock/v11=$BEDROCK_REPLACE && go mod tidy ;fi

# rakyll/magicmime requires static compilation ldflags (ie. -ldflags '-extldflags "-static"')
RUN --mount=type=secret,id=testSecret export $(cat /run/secrets/testSecret) && \
    cd /src && go test ./... -p 1
RUN cd /src && go build -v -a -tags static_all -o /go/bin/dispatcher main.go

##
# Main Image
##
FROM $REGISTRY/$BASE_IMAGE:$BASE_TAG
ENV DEBIAN_FRONTEND=noninteractive
# required for yara to find .so libraries
ENV LD_LIBRARY_PATH="/usr/local/lib:/usr/local/lib/x86_64-linux-gnu/"

COPY debian.txt /tmp/src/
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
    $(grep -vE "^\s*(#|$)" /tmp/src/debian.txt | tr "\n" " ") && \
    rm -rf /tmp/src/debian.txt /var/lib/apt/lists/*

# Copy the yara and file install from the build agent
COPY --from=builder /usr/local/lib/libyara_x_capi* /usr/local/lib/
COPY --from=builder /usr/local/lib/pkgconfig /usr/local/lib/pkgconfig

# Need to include the includes as well.
COPY --from=builder /usr/local/include/ /usr/local/include/

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

COPY --from=builder /go/bin/dispatcher /go/bin/
ARG UID=21000
ARG GID=21000
RUN groupadd -g $GID azul && useradd --create-home --shell /bin/bash -u $UID -g $GID azul
USER azul

# Verify that yara-x was copied over successfully and works in user context
RUN cat <<'EOF' > test.c
#include <yara_x.h>
int main() {
    YRX_RULES* rules;
    yrx_compile("rule dummy { condition: true }", &rules);
    yrx_rules_destroy(rules);
}
EOF
RUN gcc `pkg-config --cflags yara_x_capi` test.c `pkg-config --libs yara_x_capi`
RUN rm test.c
RUN file --version

EXPOSE 8111
ENTRYPOINT ["/go/bin/dispatcher"]
CMD ["serve"]
