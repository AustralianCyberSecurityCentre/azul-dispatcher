# Start from a Debian image with the latest version of Go installed
# and a workspace (GOPATH) configured at /go.
ARG BUILD_REGISTRY="docker.io/library"
ARG BUILD_IMAGE=golang
ARG BUILD_TAG=1.26-trixie@sha256:0dcba0d95dbfb072e9917a106b9e07d7cc298097dc83e9307056ef1889de654d

ARG REGISTRY="dhi.io"
ARG BASE_IMAGE='python'
ARG BASE_TAG='3.12-debian-dev'
# Note if this is bumped for faster builds ensure the build agent has the same version of yara.
ARG YARA_X_VERSION_TAG="1.18.0"

FROM $BUILD_REGISTRY/$BUILD_IMAGE:$BUILD_TAG AS builder
ENV DEBIAN_FRONTEND=noninteractive
ENV PIP_DISABLE_PIP_VERSION_CHECK=yes
ARG PIP_CERT
ARG PIP_CLIENT_CERT
ARG PIP_TRUSTED_HOST
ARG PIP_INDEX_URL
ARG PIP_EXTRA_INDEX_URL
ARG GIT_BRANCH_NAME
# expected to be public registry (e.g pypi.org)
ARG UV_DEFAULT_INDEX
# expected to be private registry
ARG UV_INDEX_URL
ARG UV_INSECURE_HOST
# Ensure uv installs to the correct directory
ENV UV_PROJECT_ENVIRONMENT=/usr/local
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
ENV RUST_VERSION=1.96.0
# Attempts to limit cargo RAM usage during builds.
ENV RUSTFLAGS="-C link-arg=-fuse-ld=lld"
ARG YARA_X_VERSION_TAG
ENV YARA_X_VERSION_TAG=${YARA_X_VERSION_TAG}

COPY . /src

RUN if [ -f "/src/prebuilt/libyara_x_capi.so.$YARA_X_VERSION_TAG" ]; then \
        mkdir -p /usr/local/lib/pkgconfig/ && \
        cp -r /src/prebuilt/pkgconfig/* /usr/local/lib/pkgconfig/ && \
        cp -r /src/prebuilt/include/* /usr/local/include/ && \
        cp /src/prebuilt/libyara_x_capi.so.$YARA_X_VERSION_TAG /usr/local/lib/libyara_x_capi.so.$YARA_X_VERSION_TAG && \
        cd /usr/local/lib/ && ln -s ./libyara_x_capi.so.$YARA_X_VERSION_TAG libyara_x_capi.so && ln -s ./libyara_x_capi.so.$YARA_X_VERSION_TAG libyara_x_capi.so.1; \
    fi

# Only run if libyara isn't already present.
# if [[ ! -f "/usr/local/lib/libyara_x_capi.so.$YARA_X_VERSION_TAG" ]]; then

# Download Rust tarball + signature
RUN if [ ! -f "/usr/local/lib/libyara_x_capi.so.$YARA_X_VERSION_TAG" ]; then \
        gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys 85AB96E6FA1BE5FE && \
        curl -O https://static.rust-lang.org/dist/rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz && \
        curl -O https://static.rust-lang.org/dist/rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz.asc && \
        gpg --verify rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz.asc; \
    fi

# perform rust install
RUN if [ ! -f "/usr/local/lib/libyara_x_capi.so.$YARA_X_VERSION_TAG" ]; then \
        tar xzf rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz && \
        rust-${RUST_VERSION}-x86_64-unknown-linux-gnu/install.sh \
            --prefix=/usr/local \
            --without=rust-docs && \
        rm -rf rust-${RUST_VERSION}-*; \
    fi

# perform yara-x install
RUN if [ ! -f "/usr/local/lib/libyara_x_capi.so.$YARA_X_VERSION_TAG" ]; then \
     cargo install cargo-c; \
    fi
RUN if [ ! -f "/usr/local/lib/libyara_x_capi.so.$YARA_X_VERSION_TAG" ]; then \
        git clone -b v$YARA_X_VERSION_TAG https://github.com/VirusTotal/yara-x.git; \
        cd yara-x; \
        cargo cinstall -p yara-x-capi --release --libdir /usr/local/lib/; \
        rm -rf yara-x; \
    fi

# bypass externally managed restriction in distributed python
RUN rm /usr/lib/python3.13/EXTERNALLY-MANAGED

RUN pip install uv
COPY python-deps.txt ./python-deps.txt
RUN uv pip install --system -r python-deps.txt --extra-index-url $UV_INDEX_URL --exclude-newer "7 days" --exclude-newer-package=azul-security=false --exclude-newer-package=azul-bedrock=false
RUN rm python-deps.txt

# Upgrade to dev azul dependencies or upgrade non-dev azul dependencies depending on branch.
RUN if [ "$GIT_BRANCH_NAME" = "refs/heads/dev" ]; then \
    uv pip freeze | grep 'azul-.*==' | cut -d "=" -f 1 | xargs -I {} uv pip install --extra-index-url=$UV_INDEX_URL --system --upgrade --no-deps --prerelease allow '{}>=0.0.0-dev'; \
    else \
    uv pip freeze | grep 'azul-.*==' | cut -d "=" -f 1 | xargs -I {} uv pip install --extra-index-url=$UV_INDEX_URL --system --upgrade --no-deps '{}>=0.0.0'; \
    fi

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
ENV PIP_DISABLE_PIP_VERSION_CHECK=yes
ARG PIP_CERT
ARG PIP_CLIENT_CERT
ARG PIP_TRUSTED_HOST
ARG PIP_INDEX_URL
ARG PIP_EXTRA_INDEX_URL
ARG GIT_BRANCH_NAME
# expected to be public registry (e.g pypi.org)
ARG UV_DEFAULT_INDEX
# expected to be private registry
ARG UV_INDEX_URL
ARG UV_INSECURE_HOST
# Ensure uv installs to the correct directory
ENV UV_PROJECT_ENVIRONMENT=/usr/local
# required for yara to find .so libraries
ENV LD_LIBRARY_PATH="/usr/local/lib:/usr/local/lib/x86_64-linux-gnu/"

# COPY debian.txt /tmp/src/
# bypass externally managed restriction in distributed python
# RUN rm /usr/lib/python3.13/EXTERNALLY-MANAGED

# RUN pip install uv
# COPY python-deps.txt ./python-deps.txt
# RUN uv pip install --system -r python-deps.txt --extra-index-url $UV_INDEX_URL --exclude-newer "7 days" --exclude-newer-package=azul-security=false --exclude-newer-package=azul-bedrock=false
# RUN rm python-deps.txt

# # Upgrade to dev azul dependencies or upgrade non-dev azul dependencies depending on branch.
# RUN if [ "$GIT_BRANCH_NAME" = "refs/heads/dev" ]; then \
#     uv pip freeze | grep 'azul-.*==' | cut -d "=" -f 1 | xargs -I {} uv pip install --extra-index-url=$UV_INDEX_URL --system --upgrade --no-deps --prerelease allow '{}>=0.0.0-dev'; \
#     else \
#     uv pip freeze | grep 'azul-.*==' | cut -d "=" -f 1 | xargs -I {} uv pip install --extra-index-url=$UV_INDEX_URL --system --upgrade --no-deps '{}>=0.0.0'; \
#     fi

ARG YARA_X_VERSION_TAG
ENV YARA_X_VERSION_TAG=${YARA_X_VERSION_TAG}
# Copy the yara and file install from the build agent
COPY --from=builder /usr/local/lib/libyara_x_capi.so.$YARA_X_VERSION_TAG /usr/local/lib/
# Create the symlinks to libyara_x_capi.1.16.0 (or whatever version it's up to until version 2 and then this will need an update)
RUN cd /usr/local/lib/ && ln -s ./libyara_x_capi.so.$YARA_X_VERSION_TAG libyara_x_capi.so && ln -s ./libyara_x_capi.so.$YARA_X_VERSION_TAG libyara_x_capi.so.1
COPY --from=builder /usr/local/lib/pkgconfig /usr/local/lib/pkgconfig
COPY --from=builder /usr/local/lib/libmagic.so* /usr/local/lib/
COPY --from=builder /usr/local/lib/libmagic.la /usr/local/lib/libmagic.la

# Need to include the includes as well.
COPY --from=builder /usr/local/include/ /usr/local/include/

# # default libmagic for debian can get out of date 
# contains a number of bugs for office and archive file types
# Install updated libmagic
COPY --from=builder /usr/local/bin/file /usr/local/bin/file

COPY --from=builder /go/bin/dispatcher /go/bin/
# ARG UID=21000
# ARG GID=21000
# RUN groupadd -g $GID azul && useradd --create-home --shell /bin/bash -u $UID -g $GID azul
# USER azul

# Verify that yara-x was copied over successfully and works in user context
# RUN cat <<'EOF' > test.c
# #include <yara_x.h>
# int main() {
#     YRX_RULES* rules;
#     yrx_compile("rule dummy { condition: true }", &rules);
#     yrx_rules_destroy(rules);
# }
# EOF
# RUN gcc `pkg-config --cflags yara_x_capi` test.c `pkg-config --libs yara_x_capi`
# RUN rm test.c
RUN file --version

EXPOSE 8111
ENTRYPOINT ["/go/bin/dispatcher"]
CMD ["serve"]
