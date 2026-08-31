# Start from a Debian image with the latest version of Go installed
# and a workspace (GOPATH) configured at /go.
ARG REGISTRY="dhi.io"
ARG BUILD_IMAGE='golang'
ARG BUILD_TAG='1.26-debian13-dev'
ARG BASE_IMAGE=static
ARG BASE_TAG=20250419

ARG PYTHON_BUILD_IMAGE='python'
ARG PYTHON_BUILD_TAG='3.12-debian-dev'

# Note if this is bumped for faster builds ensure the build agent has the same version of yara.
ARG YARA_X_VERSION_TAG="1.20.0"

FROM $REGISTRY/$PYTHON_BUILD_IMAGE:$PYTHON_BUILD_TAG AS pybuilder
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

# copy all files not in .dockerignore
COPY ./python-deps.txt /tmp/src/python-deps.txt
RUN pip install uv

# build and install package
WORKDIR /tmp/src
# Install dependencies required by pyinstaller.
RUN apt-get update && \
    apt install binutils -y && \
    rm -rf /tmp/src/debian.txt /var/lib/apt/lists/*
# Install azul-security and it's dependencies + pyinstaller
RUN uv pip install --system -r python-deps.txt --extra-index-url $UV_INDEX_URL --exclude-newer "7 days" --exclude-newer-package=azul-security=false --exclude-newer-package=azul-bedrock=false
# Check for dev version of azul-security
RUN if [ "$GIT_BRANCH_NAME" = "refs/heads/dev" ]; then \
    uv pip freeze | grep 'azul-.*==' | cut -d "=" -f 1 | xargs -I {} uv pip install --extra-index-url=$UV_INDEX_URL --system --upgrade --no-deps --prerelease allow '{}>=0.0.0-dev'; \
    else \
    uv pip freeze | grep 'azul-.*==' | cut -d "=" -f 1 | xargs -I {} uv pip install --extra-index-url=$UV_INDEX_URL --system --upgrade --no-deps '{}>=0.0.0'; \
    fi
# Create the azul-security executable in a dist directory.
RUN pyinstaller --onedir $( find /usr/ -type f -path "*/azul_security/cli_commands.py") --exclude-module uvloop  --name azul-security
# Delete un-needed babel files
RUN find dist/azul-security/_internal/babel/locale-data -type f ! -name 'root.dat' ! -name 'en.dat' ! -name 'en_US.dat' -delete

FROM $REGISTRY/$BUILD_IMAGE:$BUILD_TAG AS builder
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
ENV RUST_VERSION=1.98.0
# Attempts to limit cargo RAM usage during builds.
ENV RUSTFLAGS="-C link-arg=-fuse-ld=lld"
ARG YARA_X_VERSION_TAG
ENV YARA_X_VERSION_TAG=${YARA_X_VERSION_TAG}

COPY . /src

RUN if [ -f "/src/prebuilt/libyara_x_capi.so.$YARA_X_VERSION_TAG" ]; then \
    mkdir -p /usr/lib/pkgconfig/ && \
    cp -r /src/prebuilt/pkgconfig/* /usr/lib/pkgconfig/ && \
    cp -r /src/prebuilt/include/* /usr/include/ && \
    cp /src/prebuilt/libyara_x_capi.so.$YARA_X_VERSION_TAG /usr/lib/libyara_x_capi.so.$YARA_X_VERSION_TAG && \
    cd /usr/lib/ && ln -s ./libyara_x_capi.so.$YARA_X_VERSION_TAG libyara_x_capi.so && ln -s ./libyara_x_capi.so.$YARA_X_VERSION_TAG libyara_x_capi.so.1; \
    fi

# Only run if libyara isn't already present.
# if [[ ! -f "/usr/local/lib/libyara_x_capi.so.$YARA_X_VERSION_TAG" ]]; then

# Download Rust tarball + signature
RUN if [ ! -f "/usr/lib/libyara_x_capi.so.$YARA_X_VERSION_TAG" ]; then \
    gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys 85AB96E6FA1BE5FE && \
    curl -O https://static.rust-lang.org/dist/rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz && \
    curl -O https://static.rust-lang.org/dist/rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz.asc && \
    gpg --verify rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz.asc; \
    fi

# perform rust install
RUN if [ ! -f "/usr/lib/libyara_x_capi.so.$YARA_X_VERSION_TAG" ]; then \
    tar xzf rust-${RUST_VERSION}-x86_64-unknown-linux-gnu.tar.gz && \
    rust-${RUST_VERSION}-x86_64-unknown-linux-gnu/install.sh \
    --prefix=/usr \
    --without=rust-docs && \
    rm -rf rust-${RUST_VERSION}-*; \
    fi

# perform yara-x install
RUN if [ ! -f "/usr/lib/libyara_x_capi.so.$YARA_X_VERSION_TAG" ]; then \
    cargo install cargo-c; \
    fi
RUN if [ ! -f "/usr/lib/libyara_x_capi.so.$YARA_X_VERSION_TAG" ]; then \
    git clone -b v$YARA_X_VERSION_TAG https://github.com/VirusTotal/yara-x.git; \
    cd yara-x; \
    cargo cinstall -p yara-x-capi --release --libdir /usr/lib/; \
    rm -rf yara-x; \
    fi

# Install azul-security binary.
COPY --from=pybuilder /tmp/src/dist/azul-security/azul-security /usr/bin/azul-security
COPY --from=pybuilder /tmp/src/dist/azul-security/_internal /usr/bin/_internal

# default libmagic is updated slowly for debian distros and
# contains a number of bugs for office and archive file types
# Install updated libmagic
ARG FILE_GIT=https://github.com/file/file
ARG FILE_TAG=FILE5_47
RUN git clone --branch $FILE_TAG $FILE_GIT /go/file && \
    cd /go/file/ && \
    autoreconf -f -i && \
    ./configure --disable-silent-rules --prefix=/usr && \
    make -j4 && \
    make install && \
    ldconfig -v && file --version

# if BEDROCK_REPLACE, bedrock is in a different place
# you must include a version such as thing@latest
ARG BEDROCK_REPLACE=""
RUN if [ "$BEDROCK_REPLACE" != "" ] ; then \
    cd /src && go mod edit -replace github.com/AustralianCyberSecurityCentre/azul-bedrock/v12=$BEDROCK_REPLACE && go mod tidy ;fi

# rakyll/magicmime requires static compilation ldflags (ie. -ldflags '-extldflags "-static"')
RUN --mount=type=secret,id=testSecret export $(cat /run/secrets/testSecret) && \
    cd /src && go test ./... -p 1
RUN cd /src && go build -v -a -tags static_all -o /go/bin/dispatcher main.go

# This can be used to check what linker/loader is being used by dispatcher.
# It needs to be in the final image and if it isn't the image will fail with a " no such file or directory" error
# when attempting to startup dispatcher even though the file is present.
# RUN apt install binutils -y
# RUN readelf -l /go/bin/dispatcher | grep interpreter

##
# Main Image
##
FROM $REGISTRY/$BASE_IMAGE:$BASE_TAG
# Create directory /tmp/fcache
WORKDIR /tmp/fcache
WORKDIR /
# required for yara to find .so libraries
ENV LD_LIBRARY_PATH="/usr/lib:/usr/lib/x86_64-linux-gnu/:/usr/local/lib/x86_64-linux-gnu/:/usr/local/lib/"
ARG YARA_X_VERSION_TAG
ENV YARA_X_VERSION_TAG=${YARA_X_VERSION_TAG}
# Copy the yara and file install from the build agent
COPY --from=builder /usr/lib/libyara_x_capi.so* /usr/lib/
# Get pkgconfig from builder for libyara as well.
COPY --from=builder /usr/lib/pkgconfig /usr/lib/pkgconfig

# Need to include the includes as well.
COPY --from=builder /usr/include/ /usr/include/

# Install azul-security binary.
COPY --from=pybuilder /tmp/src/dist/azul-security/azul-security /usr/bin/azul-security
COPY --from=pybuilder /tmp/src/dist/azul-security/_internal /usr/bin/_internal

# Copy all of libmagic libraries in (file).
COPY --from=builder /usr/bin/file /usr/bin/file
COPY --from=builder /usr/lib/libmagic.la /usr/lib/libmagic.la
COPY --from=builder /usr/lib/libmagic.so* /usr/lib/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libfuzzy* /usr/lib/x86_64-linux-gnu/libfuzzy*
# Need all of user share for file to work
COPY --from=builder /usr/share/misc/magic.mgc /usr/share/misc/magic.mgc

# Copy linker/loader from builder (required for)
COPY --from=builder /lib64/ld-linux-x86-64.so.2 /lib64/ld-linux-x86-64.so.2

# Copy dispatcher binary.
COPY --from=builder /go/bin/dispatcher /bin/dispatcher
# ARG UID=65532
# ARG GID=65532

EXPOSE 8111
ENTRYPOINT ["/bin/dispatcher"]
CMD ["serve"]
