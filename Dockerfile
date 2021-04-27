ARG ALPINE_VERSION=notag
FROM alpine:$ALPINE_VERSION

RUN apk --no-cache add \
  ca-certificates \
  bash \
  curl \
  lsof \
  bind-tools \
  ospd-netstat \
  # go is required to be able run `go tool pprof`
  go \
  graphviz \
  tini

COPY ./entrypoint /usr/local/bin/
COPY ./latency-benchmark-service /usr/local/bin/

ARG GIT_SHA=nosha
ENV GIT_SHA=$GIT_SHA

WORKDIR /

EXPOSE 8099

# wrap with tini for signal processing and zombie killing
ENTRYPOINT ["/sbin/tini", "-g", "--", "entrypoint"]
