FROM ghcr.io/blinklabs-io/go:1.26.1-1 AS build

ARG VERSION
ARG COMMIT_HASH
ENV VERSION=${VERSION}
ENV COMMIT_HASH=${COMMIT_HASH}

WORKDIR /code
RUN go env -w GOCACHE=/go-cache
RUN go env -w GOMODCACHE=/gomod-cache
COPY go.* .
RUN go mod download
COPY . .
RUN make build

FROM cgr.dev/chainguard/static AS dingo-operator
COPY --from=build /code/dingo-operator /bin/
# static base already runs as nonroot (uid 65532)
ENTRYPOINT ["/bin/dingo-operator"]
