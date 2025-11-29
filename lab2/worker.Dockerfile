# Build stage
FROM golang:1.25 AS builder
WORKDIR /src

COPY . .

ENV CGO_ENABLED=1
ENV GOOS=linux
ENV GOARCH=amd64

# Build a static linux binary from proxy/main.go
WORKDIR /src
RUN go clean

RUN go build -buildmode=plugin -o /bin/wc.so mrapps/wc.go
RUN go build -buildmode=plugin -o /bin/indexer.so mrapps/indexer.go
RUN go build -buildmode=plugin -o /bin/mtiming.so mrapps/mtiming.go
RUN go build -buildmode=plugin -o /bin/rtiming.so mrapps/rtiming.go
RUN go build -buildmode=plugin -o /bin/jobcount.so mrapps/jobcount.go
RUN go build -buildmode=plugin -o /bin/early_exit.so mrapps/early_exit.go
RUN go build -buildmode=plugin -o /bin/crash.so mrapps/crash.go
RUN go build -buildmode=plugin -o /bin/nocrash.so mrapps/nocrash.go
RUN go build -o /bin/worker main/mrworker.go

# Production stage: minimal image using scratch
FROM scratch
# Copy the compiled binaries into the final image
COPY --from=builder /bin/worker /worker
COPY --from=builder /bin/*.so /plugins/

# Expose the port your proxy listens on (adjust if different)
EXPOSE 1235

ENTRYPOINT ["/worker", "/plugins/wc.so"]
