# Step 1: Modules caching
FROM golang:1.24-alpine AS modules
COPY go.mod go.sum /modules/
WORKDIR /modules
RUN go mod download

# Step 2: Builder
FROM golang:1.24-alpine AS builder
COPY --from=modules /go/pkg /go/pkg
COPY . /app
WORKDIR /app
RUN CGO_ENABLED=0 go build -o /bin/app ./cmd

# Step 3: Final
FROM alpine
USER root
RUN apk --no-cache add ca-certificates && update-ca-certificates
COPY --from=builder /bin/app /app
CMD ["/app"]
