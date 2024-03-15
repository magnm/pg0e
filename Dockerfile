FROM golang:1.22 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags "-s -w" -o server

FROM gcr.io/distroless/base-debian12

COPY --from=builder /src/server /
ENTRYPOINT [ "/server" ]