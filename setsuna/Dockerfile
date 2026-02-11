FROM golang:1.26-alpine as builder
WORKDIR /build
COPY go.mod go.sum main.go ./
RUN go mod tidy
RUN GOEXPERIMENT=jsonv2,greenteagc CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o setsuna .

FROM scratch
COPY --from=builder /build/setsuna setsuna
ENV CONFIG=/etc/config.yaml
ENTRYPOINT ["/setsuna"]

