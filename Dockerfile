FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /dispatch ./cmd/api
RUN CGO_ENABLED=0 go build -trimpath -o /receiver ./cmd/receiver

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /dispatch /dispatch
COPY --from=build /receiver /receiver
EXPOSE 8080
ENTRYPOINT ["/dispatch"]
