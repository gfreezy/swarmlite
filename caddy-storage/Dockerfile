FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/caddy ./cmd/caddy
RUN /out/caddy list-modules | grep -qx 'caddy.storage.swarmlite'

FROM caddy:2.11.4-alpine
COPY --from=build /out/caddy /usr/bin/caddy
COPY bootstrap.json /etc/caddy/bootstrap.json
LABEL io.swarmlite.system="true" \
      io.swarmlite.component="gateway"
CMD ["run", "--resume", "--config", "/etc/caddy/bootstrap.json"]
