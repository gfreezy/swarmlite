package main

import (
	_ "github.com/caddyserver/cache-handler"
	caddycmd "github.com/caddyserver/caddy/v2/cmd"
	_ "github.com/caddyserver/caddy/v2/modules/standard"
	_ "github.com/darkweak/storages/badger/caddy"
	_ "github.com/swarmlite/swarmlite/caddy-storage"
)

func main() {
	caddycmd.Main()
}
