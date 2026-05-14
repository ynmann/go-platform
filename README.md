## Features

- Typed context keys (`ctxkeys`)
- Generic sharded TTL cache (`ephemera`)
- In-process event bus (`eventbus`)
- Phased graceful shutdown (`stop`)
- Minimal Telegram Bot API client (`telegram`)
- Production-oriented concurrency and lifecycle primitives

## Goals

- Type safety
- Explicit ownership
- Graceful shutdown
- Observability-first design
- Low contention under load
- Minimal dependencies

## Install

```sh
go get github.com/ynmann/go-platform@latest
