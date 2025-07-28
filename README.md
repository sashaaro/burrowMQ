<p align="right">
  📄 <a href="README.ru.md">🇷🇺 Русский</a>
</p>

# 🐰 BurrowMQ — A Minimal AMQP 0.9.1 Server in Rust

**BurrowMQ** is a lightweight asynchronous implementation of the AMQP 0.9.1 server in Rust.\
Supports basic features like queues, exchanges, bindings, and message delivery.

> 🦀 BurrowMQ is a project **for learning, experimentation, and deep understanding of the AMQP protocol**. It's also a great playground for practicing async programming and architectural design in Rust.

> **Why "Burrow"?** Much like the rabbit in RabbitMQ, BurrowMQ is a minimalist AMQP implementation that works "deep inside" the protocol to provide a basic infrastructure for message delivery. It's a tribute to RabbitMQ's roots, reimagined in a simpler, educational form.
> 
> Additionally, the name is a wordplay on "borrow" — as in Rust's borrow checker. While playful, BurrowMQ is also a great way to practice and understand Rust concepts like ownership, borrowing, and async interactions.

---

## Features

-  AMQP 0.9.1 methods:
   `queue.declare`, `exchange.declare` (`direct`, `fanout`), `queue.bind`, `queue.purge`,
   `basic.publish` (incl. default exchange), `basic.consume`, `basic.ack`
-  Heartbeat support
-  Fully async with `tokio`
- Integration tests with a built-in DSL (implemented using [`nom`](https://github.com/rust-bakery/nom)) for declaratively describing messaging scenarios (publish, consume, ack, etc.), and integration with [`lapin`](https://github.com/amqp-rs/lapin) (a full-featured AMQP 0.9.1 client for Rust)

---

## Not Yet Implemented

- `topic` and `headers` exchange types
- `exchange.bind`, `exchange.unbind`
- Publisher confirms — `confirm.select`
- Queue deletion (`queue.delete`)
- `basic.reject`, `basic.nack`
- QoS support (`basic.qos`) with prefetch_count != 1
- Authentication (`connection.start-ok` with login/password)
- Message persistence to disk
- Clustering and federation
- Web UI or monitoring interface

---

## 🚀 Quick Start

### Run the server

```bash
cargo run -- --port 5672
```

The server will listen on `127.0.0.1:5672`.

---

### Running Tests

```bash
# Tests automatically start the built-in BurrowMQ server on port 5672
cargo test
```

By default, integration tests automatically launch the embedded AMQP server (BurrowMQServer) on port 5672, and then connect to it using lapin. All tests are designed to be compatible with standard AMQP 0.9.1 implementations and should work identically with RabbitMQ.

If you want to test against a real RabbitMQ instance or BurrowMQ, you can disable the embedded server and use an external one instead:

```bash

# Example: Running RabbitMQ via Docker
docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
# Or start BurrowMQ manually
cargo run -- --port 5672

# Then run tests with the embedded server disabled
NO_EMBEDDED_AMQP=1 cargo test
```


## License

MIT

<p align="center">
  <img src="docs/burrowmq-logo.png" width="200" alt="BurrowMQ logo" />
</p>
