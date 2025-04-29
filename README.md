<p align="right">
  📄 <a href="README.ru.md">🇷🇺 Русский</a>
</p>

# 🐰 BurrowMQ — A Minimal AMQP 0.9.1 Server in Rust

**BurrowMQ** is a lightweight asynchronous implementation of the AMQP 0.9.1 server in Rust.\
It's compatible with AMQP clients like [`lapin`](https://github.com/CleverCloud/lapin) and supports basic features like queues, exchanges, bindings, and message delivery.

> 🦀 BurrowMQ is a project **for learning, experimentation, and deep understanding of the AMQP protocol**. It's also a great playground for practicing async programming and architectural design in Rust.

> 🕳️ **Why "Burrow"?** Much like the rabbit in RabbitMQ, BurrowMQ is a minimalist AMQP implementation that works "deep inside" the protocol to provide a basic infrastructure for message delivery. It's a tribute to RabbitMQ's roots, reimagined in a simpler, educational form.
> 
> 🤓 Additionally, the name is a wordplay on "borrow" — as in Rust's borrow checker. While playful, BurrowMQ is also a great way to practice and understand Rust concepts like ownership, borrowing, and async interactions.

---

## ✨ Features

- ✅ Queue declaration (`queue.declare`)
- ✅ Exchange declaration (`exchange.declare`) — `direct`, `fanout`
- ✅ Queue bindings to exchanges (`queue.bind`)
- ✅ Message publishing (`basic.publish`) — includes default exchange (`exchange = ""`) and direct examples
- ✅ Consuming messages (`basic.consume`)
- ✅ Heartbeat support
- ✅ Fully async with `tokio`
- 🧪 Integration tests using `lapin`

---

## ❌ Not Yet Implemented

- `topic` and `headers` exchange types
- `exchange.bind`, `exchange.unbind`
- Publisher confirms — `confirm.select`, `basic.ack` with `delivery_tag` tracking
- Queue deletion (`queue.delete`)
- `basic.reject`, `basic.nack`
- QoS support (`basic.qos`)
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

## 🧪 Testing

The project includes integration tests using the `lapin` crate — a full-featured AMQP 0.9.1 client library for Rust.

## ⚖️ License

MIT

---

<p align="center">
  <img src="docs/burrowmq-logo.png" width="200" alt="BurrowMQ logo" />
</p>