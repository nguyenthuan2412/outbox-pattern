# Outbox Pattern for PHP

[![tests](https://github.com/nguyenthuan2412/outbox-pattern/actions/workflows/tests.yml/badge.svg)](https://github.com/nguyenthuan2412/outbox-pattern/actions/workflows/tests.yml)
[![Packagist](https://img.shields.io/packagist/v/nguyenthuan2412/outbox-pattern.svg)](https://packagist.org/packages/nguyenthuan2412/outbox-pattern)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A framework-agnostic abstraction layer for the **Transactional Outbox pattern** in PHP, with first-class **Swoole / OpenSwoole** coroutine support.

This library ships **no storage, broker, or framework bridges**. It gives you the contracts plus the orchestration that is the same in every project: recording messages, claiming, publishing, retrying, dead-lettering, and running workers. You plug in your own database and broker.

```bash
composer require nguyenthuan2412/outbox-pattern
```

Requires PHP ≥ 8.1 and `psr/log`. `ext-swoole` or `ext-openswoole` is optional.

## What's inside

| Component | Role |
|---|---|
| `Contracts\OutboxStore` | **You implement**: persist and claim messages (MySQL, PostgreSQL, MongoDB, ...) |
| `Contracts\Publisher` | **You implement**: deliver to Kafka, RabbitMQ, Redis, HTTP, ... |
| `Contracts\TransactionRunner` | **You implement**: wrap your framework or ORM transaction |
| `Contracts\RetryPolicy` | Built-in `ExponentialBackoff` and `NoRetry`, or write your own |
| `Contracts\PublishMiddleware` | Built-in `AddDeliveryHeaders`, `LoggingMiddleware`, `CallbackMiddleware` |
| `Contracts\Sleeper` | `BlockingSleeper` (CLI/FPM) and `Swoole\CoroutineSleeper` |
| `OutboxRecorder` | Write side: `record()` and `transactional()` |
| `Relay\OutboxRelay` | Read side: claim → publish → mark published / retry / dead |
| `Worker\Worker` | Polling loop with idle backoff, runtime/memory/message limits, and graceful signals |
| `Swoole\SwooleWorker` | Runs N relays concurrently as coroutines; `asProcess()` attaches it to a `Server` |
| `Store\InMemoryOutboxStore` | Reference implementation, for tests and as an executable spec |

## How it works

```
App ──(same DB transaction)──► business tables + outbox table
                                              │
                      OutboxRelay ◄── claim (lease)
                          │
                Pipeline(middlewares) ──► Publisher ──► broker
                          │
        markPublished | markFailed(retryAt) | markDead
```

Delivery is **at-least-once**. Consumers must be idempotent: deduplicate on the message id, which `AddDeliveryHeaders` sends as the `outbox-message-id` header.

## Write side

```php
use Outbox\OutboxMessage;
use Outbox\OutboxRecorder;

$recorder = new OutboxRecorder($myStore, $myTransactionRunner);

$recorder->transactional(function (OutboxRecorder $outbox) use ($order) {
    $this->orders->save($order);

    $outbox->record(OutboxMessage::create(
        eventType: 'order.created',
        payload: ['id' => $order->id, 'total' => $order->total],
        aggregateType: 'order',
        aggregateId: $order->id,
        topic: 'orders',
    ));
});
```

If you already manage the transaction yourself, call `$recorder->record(...)` inside it.

> **Your store must write on the same connection as your business data**, otherwise the outbox guarantee is lost.

Message ids are time-ordered UUIDv7. `$message->partitionKey()` returns `aggregateType:aggregateId`, which works well as a Kafka key or routing key.

## Read side (polling relay)

```php
use Outbox\Middleware\AddDeliveryHeaders;
use Outbox\Middleware\LoggingMiddleware;
use Outbox\Middleware\Pipeline;
use Outbox\Relay\OutboxRelay;
use Outbox\RetryPolicy\ExponentialBackoff;
use Outbox\Worker\Worker;
use Outbox\Worker\WorkerOptions;

$relay = new OutboxRelay(
    store: $myStore,
    publisher: new Pipeline($myKafkaPublisher, new AddDeliveryHeaders(), new LoggingMiddleware($logger)),
    retryPolicy: new ExponentialBackoff(maxAttempts: 10, baseDelaySeconds: 1, maxDelaySeconds: 3600),
    batchSize: 100,
    leaseSeconds: 60,
);

$worker = new Worker($relay, new WorkerOptions(maxRuntime: 3600, memoryLimitMb: 256));
$worker->installSignalHandlers(); // SIGTERM/SIGINT: finish the current batch, then stop
$worker->run();
```

A publisher can throw `Outbox\Exception\NonRetryableException` to send a message straight to the dead letter state. Dead messages stay in your store and can be replayed with `$store->retryDead()`.

### Middleware

Middleware wraps every publish call. Use it for logging, metrics, tracing, header enrichment, or rate limiting:

```php
use Outbox\Middleware\CallbackMiddleware;

$publisher = new Pipeline(
    $myPublisher,
    new CallbackMiddleware(function (OutboxMessage $m, callable $next) use ($tracer) {
        $span = $tracer->start('outbox.publish');
        try {
            $next($m->withHeaders($m->headers + ['traceparent' => $span->id()]));
        } finally {
            $span->end();
        }
    }),
);
```

## Swoole / OpenSwoole

```php
use Outbox\Swoole\SwooleWorker;

Swoole\Runtime::enableCoroutine(SWOOLE_HOOK_ALL);

$worker = new SwooleWorker(
    relayFactory: fn (int $i) => new OutboxRelay(new MyStore($pool), $publisher),
    concurrency: 8,
);

$worker->run();                            // standalone CLI
// or
$server->addProcess($worker->asProcess()); // next to a Swoole\Http\Server
```

- The `relayFactory` is called **once per coroutine**. Give each coroutine its own connection, or use a coroutine-safe pool. Never share one PDO or one AMQP channel across coroutines.
- `run()` reuses the current scheduler when it is already called inside a coroutine (for example in a server or a Swoole-based framework). Otherwise it starts its own.
- SIGTERM and SIGINT are handled for a graceful drain. Pass `handleSignals: false` to disable this.
- Idle waits use `Coroutine::sleep()`, so the reactor is never blocked.

See [examples/swoole-relay.php](examples/swoole-relay.php).

## Implementing `OutboxStore`

Read `InMemoryOutboxStore` as the executable spec. Your store must:

1. **`append`**: insert with status `pending` and `attempts = 0`, on the current connection and transaction.
2. **`claim($limit, $workerId, $leaseSeconds)`**: select `pending` messages with `available_at <= now` and no active lease, in insertion order. Set `locked_until = now + lease`, **increment `attempts`**, and return the updated messages. This must be **atomic**: two workers must never claim the same message.
   - MySQL 8 / PostgreSQL: `SELECT ... FOR UPDATE SKIP LOCKED` in a short transaction, then `UPDATE`.
   - Portable alternative: `UPDATE ... SET locked_by = :token ... WHERE id IN (:candidates) AND (locked_until IS NULL OR locked_until < :now)`, then `SELECT ... WHERE locked_by = :token`.
3. **`markPublished`**: set status `published`, set `published_at`, and clear the lease.
4. **`markFailed($id, $error, $retryAt)`**: keep `pending`, set `available_at = $retryAt`, store the error, and clear the lease.
5. **`markDead`**: set status `dead` and store the error. **`retryDead`**: move `dead` back to `pending` with `attempts = 0`.
6. **`prune($olderThan)`**: delete `published` messages older than the given date.

Suggested columns: `seq` (auto-increment, for ordering), `id` (unique), `event_type`, `topic`, `aggregate_type`, `aggregate_id`, `payload`, `headers`, `status`, `attempts`, `last_error`, `created_at`, `available_at`, `locked_by`, `locked_until`, `published_at`. Index `(status, available_at)`.

For **per-aggregate ordering**, make `claim` skip any message that has an earlier `pending` message with the same `aggregate_type` and `aggregate_id`.

### Example: Laravel

The library has no framework bridges on purpose. Wiring one up takes a few lines:

```php
use Illuminate\Database\ConnectionResolverInterface;
use Outbox\Contracts\TransactionRunner;

final class LaravelTransactionRunner implements TransactionRunner
{
    public function __construct(
        private readonly ConnectionResolverInterface $db,
        private readonly ?string $connection = null,
        private readonly int $attempts = 1,
    ) {
    }

    public function transactional(callable $callback): mixed
    {
        return $this->db->connection($this->connection)->transaction(fn () => $callback(), $this->attempts);
    }
}
```

Your Laravel store should resolve the **same connection name** with `$db->connection($name)->table('outbox_messages')`. Resolve the connection on each call rather than holding one, which keeps it safe under Octane/Swoole.

## Using CDC (Debezium, Maxwell, ...) instead of polling

The outbox pattern only defines the write side. You can relay messages by tailing the database log instead of polling. In that setup you only use `OutboxRecorder` and `OutboxStore::append()`, and the CDC tool replaces `OutboxRelay` and the workers. `OutboxMessage` fields map directly to the columns expected by Debezium's Outbox Event Router (`id`, `aggregatetype`, `aggregateid`, `type`, `payload`).

| | Polling (this library's relay) | Log tailing (CDC) |
|---|---|---|
| Latency | Poll interval (~100 ms to a few seconds) | Near real-time |
| DB load | Poll queries and status updates | Reads the binlog/WAL only |
| Infrastructure | None extra | Debezium (Kafka Connect or Debezium Server), binlog/WAL enabled |
| Brokers | Any | Kafka with Kafka Connect; Redis, RabbitMQ, NATS, ... with Debezium Server |

## Testing

```bash
composer install
composer test   # Swoole tests are skipped when the extension is missing
```

## License

MIT. See [LICENSE](LICENSE).
