# Outbox Pattern cho PHP

[English](README.md) | **Tiếng Việt**

[![tests](https://github.com/nguyenthuan2412/outbox-pattern/actions/workflows/tests.yml/badge.svg)](https://github.com/nguyenthuan2412/outbox-pattern/actions/workflows/tests.yml)
[![Packagist](https://img.shields.io/packagist/v/nguyenthuan2412/outbox-pattern.svg)](https://packagist.org/packages/nguyenthuan2412/outbox-pattern)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Lớp trung gian (abstraction) cho **Transactional Outbox pattern** trong PHP. Không phụ thuộc framework nào, hỗ trợ coroutine **Swoole / OpenSwoole**.

Thư viện **không kèm store, broker hay bridge cho framework nào**. Nó cung cấp các contract và phần điều phối dùng chung ở mọi project: ghi message, claim, publish, retry, dead letter và chạy worker. Phần database và broker do bạn tự triển khai.

```bash
composer require nguyenthuan2412/outbox-pattern
```

Yêu cầu PHP ≥ 8.1 và `psr/log`. `ext-swoole` hoặc `ext-openswoole` là tùy chọn.

## Thành phần

| Thành phần | Vai trò |
|---|---|
| `Contracts\OutboxStore` | **Bạn tự triển khai**: lưu và claim message (MySQL, PostgreSQL, MongoDB, ...) |
| `Contracts\Publisher` | **Bạn tự triển khai**: gửi ra Kafka, RabbitMQ, Redis, HTTP, ... |
| `Contracts\TransactionRunner` | **Bạn tự triển khai**: bọc transaction của framework hoặc ORM |
| `Contracts\RetryPolicy` | Có sẵn `ExponentialBackoff` và `NoRetry`, hoặc tự viết |
| `Contracts\PublishMiddleware` | Có sẵn `AddDeliveryHeaders`, `LoggingMiddleware`, `CallbackMiddleware` |
| `Contracts\Sleeper` | `BlockingSleeper` (CLI/FPM) và `Swoole\CoroutineSleeper` |
| `OutboxRecorder` | Phía ghi: `record()` và `transactional()` |
| `Relay\OutboxRelay` | Phía đọc: claim → publish → đánh dấu published / retry / dead |
| `Worker\Worker` | Vòng lặp polling, giãn thời gian chờ khi rảnh, giới hạn runtime/memory/số message, dừng êm khi nhận signal |
| `Swoole\SwooleWorker` | Chạy N relay song song bằng coroutine; `asProcess()` để gắn vào `Server` |
| `Store\InMemoryOutboxStore` | Bản triển khai tham chiếu, dùng để test và đọc như đặc tả |

## Luồng hoạt động

```
App ──(cùng 1 transaction)──► bảng business + bảng outbox
                                         │
                   OutboxRelay ◄── claim (lease)
                       │
             Pipeline(middlewares) ──► Publisher ──► broker
                       │
      markPublished | markFailed(retryAt) | markDead
```

Đảm bảo gửi **ít nhất một lần (at-least-once)**, nên message có thể bị gửi trùng. Consumer phải xử lý được message trùng bằng cách lọc theo message id. `AddDeliveryHeaders` gửi id này trong header `outbox-message-id`.

## Phía ghi

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

Nếu bạn tự quản lý transaction, chỉ cần gọi `$recorder->record(...)` bên trong transaction đó.

> **Store phải ghi trên cùng connection với dữ liệu business**, nếu không sẽ mất tính đảm bảo của outbox.

Message id là UUIDv7, sắp xếp được theo thời gian. `$message->partitionKey()` trả về `aggregateType:aggregateId`, dùng làm Kafka key hoặc routing key.

## Phía đọc (relay kiểu polling)

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
$worker->installSignalHandlers(); // SIGTERM/SIGINT: chạy nốt batch hiện tại rồi dừng
$worker->run();
```

Publisher có thể ném `Outbox\Exception\NonRetryableException` để đưa message thẳng vào dead letter. Message dead vẫn nằm trong store và có thể gửi lại bằng `$store->retryDead()`.

### Middleware

Middleware bọc quanh mỗi lần publish. Dùng cho logging, metrics, tracing, thêm header hoặc giới hạn tốc độ:

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

$worker->run();                            // chạy độc lập từ CLI
// hoặc
$server->addProcess($worker->asProcess()); // chạy cạnh Swoole\Http\Server
```

- `relayFactory` được gọi **một lần cho mỗi coroutine**. Mỗi coroutine cần connection riêng, hoặc dùng pool an toàn với coroutine. Không chia sẻ một PDO hay một AMQP channel giữa nhiều coroutine.
- Nếu `run()` được gọi khi đang ở trong coroutine (ví dụ trong server hoặc framework chạy trên Swoole), nó dùng scheduler hiện có. Nếu không, nó tự tạo scheduler mới.
- SIGTERM và SIGINT được xử lý để dừng êm. Truyền `handleSignals: false` để tắt.
- Khi rảnh, worker chờ bằng `Coroutine::sleep()`, nên không bao giờ chặn reactor.

Xem [examples/swoole-relay.php](examples/swoole-relay.php).

## Tự triển khai `OutboxStore`

Đọc `InMemoryOutboxStore` như một bản đặc tả chạy được. Store của bạn cần:

1. **`append`**: insert với status `pending` và `attempts = 0`, trên connection và transaction hiện tại.
2. **`claim($limit, $workerId, $leaseSeconds)`**: lấy các message `pending` có `available_at <= now` và không có lease còn hạn, theo thứ tự chèn. Đặt `locked_until = now + lease`, **tăng `attempts` lên 1**, rồi trả về các message đã cập nhật. Thao tác này phải **atomic**: hai worker không bao giờ được claim trùng một message.
   - MySQL 8 / PostgreSQL: `SELECT ... FOR UPDATE SKIP LOCKED` trong một transaction ngắn, sau đó `UPDATE`.
   - Cách dùng được với mọi DB: `UPDATE ... SET locked_by = :token ... WHERE id IN (:candidates) AND (locked_until IS NULL OR locked_until < :now)`, sau đó `SELECT ... WHERE locked_by = :token`.
3. **`markPublished`**: đặt status `published`, ghi `published_at` và xóa lease.
4. **`markFailed($id, $error, $retryAt)`**: giữ `pending`, đặt `available_at = $retryAt`, lưu lỗi và xóa lease.
5. **`markDead`**: đặt status `dead` và lưu lỗi. **`retryDead`**: đưa `dead` về `pending` với `attempts = 0`.
6. **`prune($olderThan)`**: xóa các message `published` cũ hơn mốc thời gian truyền vào.

Gợi ý cột: `seq` (auto-increment, dùng để sắp thứ tự), `id` (unique), `event_type`, `topic`, `aggregate_type`, `aggregate_id`, `payload`, `headers`, `status`, `attempts`, `last_error`, `created_at`, `available_at`, `locked_by`, `locked_until`, `published_at`. Index `(status, available_at)`.

Nếu cần **giữ thứ tự theo từng aggregate**, khi `claim` hãy bỏ qua message nào còn message `pending` sớm hơn có cùng `aggregate_type` và `aggregate_id`.

### Ví dụ: Laravel

Thư viện cố ý không có bridge cho framework nào. Tự nối vào Laravel chỉ mất vài dòng:

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

Store cho Laravel cần dùng **cùng tên connection**, qua `$db->connection($name)->table('outbox_messages')`. Hãy lấy connection mỗi lần gọi thay vì giữ sẵn một connection, để an toàn khi chạy Octane/Swoole.

## Dùng CDC (Debezium, Maxwell, ...) thay cho polling

Outbox pattern chỉ quy định phía ghi. Bạn có thể đưa message ra broker bằng cách đọc log của database thay vì polling. Khi đó bạn chỉ dùng `OutboxRecorder` và `OutboxStore::append()`, còn công cụ CDC thay thế cho `OutboxRelay` và các worker. Các trường của `OutboxMessage` khớp trực tiếp với các cột mà Outbox Event Router của Debezium yêu cầu (`id`, `aggregatetype`, `aggregateid`, `type`, `payload`).

| | Polling (relay của thư viện này) | Đọc log (CDC) |
|---|---|---|
| Độ trễ | Theo chu kỳ poll (khoảng 100 ms đến vài giây) | Gần như realtime |
| Tải lên DB | Query poll và update trạng thái | Chỉ đọc binlog/WAL |
| Hạ tầng | Không cần thêm gì | Debezium (Kafka Connect hoặc Debezium Server), bật binlog/WAL |
| Broker | Bất kỳ | Kafka nếu dùng Kafka Connect; Redis, RabbitMQ, NATS, ... nếu dùng Debezium Server |

## Chạy test

```bash
composer install
composer test   # test Swoole tự bỏ qua nếu chưa cài extension
```

## Giấy phép

MIT. Xem [LICENSE](LICENSE).
