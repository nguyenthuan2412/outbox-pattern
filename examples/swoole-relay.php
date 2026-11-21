<?php

/**
 * Example: run the relay with 8 coroutines next to a Swoole HTTP server.
 * MyPdoStore / MyKafkaPublisher are YOUR implementations of the contracts.
 */

declare(strict_types=1);

use Outbox\Middleware\AddDeliveryHeaders;
use Outbox\Middleware\Pipeline;
use Outbox\Relay\OutboxRelay;
use Outbox\RetryPolicy\ExponentialBackoff;
use Outbox\Swoole\SwooleWorker;
use Outbox\Worker\WorkerOptions;

require __DIR__ . '/../vendor/autoload.php';

Swoole\Runtime::enableCoroutine(SWOOLE_HOOK_ALL);

$relay = new SwooleWorker(
    relayFactory: function (int $i) {
        // one connection per coroutine (or use a coroutine-safe pool)
        $pdo = new PDO('mysql:host=127.0.0.1;dbname=app', 'root', 'secret');

        return new OutboxRelay(
            store: new MyPdoStore($pdo),
            publisher: new Pipeline(new MyKafkaPublisher(), new AddDeliveryHeaders()),
            retryPolicy: new ExponentialBackoff(maxAttempts: 8),
            batchSize: 50,
            leaseSeconds: 30,
            workerId: gethostname() . ':' . getmypid() . ':' . $i,
        );
    },
    concurrency: 8,
    options: new WorkerOptions(maxRuntime: 3600),
);

$server = new Swoole\Http\Server('0.0.0.0', 9501);
$server->addProcess($relay->asProcess());
$server->on('request', function ($req, $res) {
    $res->end('ok');
});
$server->start();

// Standalone (no server):  $relay->run();
