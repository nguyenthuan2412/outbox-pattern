<?php

declare(strict_types=1);

namespace Outbox\Tests\Swoole;

use Outbox\OutboxMessage;
use Outbox\Relay\OutboxRelay;
use Outbox\Store\InMemoryOutboxStore;
use Outbox\Swoole\Runtime;
use Outbox\Swoole\SwooleWorker;
use Outbox\Tests\Support\RecordingPublisher;
use Outbox\Worker\WorkerOptions;
use PHPUnit\Framework\TestCase;

final class SwooleWorkerTest extends TestCase
{
    protected function setUp(): void
    {
        if (!Runtime::available()) {
            self::markTestSkipped('ext-swoole / ext-openswoole not installed');
        }
    }

    public function testCoroutinesShareWorkWithoutDuplicates(): void
    {
        $store = new InMemoryOutboxStore();
        for ($i = 0; $i < 40; ++$i) {
            $store->append(OutboxMessage::create('e', ['i' => $i]));
        }
        // each publish "does I/O" for 50ms -> sequential would take >= 2s
        $publisher = new RecordingPublisher(fn () => Runtime::sleep(0.05));

        $worker = new SwooleWorker(
            fn (int $i) => new OutboxRelay($store, $publisher, batchSize: 1, workerId: 'co-' . $i),
            concurrency: 8,
            options: new WorkerOptions(stopWhenEmpty: true),
            handleSignals: false,
        );

        $start = microtime(true);
        $processed = $worker->run();
        $elapsed = microtime(true) - $start;

        self::assertSame(40, $processed);
        $ids = array_map(fn ($m) => $m->payload['i'], $publisher->published);
        sort($ids);
        self::assertSame(range(0, 39), $ids, 'every message published exactly once');
        self::assertLessThan(1.0, $elapsed, 'publishing ran concurrently');
    }

    public function testStopFromAnotherCoroutine(): void
    {
        $store = new InMemoryOutboxStore();
        $worker = new SwooleWorker(
            fn () => new OutboxRelay($store, new RecordingPublisher()),
            concurrency: 3,
            options: new WorkerOptions(idleSleep: 0.01, maxIdleSleep: 0.05),
            handleSignals: false,
        );

        $start = microtime(true);
        Runtime::run(function () use ($worker) {
            Runtime::create(function () use ($worker) {
                Runtime::sleep(0.2);
                $worker->stop();
            });
            $worker->run();
        });

        self::assertLessThan(1.0, microtime(true) - $start);
    }
}
