<?php

declare(strict_types=1);

namespace Outbox\Tests\Unit;

use Outbox\Contracts\TransactionRunner;
use Outbox\OutboxMessage;
use Outbox\OutboxRecorder;
use Outbox\Relay\OutboxRelay;
use Outbox\RetryPolicy\ExponentialBackoff;
use Outbox\Store\InMemoryOutboxStore;
use Outbox\Tests\Support\RecordingPublisher;
use Outbox\Uuid;
use Outbox\Worker\Worker;
use Outbox\Worker\WorkerOptions;
use PHPUnit\Framework\TestCase;

final class MiscTest extends TestCase
{
    public function testUuidV7Format(): void
    {
        $id = Uuid::v7();
        self::assertMatchesRegularExpression('/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/', $id);
        self::assertNotSame($id, Uuid::v7());
    }

    public function testBackoffGrowsAndCaps(): void
    {
        $p = new ExponentialBackoff(baseDelaySeconds: 1, multiplier: 2, maxDelaySeconds: 5, jitter: 0);
        self::assertSame(1.0, $p->delayFor(1));
        self::assertSame(4.0, $p->delayFor(3));
        self::assertSame(5.0, $p->delayFor(10));
    }

    public function testRecorderUsesTransactionRunner(): void
    {
        $store = new InMemoryOutboxStore();
        $tx = new class implements TransactionRunner {
            public int $calls = 0;
            public function transactional(callable $callback): mixed { ++$this->calls; return $callback(); }
        };
        $recorder = new OutboxRecorder($store, $tx);

        $out = $recorder->transactional(function (OutboxRecorder $o) {
            $o->record(OutboxMessage::create('e', []));
            return 'ok';
        });

        self::assertSame('ok', $out);
        self::assertSame(1, $tx->calls);
        self::assertCount(1, $store->all());
    }

    public function testWorkerStopsWhenEmpty(): void
    {
        $store = new InMemoryOutboxStore();
        for ($i = 0; $i < 25; ++$i) {
            $store->append(OutboxMessage::create('e' . $i, []));
        }
        $publisher = new RecordingPublisher();
        $worker = new Worker(new OutboxRelay($store, $publisher, batchSize: 10), new WorkerOptions(stopWhenEmpty: true));

        self::assertSame(25, $worker->run());
        self::assertCount(25, $publisher->published);
    }

    public function testWorkerMaxMessages(): void
    {
        $store = new InMemoryOutboxStore();
        for ($i = 0; $i < 25; ++$i) {
            $store->append(OutboxMessage::create('e', []));
        }
        $worker = new Worker(new OutboxRelay($store, new RecordingPublisher(), batchSize: 10), new WorkerOptions(maxMessages: 10));

        self::assertSame(10, $worker->run());
    }

    public function testWorkerSurvivesStoreErrors(): void
    {
        $calls = 0;
        $store = new class($calls) extends InMemoryOutboxStore {
            public function __construct(private int &$calls) {}
            public function claim(int $limit, string $workerId, int $leaseSeconds): array
            {
                if (++$this->calls === 1) {
                    throw new \RuntimeException('db down');
                }
                return parent::claim($limit, $workerId, $leaseSeconds);
            }
        };
        $store->append(OutboxMessage::create('e', []));
        $worker = new Worker(new OutboxRelay($store, new RecordingPublisher()), new WorkerOptions(errorSleep: 0, stopWhenEmpty: true));

        self::assertSame(1, $worker->run());
        self::assertSame(3, $calls);
    }
}
