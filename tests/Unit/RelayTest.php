<?php

declare(strict_types=1);

namespace Outbox\Tests\Unit;

use Outbox\Exception\NonRetryableException;
use Outbox\OutboxMessage;
use Outbox\Relay\OutboxRelay;
use Outbox\Relay\Outcome;
use Outbox\RetryPolicy\ExponentialBackoff;
use Outbox\Store\InMemoryOutboxStore;
use Outbox\Tests\Support\RecordingPublisher;
use PHPUnit\Framework\TestCase;

final class RelayTest extends TestCase
{
    public function testPublishesAndMarksPublished(): void
    {
        $store = new InMemoryOutboxStore();
        $store->append(OutboxMessage::create('a', ['x' => 1]), OutboxMessage::create('b', []));
        $publisher = new RecordingPublisher();

        $result = (new OutboxRelay($store, $publisher))->runOnce();

        self::assertSame(2, $result->claimed);
        self::assertSame(2, $result->published);
        self::assertSame(['a', 'b'], array_map(fn ($m) => $m->eventType, $publisher->published));
        self::assertSame(1, $publisher->published[0]->attempts);
        foreach ($store->all() as $m) {
            self::assertSame(OutboxMessage::STATUS_PUBLISHED, $m->status);
        }
        self::assertTrue((new OutboxRelay($store, $publisher))->runOnce()->isEmpty());
    }

    public function testFailureSchedulesRetryInFuture(): void
    {
        $store = new InMemoryOutboxStore();
        $store->append(OutboxMessage::create('a', []));
        $publisher = new RecordingPublisher(fn () => throw new \RuntimeException('broker down'));
        $relay = new OutboxRelay($store, $publisher, new ExponentialBackoff(maxAttempts: 3, baseDelaySeconds: 60, jitter: 0));

        $result = $relay->runOnce();

        self::assertSame(1, $result->retried);
        $m = $store->all()[0];
        self::assertSame(OutboxMessage::STATUS_PENDING, $m->status);
        self::assertStringContainsString('broker down', (string) $m->lastError);
        self::assertGreaterThan(new \DateTimeImmutable('+50 seconds'), $m->availableAt);
        self::assertTrue($relay->runOnce()->isEmpty(), 'not due yet');
    }

    public function testGoesDeadAfterMaxAttemptsAndCanBeReplayed(): void
    {
        $store = new InMemoryOutboxStore();
        $store->append(OutboxMessage::create('a', []));
        $fail = true;
        $publisher = new RecordingPublisher(function () use (&$fail) {
            if ($fail) {
                throw new \RuntimeException('nope');
            }
        });
        $relay = new OutboxRelay($store, $publisher, new ExponentialBackoff(maxAttempts: 2, baseDelaySeconds: 0, jitter: 0));

        self::assertSame(1, $relay->runOnce()->retried);
        self::assertSame(1, $relay->runOnce()->dead);
        self::assertSame(OutboxMessage::STATUS_DEAD, $store->all()[0]->status);
        self::assertTrue($relay->runOnce()->isEmpty());

        $fail = false;
        self::assertSame(1, $store->retryDead());
        self::assertSame(1, $relay->runOnce()->published);
    }

    public function testNonRetryableGoesStraightToDead(): void
    {
        $store = new InMemoryOutboxStore();
        $store->append(OutboxMessage::create('a', []));
        $relay = new OutboxRelay($store, new RecordingPublisher(fn () => throw new NonRetryableException('bad schema')));

        self::assertSame(Outcome::Dead, $relay->handle($relay->claim()[0]));
    }

    public function testStoreErrorsAreNotSwallowed(): void
    {
        $store = new class extends \Outbox\Tests\Support\FailingMarkStore {};
        $store->append(OutboxMessage::create('a', []));
        $relay = new OutboxRelay($store, new RecordingPublisher());

        $this->expectException(\RuntimeException::class);
        $relay->runOnce();
    }

    public function testLeasePreventsDoubleClaim(): void
    {
        $store = new InMemoryOutboxStore();
        $store->append(OutboxMessage::create('a', []));

        self::assertCount(1, $store->claim(10, 'w1', 60));
        self::assertCount(0, $store->claim(10, 'w2', 60));
    }

    public function testExpiredLeaseIsReclaimed(): void
    {
        $store = new InMemoryOutboxStore();
        $store->append(OutboxMessage::create('a', []));

        self::assertCount(1, $store->claim(10, 'w1', 0));
        $again = $store->claim(10, 'w2', 60);
        self::assertCount(1, $again);
        self::assertSame(2, $again[0]->attempts);
    }
}
