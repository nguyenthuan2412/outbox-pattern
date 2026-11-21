<?php

declare(strict_types=1);

namespace Outbox\Tests\Unit;

use Outbox\Middleware\AddDeliveryHeaders;
use Outbox\Middleware\CallbackMiddleware;
use Outbox\Middleware\Pipeline;
use Outbox\OutboxMessage;
use Outbox\Tests\Support\RecordingPublisher;
use PHPUnit\Framework\TestCase;

final class PipelineTest extends TestCase
{
    public function testMiddlewaresRunInOrderAndCanModifyMessage(): void
    {
        $log = [];
        $publisher = new RecordingPublisher();
        $pipeline = new Pipeline(
            $publisher,
            new CallbackMiddleware(function ($m, $next) use (&$log) { $log[] = 'a:in'; $next($m); $log[] = 'a:out'; }),
            new CallbackMiddleware(function ($m, $next) use (&$log) { $log[] = 'b'; $next($m->withHeaders(['x' => '1'])); }),
        );

        $pipeline->publish(OutboxMessage::create('e', []));

        self::assertSame(['a:in', 'b', 'a:out'], $log);
        self::assertSame(['x' => '1'], $publisher->published[0]->headers);
    }

    public function testDeliveryHeaders(): void
    {
        $publisher = new RecordingPublisher();
        $msg = OutboxMessage::create('order.created', [], 'order', 42, ['trace' => 't1']);

        (new Pipeline($publisher, new AddDeliveryHeaders()))->publish($msg);

        $h = $publisher->published[0]->headers;
        self::assertSame($msg->id, $h['outbox-message-id']);
        self::assertSame('order.created', $h['outbox-event-type']);
        self::assertSame('42', $h['outbox-aggregate-id']);
        self::assertSame('t1', $h['trace']);
        self::assertSame('order:42', $msg->partitionKey());
    }

    public function testShortCircuitSkipsPublisher(): void
    {
        $publisher = new RecordingPublisher();
        (new Pipeline($publisher, new CallbackMiddleware(fn () => null)))->publish(OutboxMessage::create('e', []));
        self::assertSame([], $publisher->published);
    }
}
