<?php

declare(strict_types=1);

namespace Outbox\Middleware;

use Outbox\Contracts\PublishMiddleware;
use Outbox\OutboxMessage;

/**
 * Adds standard headers so consumers can de-duplicate and route:
 * outbox-message-id, outbox-event-type, outbox-aggregate-type, outbox-aggregate-id,
 * outbox-created-at, outbox-attempt.
 */
final class AddDeliveryHeaders implements PublishMiddleware
{
    public function process(OutboxMessage $message, callable $next): void
    {
        $headers = $message->headers + array_filter([
            'outbox-message-id' => $message->id,
            'outbox-event-type' => $message->eventType,
            'outbox-aggregate-type' => $message->aggregateType,
            'outbox-aggregate-id' => $message->aggregateId,
            'outbox-created-at' => $message->createdAt->format(\DATE_RFC3339_EXTENDED),
            'outbox-attempt' => (string) $message->attempts,
        ], static fn ($v) => $v !== null);

        $next($message->withHeaders($headers));
    }
}
