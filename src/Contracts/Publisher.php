<?php

declare(strict_types=1);

namespace Outbox\Contracts;

use Outbox\OutboxMessage;

/**
 * Delivers an outbox message to the outside world (Kafka, RabbitMQ, Redis, HTTP, ...).
 *
 * Must throw on failure; returning normally means the message was delivered.
 * Delivery is at-least-once: consumers should de-duplicate using $message->id
 * (also sent as the "outbox-message-id" header by the bundled publishers).
 */
interface Publisher
{
    public function publish(OutboxMessage $message): void;
}
