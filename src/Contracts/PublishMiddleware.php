<?php

declare(strict_types=1);

namespace Outbox\Contracts;

use Outbox\OutboxMessage;

/**
 * Wraps the publish call (logging, metrics, tracing, header enrichment, rate limiting...).
 *
 * Call $next($message) to continue the pipeline. You may pass a modified message.
 * Throw to fail the publish.
 */
interface PublishMiddleware
{
    /**
     * @param callable(OutboxMessage): void $next
     */
    public function process(OutboxMessage $message, callable $next): void;
}
