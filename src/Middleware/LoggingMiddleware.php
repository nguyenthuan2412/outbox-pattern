<?php

declare(strict_types=1);

namespace Outbox\Middleware;

use Outbox\Contracts\PublishMiddleware;
use Outbox\OutboxMessage;
use Psr\Log\LoggerInterface;

final class LoggingMiddleware implements PublishMiddleware
{
    public function __construct(private readonly LoggerInterface $logger)
    {
    }

    public function process(OutboxMessage $message, callable $next): void
    {
        $start = hrtime(true);
        $context = ['id' => $message->id, 'event' => $message->eventType, 'attempt' => $message->attempts];
        try {
            $next($message);
            $this->logger->debug('Outbox message published', $context + ['ms' => (hrtime(true) - $start) / 1e6]);
        } catch (\Throwable $e) {
            $this->logger->warning('Outbox publish failed: ' . $e->getMessage(), $context + ['exception' => $e]);
            throw $e;
        }
    }
}
