<?php

declare(strict_types=1);

namespace Outbox\Worker;

use Outbox\Contracts\Sleeper;
use Outbox\Relay\BatchResult;
use Outbox\Relay\OutboxRelay;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Polling loop around OutboxRelay. Runtime-agnostic: inject a Sleeper
 * (BlockingSleeper for FPM/CLI, CoroutineSleeper inside Swoole).
 */
final class Worker
{
    private bool $stopping = false;
    private readonly Sleeper $sleeper;
    private readonly LoggerInterface $logger;

    /** @var (\Closure(BatchResult): void)|null */
    private ?\Closure $onBatch = null;

    public function __construct(
        private readonly OutboxRelay $relay,
        private readonly WorkerOptions $options = new WorkerOptions(),
        ?Sleeper $sleeper = null,
        ?LoggerInterface $logger = null,
    ) {
        $this->sleeper = $sleeper ?? new BlockingSleeper();
        $this->logger = $logger ?? new NullLogger();
    }

    /** @param callable(BatchResult): void $listener */
    public function onBatch(callable $listener): self
    {
        $this->onBatch = $listener(...);

        return $this;
    }

    public function stop(): void
    {
        $this->stopping = true;
    }

    public function isStopping(): bool
    {
        return $this->stopping;
    }

    /**
     * Handle SIGTERM/SIGINT gracefully in blocking mode (finishes the current batch).
     * In Swoole use SwooleWorker which installs its own handlers.
     */
    public function installSignalHandlers(): void
    {
        if (!function_exists('pcntl_async_signals')) {
            return;
        }
        pcntl_async_signals(true);
        foreach ([\SIGTERM, \SIGINT, \SIGQUIT] as $signal) {
            pcntl_signal($signal, fn () => $this->stop());
        }
    }

    /**
     * @return int total messages processed
     */
    public function run(): int
    {
        $started = microtime(true);
        $processed = 0;
        $idle = $this->options->idleSleep;

        while (!$this->stopping) {
            try {
                $result = $this->relay->runOnce();
            } catch (\Throwable $e) {
                $this->logger->error('Outbox worker error: ' . $e->getMessage(), ['exception' => $e]);
                $this->sleeper->sleep($this->options->errorSleep);
                continue;
            }

            if ($this->onBatch !== null && !$result->isEmpty()) {
                ($this->onBatch)($result);
            }
            $processed += $result->claimed;

            if ($this->shouldStop($started, $processed)) {
                break;
            }

            if ($result->isEmpty()) {
                if ($this->options->stopWhenEmpty) {
                    break;
                }
                $this->sleeper->sleep($idle);
                $idle = min($this->options->maxIdleSleep, $idle * 2);
            } else {
                $idle = $this->options->idleSleep;
            }
        }

        return $processed;
    }

    private function shouldStop(float $started, int $processed): bool
    {
        $o = $this->options;

        return ($o->maxMessages > 0 && $processed >= $o->maxMessages)
            || ($o->maxRuntime > 0 && microtime(true) - $started >= $o->maxRuntime)
            || ($o->memoryLimitMb > 0 && memory_get_usage(true) >= $o->memoryLimitMb * 1024 * 1024);
    }
}
