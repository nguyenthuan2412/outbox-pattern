<?php

declare(strict_types=1);

namespace Outbox\Swoole;

use Outbox\Relay\BatchResult;
use Outbox\Relay\OutboxRelay;
use Outbox\Worker\Worker;
use Outbox\Worker\WorkerOptions;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Runs N relay loops concurrently as coroutines in one process.
 *
 * The relay factory is called once per coroutine. That's where you give each coroutine
 * its own DB connection / broker channel (or a coroutine-safe pool). Sharing one PDO or
 * one AMQP channel across coroutines is NOT safe.
 *
 *   $worker = new SwooleWorker(
 *       relayFactory: fn (int $i) => new OutboxRelay(new MyStore($pool), new MyKafkaPublisher($producer)),
 *       concurrency: 8,
 *   );
 *   $worker->run();                          // standalone CLI
 *   $server->addProcess($worker->asProcess()); // or alongside a Swoole\Http\Server
 *
 * Remember to enable coroutine hooks (Swoole\Runtime::enableCoroutine(SWOOLE_HOOK_ALL))
 * so blocking I/O in your store/publisher yields instead of blocking.
 */
final class SwooleWorker
{
    /** @var \Closure(int): OutboxRelay */
    private \Closure $relayFactory;
    private readonly LoggerInterface $logger;

    /** @var array<int, Worker> */
    private array $workers = [];
    private bool $stopping = false;

    /** @var (\Closure(BatchResult, int): void)|null */
    private ?\Closure $onBatch = null;

    /**
     * @param callable(int $coroutineIndex): OutboxRelay $relayFactory
     */
    public function __construct(
        callable $relayFactory,
        private readonly int $concurrency = 4,
        private readonly WorkerOptions $options = new WorkerOptions(),
        ?LoggerInterface $logger = null,
        private readonly bool $handleSignals = true,
    ) {
        Runtime::assertAvailable();
        if ($concurrency < 1) {
            throw new \InvalidArgumentException('concurrency must be >= 1');
        }
        $this->relayFactory = $relayFactory(...);
        $this->logger = $logger ?? new NullLogger();
    }

    /** @param callable(BatchResult, int $coroutineIndex): void $listener */
    public function onBatch(callable $listener): self
    {
        $this->onBatch = $listener(...);

        return $this;
    }

    /**
     * Blocks until every coroutine loop has stopped. Returns total messages processed.
     * Works both from plain CLI (starts a scheduler) and from inside an existing coroutine.
     */
    public function run(): int
    {
        return (int) Runtime::run(fn () => $this->runInCoroutine());
    }

    public function stop(): void
    {
        $this->stopping = true;
        foreach ($this->workers as $worker) {
            $worker->stop();
        }
    }

    /**
     * Wrap the worker in a Swoole\Process for $server->addProcess(...).
     * The process restarts automatically if it exits (Swoole manager behaviour).
     *
     * @return \Swoole\Process|\OpenSwoole\Process
     */
    public function asProcess(string $name = 'outbox-relay'): object
    {
        $class = Runtime::process();

        return new $class(function () use ($name): void {
            if (function_exists('cli_set_process_title')) {
                @cli_set_process_title($name);
            }
            $this->run();
        }, false, 0, true);
    }

    private function runInCoroutine(): int
    {
        $channelClass = Runtime::channel();
        $done = new $channelClass($this->concurrency);
        $total = 0;

        if ($this->handleSignals) {
            $this->installSignalHandlers();
        }

        for ($i = 0; $i < $this->concurrency; ++$i) {
            Runtime::create(function () use ($i, $done): void {
                $processed = 0;
                try {
                    $worker = new Worker(($this->relayFactory)($i), $this->options, new CoroutineSleeper(), $this->logger);
                    if ($this->onBatch !== null) {
                        $listener = $this->onBatch;
                        $worker->onBatch(static fn (BatchResult $r) => $listener($r, $i));
                    }
                    $this->workers[$i] = $worker;
                    if ($this->stopping) {
                        $worker->stop();
                    }
                    $processed = $worker->run();
                } catch (\Throwable $e) {
                    $this->logger->critical('Outbox coroutine #' . $i . ' crashed: ' . $e->getMessage(), ['exception' => $e]);
                } finally {
                    unset($this->workers[$i]);
                    $done->push($processed);
                }
            });
        }

        for ($i = 0; $i < $this->concurrency; ++$i) {
            $total += (int) $done->pop();
        }

        if ($this->handleSignals) {
            $this->removeSignalHandlers();
        }

        return $total;
    }

    private function installSignalHandlers(): void
    {
        $process = Runtime::process();
        foreach ([\SIGTERM, \SIGINT] as $signal) {
            $process::signal($signal, function () {
                $this->logger->info('Outbox worker received stop signal, draining...');
                $this->stop();
            });
        }
    }

    private function removeSignalHandlers(): void
    {
        $process = Runtime::process();
        foreach ([\SIGTERM, \SIGINT] as $signal) {
            $process::signal($signal, null);
        }
    }
}
