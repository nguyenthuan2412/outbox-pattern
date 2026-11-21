<?php

declare(strict_types=1);

namespace Outbox\Swoole;

/**
 * Tiny compatibility layer over ext-swoole and ext-openswoole.
 *
 * @internal
 */
final class Runtime
{
    public static function available(): bool
    {
        return extension_loaded('swoole') || extension_loaded('openswoole');
    }

    public static function assertAvailable(): void
    {
        if (!self::available()) {
            throw new \RuntimeException('ext-swoole or ext-openswoole is required for the Swoole runtime.');
        }
    }

    public static function isOpenSwoole(): bool
    {
        return !extension_loaded('swoole') && extension_loaded('openswoole');
    }

    /** @return class-string */
    public static function coroutine(): string
    {
        return self::isOpenSwoole() ? \OpenSwoole\Coroutine::class : \Swoole\Coroutine::class;
    }

    /** @return class-string */
    public static function channel(): string
    {
        return self::isOpenSwoole() ? \OpenSwoole\Coroutine\Channel::class : \Swoole\Coroutine\Channel::class;
    }

    /** @return class-string */
    public static function process(): string
    {
        return self::isOpenSwoole() ? \OpenSwoole\Process::class : \Swoole\Process::class;
    }

    public static function inCoroutine(): bool
    {
        return self::coroutine()::getCid() > 0;
    }

    public static function create(callable $fn): int|false
    {
        return self::coroutine()::create($fn);
    }

    public static function sleep(float $seconds): void
    {
        self::coroutine()::sleep($seconds);
    }

    /**
     * Run $fn inside a coroutine scheduler, or directly if we're already in one.
     */
    public static function run(callable $fn): mixed
    {
        if (self::inCoroutine()) {
            return $fn();
        }

        $result = null;
        $error = null;
        $wrapped = static function () use ($fn, &$result, &$error): void {
            try {
                $result = $fn();
            } catch (\Throwable $e) {
                $error = $e;
            }
        };

        if (self::isOpenSwoole()) {
            \OpenSwoole\Coroutine::run($wrapped);
        } else {
            \Swoole\Coroutine\run($wrapped);
        }

        if ($error !== null) {
            throw $error;
        }

        return $result;
    }
}
