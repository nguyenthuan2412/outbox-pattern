<?php

declare(strict_types=1);

namespace Outbox\Relay;

enum Outcome: string
{
    case Published = 'published';
    case Retry = 'retry';
    case Dead = 'dead';
}
