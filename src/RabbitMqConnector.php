<?php

declare(strict_types=1);

namespace Antevemus\ASpecificationRabbitMQ;

use Antevemus\ASpecificationRabbitMQ\Internal\Amqp;
use Closure;
use InvalidArgumentException;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use SensitiveParameter;

/**
 * RabbitMqConnector - Opens Connections to One RabbitMQ Broker
 *
 * A factory, not a holder: RabbitMqTransport and RabbitMqSource each call connect() for a connection of their
 * own, and again after the one they had broke. A connector made from the host, the port and the credentials
 * opens an AMQPStreamConnection of php-amqplib (pure PHP, ext-sockets; ext-amqp is not needed); one made by
 * fromFactory() runs the closure given, for TLS or any other option of php-amqplib; one made by fromUrl() reads
 * an `amqp://user:password@host:port/vhost` URL.
 *
 * The password never leaves the object: describe() names the broker by its host, port and virtual host only,
 * and the parameter is marked #[SensitiveParameter], so a stack trace does not show it either.
 *
 * <code>
 * $connector = RabbitMqConnector::fromUrl('amqp://guest:guest@127.0.0.1:5672/%2F');
 * $connector->describe(); // 'amqp://127.0.0.1:5672/'
 * </code>
 *
 * @version    1.0.0
 * @package    Antevemus\ASpecificationRabbitMQ
 * @author     Heliton Junior (CTO) - <contato@antevemus.com.br>
 * @copyright  Copyright (c) 2026 Antevemus Soluções Inovadoras em TI Ltda.
 * @license    MIT
 */
final class RabbitMqConnector
{
    /** @var Closure(): AbstractConnection */
    private Closure $factory;

    private string $description;

    /**
     * @param string $host Host name or address of the broker
     * @param int $port AMQP port (1 to 65535)
     * @param string $user User name
     * @param string $password Password (never shown)
     * @param string $vhost Virtual host
     * @param float $connectionTimeout Seconds to open the connection (> 0)
     * @param float $readWriteTimeout Seconds of a read or a write on the socket (> 0)
     * @param int $heartbeat Heartbeat interval in seconds (0 = none; with one, readWriteTimeout must be at least twice it)
     * @throws InvalidArgumentException When a value is out of its range
     */
    public function __construct(
        string $host = '127.0.0.1',
        int $port = 5672,
        string $user = 'guest',
        #[SensitiveParameter] string $password = 'guest',
        string $vhost = '/',
        float $connectionTimeout = 3.0,
        float $readWriteTimeout = 10.0,
        int $heartbeat = 0
    ) {
        if ($host === '' || $port < 1 || $port > 65535 || $vhost === '') {
            throw new InvalidArgumentException('A RabbitMQ connector needs a host, a port from 1 to 65535 and a virtual host.');
        }
        if (!Amqp::isPositive($connectionTimeout) || !Amqp::isPositive($readWriteTimeout) || $heartbeat < 0 || $readWriteTimeout < 2 * $heartbeat) {
            throw new InvalidArgumentException('The timeouts of a RabbitMQ connector must be positive, the heartbeat zero or more, and the read/write timeout at least twice the heartbeat.');
        }
        $this->description = sprintf('amqp://%s:%d/%s', $host, $port, $vhost === '/' ? '' : $vhost);
        $this->factory = static fn(): AbstractConnection => new AMQPStreamConnection(
            $host,
            $port,
            $user,
            $password,
            $vhost,
            connection_timeout: $connectionTimeout,
            read_write_timeout: $readWriteTimeout,
            heartbeat: $heartbeat
        );
    }

    /**
     * A connector from an `amqp://user:password@host:port/vhost` URL: the user, the password and the virtual
     * host percent-decoded (`%2F` is `/`); the port 5672, the user and the password `guest` and the virtual host
     * `/` when absent. Other schemes (amqps) are refused: use fromFactory() with an AMQPSSLConnection.
     *
     * @throws InvalidArgumentException When the text is not such a URL (the message never repeats it)
     */
    public static function fromUrl(#[SensitiveParameter] string $url): self
    {
        $parts = parse_url($url);
        if ($parts === false || ($parts['scheme'] ?? null) !== 'amqp' || !isset($parts['host']) || isset($parts['query']) || isset($parts['fragment'])) {
            throw new InvalidArgumentException('A RabbitMQ URL is amqp://user:password@host:port/vhost (the URL given is not shown, it may hold a password).');
        }
        $path = $parts['path'] ?? '';

        return new self(
            $parts['host'],
            $parts['port'] ?? 5672,
            rawurldecode($parts['user'] ?? 'guest'),
            rawurldecode($parts['pass'] ?? 'guest'),
            $path === '' || $path === '/' ? '/' : rawurldecode(substr($path, 1))
        );
    }

    /**
     * A connector that runs a closure of the application for each connection (TLS, a cluster of hosts, any
     * option of php-amqplib).
     *
     * @param Closure(): AbstractConnection $factory
     * @param string $description How the broker is named in a reason (never a password)
     */
    public static function fromFactory(Closure $factory, string $description = 'amqp (custom connection)'): self
    {
        $connector = new self();
        $connector->factory = $factory;
        $connector->description = $description;

        return $connector;
    }

    /**
     * Opens a new connection.
     *
     * @throws \Throwable What php-amqplib throws when the broker cannot be reached or refuses the login
     */
    public function connect(): AbstractConnection
    {
        return ($this->factory)();
    }

    /** The broker, as a reason names it: scheme, host, port and virtual host, never the credentials. */
    public function describe(): string
    {
        return $this->description;
    }
}
