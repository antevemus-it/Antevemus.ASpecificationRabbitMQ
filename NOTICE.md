Antevemus ASpecification RabbitMQ
Copyright (c) 2026 Antevemus Soluções Inovadoras em TI Ltda.
License: MIT (SPDX: MIT) — see LICENSE.

Website: https://antevemus.com.br
Email: contato@antevemus.com.br

-------------------------------------------------------------------------------
1. SCOPE & PURPOSE
-------------------------------------------------------------------------------

Antevemus ASpecification RabbitMQ is the RabbitMQ satellite of Antevemus
ASpecification (antevemus/aspecification) for PHP 8.2+. It implements the two
interfaces of the broker-agnostic messaging hook of the core
(IMessageTransport, IMessageSource) over RabbitMQ: a transport with publisher
confirms and the mandatory flag, a source over quorum queues with manual
acknowledgements, the delivery count of the broker, requeues kept in order and
a dead letter exchange, and an idempotent declaration of that topology.

-------------------------------------------------------------------------------
2. PROVENANCE — NOTHING PORTED
-------------------------------------------------------------------------------

No source code of any third-party project was copied, ported or translated
into this package. It is an independent implementation, written by Antevemus
Soluções Inovadoras em TI Ltda. on the public API of its runtime dependencies
(section 4) and on the public documentation of RabbitMQ and of the AMQP 0-9-1
protocol (publisher confirms, mandatory publishing, quorum queues, dead letter
exchanges); the behaviour of RabbitMQ 3.13 it relies on (a quorum queue puts a
message rejected with requeue at the tail of the queue, gives the deliveries
of a closed connection back at its head, and counts the deliveries in
x-delivery-count) was measured against the broker and is checked by the test
suite. The only code it did not write for this package is first-party code of
the same author, copied on purpose (section 3).

-------------------------------------------------------------------------------
3. FIRST-PARTY CODE COPIED ON PURPOSE
-------------------------------------------------------------------------------

The documentation guards tests/Unit/DocumentationTreeTest.php and
tests/Unit/DocumentationBlocksTest.php are copied from Antevemus ASpecification
BR (antevemus/aspecification-br 1.1.0, itself copying them from Antevemus
ASpecification Revolt 1.0.0), by the same author and under the same MIT
License, and adapted (the table of blocks; the blocks that talk to a broker).

    Origin: DocumentationTreeTest, DocumentationBlocksTest (antevemus/aspecification-br 1.1.0)
    Originator: Antevemus Soluções Inovadoras em TI Ltda.
    License: MIT License
    Website: https://github.com/antevemus-it/Antevemus.ASpecificationBr

-------------------------------------------------------------------------------
4. THIRD-PARTY COMPONENTS & LICENSES
-------------------------------------------------------------------------------

Runtime, installed by Composer as separate packages, each with its own license
file; none of them is copied into or distributed with this package:

- antevemus/aspecification (^2.15, MIT, by the same author), which brings
  antevemus/adeep-property-access (MIT, same author) and the Symfony polyfills
  symfony/polyfill-mbstring and symfony/polyfill-php82 (MIT).
- php-amqplib/php-amqplib (^3.7, LGPL-2.1-or-later), the AMQP 0-9-1 client,
  which brings phpseclib/phpseclib (MIT) and paragonie/constant_time_encoding
  (MIT). It needs the PHP extensions sockets and mbstring.

What the LGPL of php-amqplib means for this MIT package: this package does not
contain, modify or redistribute any part of php-amqplib; it declares it as a
dependency and calls its public classes, which Composer installs, from
Packagist, next to it. In the terms of the LGPL 2.1 (section 5) this package
is a "work that uses the Library", not a work based on it, so the LGPL does not
extend to it and it stays under the MIT License alone. The LGPL governs
php-amqplib itself: whoever distributes php-amqplib, alone or bundled with an
application (a release archive that includes the vendor/ directory, for
example), must do so under the LGPL: keep its license file and copyright
notices, give its source (in PHP the distributed files are the source), let the
recipient modify it and replace it with another version (in a Composer
installation, by updating the package), and not forbid the reverse engineering
needed to debug such modifications. A change made to php-amqplib itself is a
modification of the Library and is LGPL. None of this restricts the license of
the application or of this package.

Development only: phpunit/phpunit (BSD-3-Clause) and infection/infection
(BSD-3-Clause), with their own dependencies; none of them is distributed with
the package. The tests run against the official RabbitMQ Docker image
(rabbitmq:3.13, MPL-2.0), which is not distributed either.
