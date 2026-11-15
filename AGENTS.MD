## Build and test

Use JDK 21 or later. Database and example tests require Docker for PostgreSQL
Testcontainers.

```sh
sbt test
```

For repeated invocations, connect to a persistent sbt server instead of
starting a new JVM each time — this skips the JVM startup on every run:

```sh
sbt --client test
```

The `example` module contains equivalent document-processing workflows using
AtomicFlow, workflows4s, dbos4s (DBOS Transact), and Temporal (Java SDK). API and storage details are in [`spec/`](spec/).
Runtime design notes are in [`docs/design/`](docs/design/).
[`DEVIATIONS.md`](DEVIATIONS.md) records implementation decisions and differences
from the design. System comparisons are in [`docs/comparison/`](docs/comparison/).
