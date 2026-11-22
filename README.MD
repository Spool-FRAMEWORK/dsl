# Spool — DSL

[![License](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/build-Maven-C71A36.svg)](https://maven.apache.org/)

**DSL** is the **Spool** framework module that lets you **declare your entire pipeline in a single `Spool.yaml` file** and boot it with one line of Java. No wiring code, no boilerplate.

Every YAML example in this README is loaded by `ReadmeExamplesTest`, so it is the real format of the descriptor.

---

## Table of Contents

- [What it does](#what-it-does)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Spool.yaml Reference](#spoolyaml-reference)
  - [Infrastructure](#infrastructure)
  - [Crawler](#crawler)
  - [Janitor](#janitor)
  - [Durations](#durations)
  - [Schedule](#schedule)
  - [Quarantine](#quarantine)
  - [Ingester](#ingester)
  - [Mounter](#mounter)
  - [Authentication](#authentication)
  - [Includes and profiles](#includes-and-profiles)
  - [Variables](#variables)
- [Infrastructure Options](#infrastructure-options)
- [License](#license)

---

## What it does

```
Spool.yaml
│
▼
SpoolNodeDSL.fromDescriptor(path)  →  SpoolNode
│
├── infrastructure  →  named pieces: EVENT_BUS · INBOX · DATA_LAKE · DATA_MART · QUARANTINE_REPOSITORY · AGGREGATE_METADATA
│
└── SpoolModuleDescriptor[]
    ├── CrawlerDescriptor   →  CrawlerBuilderFactory  →  Crawler
    ├── JanitorDescriptor   →  JanitorBuilderFactory  →  Janitor
    └── IngesterDescriptor  →  IngesterBuilderFactory →  Ingester
```

Each piece of the infrastructure names its adapter with `type`, and the adapter is found among the plugins registered through the Java
SPI (`spool-infrastructure`). The whole descriptor is checked before any module starts: a wrong key or value stops
the node and the error says its path.

---

## Installation

```xml
<dependency>
    <groupId>io.github.spool-framework</groupId>
    <artifactId>dsl</artifactId>
    <version>${spool.version}</version>
</dependency>
```

Releases are on [Maven Central](https://central.sonatype.com/artifact/io.github.spool-framework/dsl). The `SNAPSHOT`
of `develop` is on GitHub Packages:

```xml
<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/Spool-FRAMEWORK/*</url>
    </repository>
</repositories>
```

> Requires **Java 21+**.

---

## Quick Start

### 1. Create `src/main/resources/Spool.yaml`

```yaml
watchdog: "http://localhost:8090"

infrastructure:
  bus:
    kind: EVENT_BUS
    type: KAFKA
    configuration:
      bootstrap.servers: "localhost:9092"

  inbox:
    kind: INBOX
    type: S3
    configuration:
      bucket: spool-inbox
      region: us-east-1
      endpoint: http://localhost:4566

  lake:
    kind: DATA_LAKE
    type: S3
    configuration:
      bucket: spool-datalake
      region: us-east-1
      endpoint: http://localhost:4566

  quarantine:
    kind: QUARANTINE_REPOSITORY
    type: FILE_SYSTEM
    configuration:
      path: /data/spool/quarantine

modules:
  - crawler:
      type: POLL
      id: my-crawler
      schedule:
        every: 1m
      source:
        type: HTTP
        configuration:
          sourceId: my-source
          url: "https://api.example.com/data"
        normalizer: JSON_ARRAY
        mediaType: JSON
      eventMapping:
        namingConvention: SNAKE_CASE

  - ingester:
      type: FLUSH
      id: my-ingester
      configuration:
        size: 100
        every: 10s

  - janitor:
      id: my-janitor
      schedule:
        every: 5s
```

### 2. Boot the pipeline

```java
public class Main {
    public static void main(String[] args) throws IOException {
        SpoolNode node = SpoolNodeDSL.fromDescriptor("/Spool.yaml");
        node.start();
    }
}
```

`fromDescriptor(String)` reads the descriptor from the classpath and `fromDescriptor(Path)` from the file system, so
the same jar can run with different descriptors. Through `SpoolRuntime`, which also configures OpenTelemetry:

```java
SpoolRuntime.builder()
        .OpenTelemetryConfiguration(otelConfig)
        .withNodeFromDSL("/Spool.yaml")
        .build()
        .start();
```

### Several nodes from a folder

`SpoolNodeDSL.fromFolder(Path)` and `fromFolder(String)` give a node for every file of a folder and its subfolders
whose name ends in `.spool.yaml`, from the file system or from the classpath (also inside a jar). Any other file is
not a node, so the pieces the descriptors include can live next to them:

```
pipelines/
├── shared/
│   └── kafka.yaml              included by both, not a node
├── boe/
│   ├── boe.spool.yaml          a node
│   └── boe-modules.yaml        included by boe, not a node
└── bocm/
    └── bocm.spool.yaml         a node
```

Each node opens its HTTP server on the port of `server.port`, 8080 when the descriptor does not say it:

```yaml
server:
  port: 8091
infrastructure:
  bus: {kind: EVENT_BUS, type: IN_MEMORY}
```

Two descriptors of the same process on the same port stop the load, naming both. So do a descriptor that cannot be
loaded, naming its file, and a folder without any `.spool.yaml`. One node with its modules in several files is not
this: it is one descriptor with includes (see [Includes and profiles](#includes-and-profiles)).

---

## Spool.yaml Reference

### Infrastructure

`infrastructure` is a map of pieces
([ADR-018](https://linear.app/javier-castilla/document/adr-018-la-infraestructura-se-declara-como-piezas-con-nombre-y-cada-31e03aba25db)).
The key of each piece is a name of your choice, and the piece says what it is with `kind`, which plugin builds it
with `type`, and that plugin's keys in `configuration`. The types and their keys are in
[Infrastructure Options](#infrastructure-options).

| Kind | What it is |
|---|---|
| `EVENT_BUS` | The bus the modules publish and consume events on. |
| `INBOX` | Where the crawler stores what it captures and the janitor looks after it. |
| `DATA_LAKE` | Where the ingester writes bronze. The mounter reads it and writes silver into it, and the janitor writes back to it what was quarantined from bronze. |
| `DATA_MART` | Where mounters to gold write. Only they need it. |
| `QUARANTINE_REPOSITORY` | Where the janitor and the mounters to silver keep what is in quarantine (see [Quarantine](#quarantine)). |
| `AGGREGATE_METADATA` | Where a mounter to gold with an incremental aggregator keeps the state and the sources of each aggregate (see [Aggregating incrementally](#aggregating-incrementally)). It is optional. |

A module takes the only piece of each kind it needs, so with one piece of each kind there is nothing else to write.
With several pieces of a kind, each module that needs that kind names its piece in `uses`:

```yaml
infrastructure:
  bus: {kind: EVENT_BUS, type: IN_MEMORY}
  lake: {kind: DATA_LAKE, type: FILE_SYSTEM, configuration: {path: /data/lake}}
  gold: {kind: DATA_MART, type: FILE_SYSTEM, configuration: {path: /data/gold}}
  reports: {kind: DATA_MART, type: FILE_SYSTEM, configuration: {path: /data/reports}}

modules:
  - mounter:
      id: gold-total
      uses: [gold]
      type: POLL
      schedule: {every: 1h}
      mode: AGGREGATION
      read: {partitions: ["source=payments"]}
      write: {dataMart: payments-total, partitioning: {type: SAME_AS_SOURCE}}
  - mounter:
      id: reports-total
      uses: [reports]
      type: POLL
      schedule: {every: 1d}
      mode: AGGREGATION
      read: {partitions: ["source=payments"]}
      write: {dataMart: payments-report, partitioning: {type: SAME_AS_SOURCE}}
```

- A module that needs a kind with several pieces and names none stops the load, listing the pieces of that kind.
  So does a name in `uses` that is not declared, two pieces of the same kind in one `uses`, and a piece of a kind
  the module does not use.
- A module only needs the pieces it uses: a node with only an ingester needs no inbox. A missing piece stops the node
  with the name of the module.
- Modules on different buses do not see each other's events. The crawler and the janitor of an inbox, and the
  ingester and the mounters of a data lake, have to use the same bus.
- `watchdog`, at the root of the descriptor, is the URL the modules send their heartbeat to. It is optional.
- A descriptor of the previous format, with a fixed key per component (`eventBus:`, `inbox:`...), stops the load
  saying which `kind` to add to each one, and `infrastructure.watchdog` says to move it to the root.

#### Health of each piece

The health of the node (`GET /spool/health`) lists, for each module, every piece it uses with its name, its type and
its status, and the heartbeat sends the same to the watchdog
([ADR-030](https://linear.app/javier-castilla/document/adr-030-la-salud-de-un-modulo-sale-de-cada-pieza-de-infraestructura-abe3aade918e)).
A piece is `UNHEALTHY` while the last call to it failed or its check fails, and `DEGRADED` when its check is slow. A
module takes the worst status of its pieces, and the node answers 503 when one is `UNHEALTHY`.

The check of a piece comes from the plugin of its type (`FILE_SYSTEM`, `RAW_FILE_SYSTEM`, `S3` and `KAFKA` have one);
a type without one is judged only by the calls to it. Two optional keys of the piece's `configuration` change how
often it is checked:

| Key | Default | What it does |
|---|---|---|
| `healthCheckInterval` | `10s` | The shortest time between two checks; in between, the last result is reused |
| `healthSlowAfter` | `2s` | A check that takes longer leaves the piece `DEGRADED` |

```yaml
infrastructure:
  lake: {kind: DATA_LAKE, type: S3, configuration: {bucket: lake, region: eu-west-1, endpoint: "https://s3.eu-west-1.amazonaws.com", healthCheckInterval: 30s}}
```

---

### Crawler

Reads a source and stores each record in the inbox. A `POLL` crawler asks the source on its schedule; a `STREAM`
crawler listens to it.

```yaml
- crawler:
    type: POLL
    id: my-crawler
    schedule:
      every: 1m                 # without a schedule, a POLL crawler polls once; see Schedule
    source:
      type: HTTP
      configuration:
        sourceId: my-source
        url: "https://api.example.com/data"
      normalizer: JSON_ARRAY    # how the response is split into records
      mediaType: JSON           # what the records hold
      rootPath: results         # dot-notation path to the records, optional
      enrichment:
        - source: entityName    # field copied into every record
          target: entity        # with this name, optional
    eventMapping:
      namingConvention: SNAKE_CASE
      attributeList:
        - entityName
    errorRouter:
      type: DEFAULT
```

| Key | Description |
|---|---|
| `type` | `POLL`, `STREAM` or `WEBHOOK`. |
| `source.type` | The source plugin: `HTTP` for `POLL`, `IN_MEMORY` for `STREAM`. |
| `source.normalizer` | The normalizer that turns what the source reads into records: `JSON`, `JSON_ARRAY`, `JSON_OBJECT`, `EVENT_CLASS`, `EVENT_CLASS_ARRAY` or `PDF`, or one composed from steps (see below). |
| `source.mediaType` | The media type of the records, as an alias (`JSON`, `TEXT`, `PDF`, `PNG`, `JPEG`, `MP4`) or in full (`application/x-ndjson`). It only labels the records. |
| `eventMapping.namingConvention` | How field names are written in the events. |
| `eventMapping.domainMappingList` | The domain events the crawler publishes, by the simple name of their class. Optional. |
| `eventMapping.eventPackage` | The package of those classes. Optional; `events` when missing. |
| `errorRouter` | Where the crawler reports what fails. Optional; `DEFAULT` when missing. |

### Webhook

A `WEBHOOK` crawler receives its records instead of asking for them: senders `POST` them to a path of the node's HTTP
server. Every delivery is identified by an authentication provider, and the sender needs a role
([ADR-013](https://linear.app/javier-castilla/document/adr-013-toda-entrada-http-se-autentica-con-un-proveedor-con-nombre-7a1ffe638f42)).
The path, the provider and the role are required, so a webhook is never open.

```yaml
authentication:
  boe-sender:
    type: HMAC_SIGNATURE
    configuration:
      header: X-Signature-256
      secret: ${env:BOE_WEBHOOK_SECRET}
      timestampHeader: X-Signature-Timestamp   # refuses a signed request sent again later
      tolerance: PT5M
      roles: boe-publisher
infrastructure:
  bus: {kind: EVENT_BUS, type: IN_MEMORY}
  inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: /data/inbox}}
modules:
  - crawler:
      type: WEBHOOK
      id: boe-webhook
      source:
        type: WEBHOOK
        configuration:
          sourceId: boe
          path: /spool/webhooks/boe
        normalizer: JSON_ARRAY
        mediaType: JSON
      eventMapping:
        namingConvention: SNAKE_CASE
      webhook:
        authentication: boe-sender
        role: boe-publisher
        maxBodySize: 1048576    # bytes, 1 MiB by default
```

| Answer | When |
|---|---|
| `202` | The records were stored, or were already in the inbox. The body says how many of each. |
| `400` | No records could be read from the body. |
| `401` / `403` | The sender is not identified, or lacks the role. |
| `413` | The body is bigger than `maxBodySize`. It is refused before being read whole. |
| `415` | The `Content-Type` is not the `mediaType` of the source. |
| `503` | The inbox failed: the sender should send the delivery again. |

A delivery sent twice stores its records once, by their idempotency key. With `HMAC_SIGNATURE`, sign a timestamp too:
without one, a captured request could be sent again.

### Composing a normalizer

`JSON`, `JSON_ARRAY` and `JSON_OBJECT` publish their stages: `raw`, `deserialized`, `extracted`, `located`, `split` and `serialized`. A descriptor can add steps after a stage, replace the step that gives a stage, or replace every step between two stages. Replacements run first, then additions.

```yaml
- crawler:
    type: POLL
    id: boe-crawler
    source:
      type: HTTP
      configuration:
        sourceId: boe
        url: "https://www.boe.es/datosabiertos/api/boe/sumario/20261121"
      normalizer:
        from: JSON_ARRAY
        after:
          deserialized: [BOE_SUMMARY]   # runs right after the payload is read as JSON
      mediaType: JSON
      rootPath: data.sumario.diario.0.items
    eventMapping:
      namingConvention: SNAKE_CASE
```

Replacing a step, or a stretch of steps:

```yaml
- crawler:
    type: POLL
    id: boe-crawler
    source:
      type: HTTP
      configuration:
        sourceId: boe
        url: "https://www.boe.es/datosabiertos/api/boe/sumario/20261121"
      normalizer:
        from: JSON_ARRAY
        replace:
          - stage: deserialized              # the step that gives this stage
            with: READ_XML
          - between: [split, serialized]     # every step from split to serialized
            with: [TO_CSV_LINE, TEXT_TO_BYTES]
      mediaType: text/csv
      rootPath: sumario.diario.items
    eventMapping:
      namingConvention: SNAKE_CASE
```

A normalizer can also be built only from steps, from the raw payload to the records: `normalizer: {steps: [LINES, TEXT_TO_BYTES]}`.

A step is a class that implements `Step` from core, with a public constructor without arguments, annotated with `@NormalizerStep("NAME")` from the crawler. The crawler's annotation processor lists it at compile time, so it only has to be on the classpath. The node checks that every step takes what the step before it gives when it loads the descriptor, and logs the normalizer that results.

---

### Janitor

Runs on a schedule to keep the inbox healthy: republishes stuck envelopes, quarantines failed ones, removes expired
ones.

```yaml
- janitor:
    id: my-janitor
    schedule:
      every: 5s                 # 5s by default
    configuration:
      threshold: 10s
      ttl: 1d
      maxInboxUpdateAttempts: 3
```

| Key | Description |
|---|---|
| `schedule` | When the janitor runs a round, as described in [Schedule](#schedule). Every 5s by default. |
| `configuration.threshold` | How long an envelope can stay without moving before it is republished. |
| `configuration.ttl` | How long an envelope stays in the inbox before it expires. |
| `configuration.maxInboxUpdateAttempts` | Rounds an event is tried when the inbox fails to apply it. 3 by default. |
| `configuration.processedKeysWindow` | How long the janitor remembers the envelopes it marked as persisted, so a repeated `EnvelopePersisted` does not start their `ttl` again. 10m by default. |

With a `quarantineServer` block, the janitor serves on its node's HTTP server an API to review and resolve the
quarantine ([ADR-014](https://linear.app/javier-castilla/document/adr-014-el-janitor-expone-una-api-http-para-revisar-y-resolver-la-e7e6abbdabeb)).
Without it, there is no API.

```yaml
- janitor:
    id: orders-janitor
    quarantineServer:
      authentication: people
      roles:
        read: quarantine-reader
        resolve: quarantine-resolver
        bypass: quarantine-bypasser
      pageSize: 100
      maxPageSize: 1000
```

| Key | Description |
|---|---|
| `quarantineServer.authentication` | The provider of the `authentication` block that identifies the callers. |
| `quarantineServer.roles.read` | The role that lists the quarantine and shows a record. |
| `quarantineServer.roles.resolve` | The role that republishes, amends and purges a record. |
| `quarantineServer.roles.bypass` | The role that lets a record through although a validator rejected it. |
| `quarantineServer.pageSize` | Records per page when the caller does not say. 100 by default. |
| `quarantineServer.maxPageSize` | Records a page can have at most. 1000 by default. |

The routes are `GET /spool/quarantine?status=&limit=&cursor=`, `GET /spool/quarantine/{key}` and
`POST /spool/quarantine/{key}/republish`, `/amend` (the amended payload as the body), `/bypass`
(`{"reason": "..."}`) and `/purge`. Each resolution keeps who made it. The contract is
`quarantine-api.yaml` in the janitor.

### Durations

Every time in the descriptor is a duration, in ISO 8601 (`PT5S`, `P30D`) or in the short form: `100ms`, `5s`, `10m`,
`2h`, `30d`. A bare number is not accepted, because it would not say its unit. The keys that held milliseconds
(`millisecondsTTL`, `scheduleMilliseconds`...) are no longer read: loading fails and names the key that replaces
them ([ADR-011](https://linear.app/javier-castilla/document/adr-011-el-descriptor-admite-variables-propias-y-de-entorno-y-b7e8390d83c6)).

### Schedule

A polling crawler, a janitor and a polling mounter say when they run with `schedule`, in one of three forms
([ADR-015](https://linear.app/javier-castilla/document/adr-015-cuando-se-ejecuta-un-modulo-se-describe-con-un-schedule-019f11c013cd)).

```yaml
- janitor:
    id: once
    schedule: once                                     # one round when the module starts
- janitor:
    id: every-five-minutes
    schedule: {every: 5m}                              # when the module starts, then 5 minutes after each round ends
- janitor:
    id: first-of-the-month
    schedule: {cron: "0 8 1 * *", zone: Europe/Madrid} # the 1st of each month at 8:00 in Madrid, also in summer time
```

A cron expression is a Unix one of 5 fields: minute, hour, day of the month, month and day of the week. Each field
takes `*`, a value (`5`), a list (`1,15`), a range (`9-18`) or a step (`*/15`, `0-30/10`). Months and days of the
week also take names (`JAN`...`DEC`, `SUN`...`SAT`), and Sunday is `0` or `7`. The macros `@hourly`, `@daily`,
`@midnight`, `@weekly`, `@monthly`, `@yearly` and `@annually` stand for their usual expressions.

The `zone` is required with `cron`, because the same descriptor would otherwise run at different hours depending on
where it is deployed. It takes any time zone Java knows, such as `Europe/Madrid`, `UTC` or `+02:00`.

A Unix cron expression has some limits. It has no seconds, so a shorter period needs `every`. It cannot say "the last
day of the month" or "the first Monday of the month". With both a day of the month and a day of the week,
either of them is enough: `0 0 1 * MON` runs on the 1st and on every Monday. `@reboot` is not accepted, since
`schedule: once` does that.

The dates that pass while a round is running, or while the node is stopped, are not caught up: the next round is
the next date of the expression after the round ends. Loading fails, with the path of the value, on an expression
that is not valid, a zone that does not exist, a `cron` without `zone`, or two forms at once.

---

### Quarantine

An envelope goes to quarantine when the ingester finds that its payload fails a critical validation, when the janitor has republished it too many times without success, or when the mounter rejects it on its way from bronze to silver. The quarantine repository keeps one record per envelope:

| Field | Content |
|---|---|
| `envelope` | The whole envelope: key, metadata (source, correlation id...), media type, payload, retries and dates. |
| `location` | Where the envelope stayed: `INBOX`, or `BRONZE` with its partition. |
| `history` | Every time the envelope came into quarantine: its `reason` (`VALIDATION_FAILED` or `RETRIES_EXHAUSTED`), its `violations`, `quarantinedAt`, and how that time was resolved (`PENDING`, `REPUBLISHED`, `CORRECTED` with the new key, or `PURGED`). The last entry is the current one. |

```yaml
infrastructure:
  quarantineRepository:
    kind: QUARANTINE_REPOSITORY
    type: FILE_SYSTEM
    configuration:
      path: /data/spool/quarantine   # one folder per status, one JSON file per envelope named after its key
```

The janitor records what comes from the inbox and the mounter what it rejects; the ingester just publishes `EnvelopeQuarantined`. A pending record can be republished as it was, republished with a corrected payload (under a new key, with the original one in its `CORRECTED_FROM` metadata) or purged, even after the inbox has removed the envelope. Each of these marks the record and publishes `EnvelopeRepublished` or `EnvelopePurged`. If a resolved envelope comes back into quarantine, its record is reopened with a new entry.

Two settings in the janitor configuration:

| Key | Effect |
|---|---|
| `quarantineTtl` | Removes from the inbox what has been in `QUARANTINED` for longer. Its record stays pending. |
| `quarantineRetention` | Deletes the records resolved longer ago. Pending records are never deleted. Without it, resolved records are kept. |

A record the mounter rejected stays in bronze no longer: the mounter removes it from its bronze partition after keeping the record. Resolving it writes the envelope back to that partition (or the corrected one, under its new key) and publishes `EnvelopePersisted` so it is mounted again; the inbox is not touched. The janitor writes to the `DATA_LAKE` piece; without one, those records are skipped.

The old `infrastructure.quarantineStore` key is no longer accepted.

---

### Ingester

Consumes events from the bus and writes envelopes to the data lake.

```yaml
- ingester:
    type: FLUSH
    id: my-ingester
    configuration:
      size: 100                 # 100 by default
      every: 10s
```

| Type | Behaviour |
|---|---|
| `FLUSH` | Writes in batches: when `size` envelopes arrive or, if set, `every` that long. |
| `REACTIVE` | Writes each envelope immediately on arrival. |

An envelope can be announced more than once: the bus delivers at least once, and the janitor republishes an
envelope that waits longer than its `threshold`. The ingester remembers the envelopes it wrote for
`configuration.processedKeysWindow` (10m by default): an envelope announced again within it is not written again,
but its `EnvelopePersisted` is published again. An envelope sent to quarantine is not remembered, so it is validated
again when it comes back ([ADR-007](https://linear.app/javier-castilla/document/adr-007-cada-consumidor-recuerda-las-claves-que-ya-ha-procesado-3544b675ef87),
[ADR-010](https://linear.app/javier-castilla/document/adr-010-un-rechazo-no-cuenta-como-procesado-y-solo-recuerdan-claves-aee50158955c)).
The keys are kept in the memory of the process: a restart forgets them, and each replica has its own.

---

### Mounter

Fills one data mart from a partition of the data lake. A `TRANSFORMATION` mount reads bronze and writes silver in the
data lake; an `AGGREGATION` mount reads silver and writes gold in the data mart.

```yaml
- mounter:
    id: payments-silver
    type: POLL
    schedule:
      every: 1m
    mode: TRANSFORMATION
    validate: true
    read:
      partitions: ["source=payments/eventType=Payment"]
    write:
      dataMart: payments
      partitioning:
        type: ATTRIBUTES
        attributes: [payload.country]
```

| Key | Description |
|---|---|
| `type` | `POLL` mounts every `schedule.every`, which it requires; `REACTIVE` mounts when what it reads has just been written: a `TRANSFORMATION` mount each time the ingester reports a written partition of bronze, and an `AGGREGATION` mount each time a mounter of silver reports a mounted partition that holds what it reads. |
| `mode` | `TRANSFORMATION`, from bronze to silver, or `AGGREGATION`, from silver to gold. |
| `validate` | Validates on read what is promoted to silver, with the validators of each source. Only for `TRANSFORMATION`. `false` by default. |
| `aggregator` | Full name of a class that implements `MountAggregator` and has a public constructor without arguments. Without it, records are copied as they are. |
| `read.partitions` | The partitions to read, written as paths, at least one. The mounter reads them all in one go, so an aggregator combines records of sources that do not hang from the same partition, and a record kept in two partitions that overlap is read once. |
| `write.dataMart` | The data set the mounter fills, inside its data lake or its data mart. It is not the name of a piece: that goes in `uses`. |
| `write.partitioning.type` | `SAME_AS_SOURCE` writes each record in the partition it comes with: the one it had in the data lake, or the one the aggregator gave it. `ATTRIBUTES` builds it from `write.partitioning.attributes`, such as `payload.country`. |

A mounter to silver always has a quarantine, so it needs a `QUARANTINE_REPOSITORY` piece. A record that fails
validation, or that lacks a partition attribute, goes to it and is removed from bronze. Writing silver needs a data
lake type that mounters can write to: `FILE_SYSTEM` today.

#### Aggregating incrementally

An `AGGREGATION` mounter whose `aggregator` implements `IncrementalMountAggregator` can add what is new to what it
aggregated before, instead of reading silver whole in each round
([ADR-027](https://linear.app/javier-castilla/document/adr-027-el-mounter-agrega-de-forma-incremental-con-un-agregador-que-c8a5f0f44866)).
It does so when its infrastructure has an `AGGREGATE_METADATA` piece, the only one of its kind or the one it names in
`uses`:

```yaml
infrastructure:
  bus: {kind: EVENT_BUS, type: IN_MEMORY}
  lake: {kind: DATA_LAKE, type: FILE_SYSTEM, configuration: {path: /data/lake}}
  gold: {kind: DATA_MART, type: FILE_SYSTEM, configuration: {path: /data/gold}}
  aggregates: {kind: AGGREGATE_METADATA, type: FILE_SYSTEM, configuration: {path: /data/aggregates}}
```

- Each round reads only the partitions of silver that are closed and not read yet. A partition is closed 15 minutes
  after its last date entry ends, so `year=2026/month=11/day=17/hour=10` is read from 11:15 UTC on. Records that
  reach it later do not reach gold.
- The piece keeps, for each aggregate, the partitions of silver it comes from, how many records it has added, when it
  changed and the state of its aggregator, in JSON. The result of an aggregate that changes replaces the one before
  it, so gold has a single result per aggregate.
- If the partitions read include one without a date, such as `country=ES`, the mounter aggregates them all again in
  every round.
- A mounter that names the piece in `uses` with an aggregator that is not incremental, or that is a `TRANSFORMATION`
  mount, stops the load.
- Without the piece, the mounter aggregates everything in every round, as any other.

---

### Authentication

The HTTP entries of a node check who calls them with a provider declared by name in the `authentication` block
([ADR-013](https://linear.app/javier-castilla/document/adr-013-toda-entrada-http-se-autentica-con-un-proveedor-con-nombre-7a1ffe638f42)).
Secrets come from the environment (see [Variables](#variables)).

```yaml
authentication:
  operators:
    type: OIDC
    configuration:
      issuer: https://identity.example/realms/spool
      audience: spool-janitor
  webhooks:
    type: HMAC_SIGNATURE
    configuration:
      header: X-Signature-256
      secret: ${env:WEBHOOK_SECRET}

infrastructure:
  eventBus:
    kind: EVENT_BUS
    type: IN_MEMORY
```

| Type | Required keys | Optional keys |
|---|---|---|
| `OIDC` | `issuer`, `audience` | `jwksUri`, `rolesClaim` |
| `HMAC_SIGNATURE` | `header`, `secret` | `timestampHeader`, `tolerance`, `roles` |

---

### Includes and profiles

A descriptor can take part of its keys from other files, and can change some of them for an environment
([ADR-016](https://linear.app/javier-castilla/document/adr-016-un-descriptor-puede-incluir-otros-ficheros-y-elegir-al-b5e8fce73f02)).

```yaml
# shared/infrastructure.yaml
infrastructure:
  eventBus:
    kind: EVENT_BUS
    type: IN_MEMORY
```

```yaml
include:
  - shared/infrastructure.yaml      # relative to this file

profiles:
  production:
    infrastructure:
      eventBus:
        kind: EVENT_BUS
        type: KAFKA
        configuration:
          bootstrap.servers: "kafka:9092"

modules:
  - ingester:
      type: REACTIVE
      id: my-ingester
```

- Included files are read from the same place as the descriptor, classpath or file system, next to the file that
  names them.
- Files are combined in the order of `include`, and the file that includes them goes on top. Objects are combined
  key by key; any other value, lists included, is replaced whole.
- A profile changes only the keys it declares. Choose it with `SpoolNodeDSL.fromDescriptor("/Spool.yaml", "production")`
  or with the `SPOOL_PROFILE` environment variable; the argument wins. With neither, `profiles` is ignored.
- A profile cannot declare `include` or `profiles`. An unknown profile, a missing file or an include cycle stops
  the load, and every error names the file, and the profile, the wrong value comes from.

### Variables

A value can use the descriptor's own variables and the environment, so it is written once and no secret lives in
the file:

```yaml
variables:
  root: /data/spool
  inbox: ${root}/inbox

profiles:
  production:
    variables:
      root: /srv/spool

infrastructure:
  eventBus:
    kind: EVENT_BUS
    type: IN_MEMORY
  inbox:
    kind: INBOX
    type: FILE_SYSTEM
    configuration:
      path: ${inbox}
  dataLake:
    kind: DATA_LAKE
    type: S3
    configuration:
      bucket: ${env:SPOOL_BUCKET}
      region: us-east-1
      endpoint: ${env:S3_ENDPOINT:-http://localhost:4566}
```

- `${name}` uses a variable of the `variables` block, and a variable can use others. `${env:NAME}` reads an
  environment variable, and `${env:NAME:-default}` gives it a default.
- A reference works as a whole value or inside a text (`${root}/inbox`). Only values are replaced, never keys.
  `$${` writes a literal `${`.
- Variables are replaced after the includes and the chosen profile are combined, so a profile can change a variable.
- A variable that does not exist, an environment variable with no value and no default, or variables that use each
  other in a circle stop the load. The error names the path of the value and the reference.
- A value read from the environment never shows up in an error: the error shows `${env:NAME}` instead.
- YAML anchors (`&name`, `*name`) repeat a whole block inside one file.

---

## Infrastructure Options

| Kind | Types | Configuration |
|---|---|---|
| `EVENT_BUS` | `IN_MEMORY` | none |
| | `KAFKA` | `bootstrap.servers`, and `subscriber.id` optional |
| `INBOX` | `FILE_SYSTEM` | `path` |
| | `S3` | `bucket`, `region`, `endpoint` |
| `DATA_LAKE` | `FILE_SYSTEM` | `path` |
| | `S3` | `bucket`, `region`, `endpoint` |
| `DATA_MART` | `FILE_SYSTEM` | `path` |
| | `RAW_FILE_SYSTEM` | `path` |
| `QUARANTINE_REPOSITORY` | `FILE_SYSTEM` | `path` |
| `AGGREGATE_METADATA` | `FILE_SYSTEM` | `path` |
| | `S3` | `bucket`, `region`, `endpoint` |

Every port, its types and their keys are in the [catalogue of adapters](https://github.com/Spool-FRAMEWORK/infrastructure#available-adapters)
of `spool-infrastructure`. There is no relational backend (no PostgreSQL, no JDBC).

A new adapter is a plugin: a class that implements the provider interface of its port (`InboxWriterProvider`,
`EventBusProvider`...), annotated with `@SpoolPlugin`. The name it returns from `name()` is the `type` the descriptor
uses, and an unknown `type` stops the node listing the known ones.

---

## License

Distributed under the [GNU Affero General Public License v3.0](LICENSE).
