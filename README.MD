# Spool — DSL

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
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
  - [Quarantine](#quarantine)
  - [Ingester](#ingester)
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
├── InfrastructureDescriptor  →  eventBus · inbox · dataLake · quarantineRepository · watchdog
│
└── SpoolModuleDescriptor[]
    ├── CrawlerDescriptor   →  CrawlerBuilderFactory  →  Crawler
    ├── JanitorDescriptor   →  JanitorBuilderFactory  →  Janitor
    └── IngesterDescriptor  →  IngesterBuilderFactory →  Ingester
```

Each component names its adapter with `type`, and the adapter is found among the plugins registered through the Java
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
infrastructure:
  watchdog: "http://localhost:8090"

  eventBus:
    type: KAFKA
    configuration:
      bootstrap.servers: "localhost:9092"

  inbox:
    type: S3
    configuration:
      bucket: spool-inbox
      region: us-east-1
      endpoint: http://localhost:4566

  dataLake:
    type: S3
    configuration:
      bucket: spool-datalake
      region: us-east-1
      endpoint: http://localhost:4566

  quarantineRepository:
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
        mediaType: JSON_ARRAY
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

---

## Spool.yaml Reference

### Infrastructure

Every component has a `type`, the name of its plugin, and a `configuration` with the keys that plugin reads. The
types and their keys are in [Infrastructure Options](#infrastructure-options).

| Key | Description |
|---|---|
| `watchdog` | URL of the watchdog the modules send their heartbeat to. Optional. |
| `eventBus` | The bus the modules publish and consume events on. |
| `inbox` | Where the crawler stores what it captures and the janitor looks after it. |
| `dataLake` | Where the ingester writes. The janitor also writes back to it what was quarantined from bronze. |
| `quarantineRepository` | Where the janitor keeps what is in quarantine (see [Quarantine](#quarantine)). |

A module only needs the components it uses: a node with only an ingester needs no inbox. A missing component that a
module needs stops the node with the name of the module.

---

### Crawler

Reads a source and stores each record in the inbox. A `POLL` crawler asks the source on its schedule; a `STREAM`
crawler listens to it.

```yaml
- crawler:
    type: POLL
    id: my-crawler
    schedule:
      every: 1m                 # without a schedule, a POLL crawler polls once
    source:
      type: HTTP
      configuration:
        sourceId: my-source
        url: "https://api.example.com/data"
      mediaType: JSON_ARRAY     # how the response is split into records
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
| `type` | `POLL` or `STREAM`. |
| `source.type` | The source plugin: `HTTP` for `POLL`, `IN_MEMORY` for `STREAM`. |
| `source.mediaType` | `JSON`, `JSON_ARRAY`, `JSON_OBJECT`, `EVENT_CLASS`, `EVENT_CLASS_ARRAY` or `PDF`. |
| `eventMapping.namingConvention` | How field names are written in the events. |
| `eventMapping.domainMappingList` | The domain events the crawler publishes, by the simple name of their class. Optional. |
| `eventMapping.eventPackage` | The package of those classes. Optional; `events` when missing. |
| `errorRouter` | Where the crawler reports what fails. Optional; `DEFAULT` when missing. |

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
| `schedule.every` | How often the janitor runs a round. |
| `configuration.threshold` | How long an envelope can stay without moving before it is republished. |
| `configuration.ttl` | How long an envelope stays in the inbox before it expires. |
| `configuration.maxInboxUpdateAttempts` | Rounds an event is tried when the inbox fails to apply it. 3 by default. |

### Durations

Every time in the descriptor is a duration, in ISO 8601 (`PT5S`, `P30D`) or in the short form: `100ms`, `5s`, `10m`,
`2h`, `30d`. A bare number is not accepted, because it would not say its unit. The keys that held milliseconds
(`millisecondsTTL`, `scheduleMilliseconds`...) are no longer read: loading fails and names the key that replaces
them ([ADR-011](https://linear.app/javier-castilla/document/adr-011-el-descriptor-admite-variables-propias-y-de-entorno-y-b7e8390d83c6)).

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

A record the mounter rejected stays in bronze no longer: the mounter removes it from its bronze partition after keeping the record. Resolving it writes the envelope back to that partition (or the corrected one, under its new key) and publishes `EnvelopePersisted` so it is mounted again; the inbox is not touched. The janitor writes to the `infrastructure.dataLake`; without one, those records are skipped.

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
    type: IN_MEMORY
```

```yaml
include:
  - shared/infrastructure.yaml      # relative to this file

profiles:
  production:
    infrastructure:
      eventBus:
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
    type: IN_MEMORY
  inbox:
    type: FILE_SYSTEM
    configuration:
      path: ${inbox}
  dataLake:
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

| Component | Types | Configuration |
|---|---|---|
| `eventBus` | `IN_MEMORY` | none |
| | `KAFKA` | `bootstrap.servers`, and `subscriber.id` optional |
| `inbox` | `FILE_SYSTEM` | `path` |
| | `S3` | `bucket`, `region`, `endpoint` |
| `dataLake` | `FILE_SYSTEM` | `path` |
| | `S3` | `bucket`, `region`, `endpoint` |
| `quarantineRepository` | `FILE_SYSTEM` | `path` |

Every port, its types and their keys are in the [catalogue of adapters](https://github.com/Spool-FRAMEWORK/infrastructure#available-adapters)
of `spool-infrastructure`. There is no relational backend (no PostgreSQL, no JDBC).

A new adapter is a plugin: a class that implements the provider interface of its port (`InboxWriterProvider`,
`EventBusProvider`...), annotated with `@SpoolPlugin`. The name it returns from `name()` is the `type` the descriptor
uses, and an unknown `type` stops the node listing the known ones.

---

## License

Distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
