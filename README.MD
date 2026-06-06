# Spool — DSL

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/build-Maven-C71A36.svg)](https://maven.apache.org/)

**DSL** is the **Spool** framework module that lets you **declare your entire pipeline in a single `Spool.yaml` file** and boot it with one line of Java. No wiring code, no boilerplate.

---

## Table of Contents

- [What it does](#what-it-does)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Spool.yaml Reference](#spoolyaml-reference)
  - [Infrastructure](#infrastructure)
  - [Crawler](#crawler)
  - [Janitor](#janitor)
  - [Ingester](#ingester)
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
├── InfrastructureDescriptor  →  eventBus · inbox · dataLake · watchdog
│
└── SpoolModuleDescriptor[]
    ├── CrawlerDescriptor   →  CrawlerBuilderFactory  →  Crawler
    ├── JanitorDescriptor   →  JanitorBuilderFactory  →  Janitor
    └── IngesterDescriptor  →  IngesterBuilderFactory →  Ingester
```

Each builder resolves its infrastructure adapters through `PluginRegistry` using the Java SPI system (`spool-infrastructure`). No implementation detail leaks into your descriptor.

---

## Installation

```xml
<dependency>
    <groupId>io.github.spool-framework</groupId>
    <artifactId>dsl</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

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
  watchdog:
    url: "http://localhost:8090"

  eventBus:
    type: "kafka"
    url: "localhost:9092"

  inbox:
    s3:
      bucket: spool-inbox
      region: us-east-1
      endpoint: http://localhost:4566

  dataLake:
    s3:
      bucket: spool-datalake
      region: us-east-1
      endpoint: http://localhost:4566

modules:

  - crawler:
      id: "my-crawler"
      source:
        id: "my-source"
        format: JSON_ARRAY
        poll:
          http:
            url: "https://api.example.com/data"
      schedule:
        everyMilliseconds: 60000

  - janitor:
      id: "my-janitor"
      workers: 2
      everyMilliseconds: 5000

  - ingester:
      id: "my-ingester"
      flush:
        size: 100
        every: 10
        unit: "SECONDS"
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

Or through `SpoolRuntime` to also configure OpenTelemetry:

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

| Key | Description |
|---|---|
| `watchdog.url` | Watchdog HTTP endpoint for heartbeat reporting. |
| `eventBus.type` | Event bus implementation (`kafka`, `inMemory`). |
| `inbox.*` | Inbox storage configuration (see [Infrastructure Options](#infrastructure-options)). |
| `dataLake.*` | Data lake storage configuration. |

---

### Crawler

Polls a source on a schedule and stores each record in the inbox.

```yaml
- crawler:
    id: "my-crawler"
    source:
      id: "my-source"
      format: JSON_ARRAY        # JSON_ARRAY, JSON_OBJECT, YAML_ARRAY, RESULT_SET
      rootPath: "results"       # dot-notation path to the records array (optional)
      enrichment:
        - source: "entityName"  # field injected into every emitted envelope
      poll:
        http:
          url: "https://api.example.com/data"
    schedule:
      everyMilliseconds: 60000
    eventMapping:
      namingConvention: SNAKE_CASE
      attributeList:
        - value: "entityName"
```

Poll source types: `http` · `file` · `database` · `custom`

---

### Janitor

Runs on a schedule to keep the inbox healthy — republishes stuck envelopes, quarantines failed ones, removes expired ones.

```yaml
- janitor:
    id: "my-janitor"
    workers: 2
    everyMilliseconds: 5000
```

| Field | Description |
|---|---|
| `workers` | Number of concurrent polling workers. |
| `everyMilliseconds` | Polling interval in ms. |

---

### Ingester

Consumes events from the bus and writes envelopes to the data lake.

```yaml
- ingester:
    id: "my-ingester"
    flush:
      size: 100
      every: 10
      unit: "SECONDS"
```

| Mode | How to activate | Behaviour |
|---|---|---|
| **Buffered** | Set the `flush` block | Writes in batches — by size threshold or time window. |
| **Reactive** | Omit `flush` | Writes each envelope immediately on arrival. |

---

## Infrastructure Options

| Component | Built-in types |
|---|---|
| `eventBus` | `kafka` · `inMemory` |
| `inbox` | `s3` · `filesystem` · `inMemory` · `custom` |
| `dataLake` | `s3` · `filesystem` · `inMemory` · `custom` |

Every component supports a `custom` type backed by the `PluginRegistry` SPI (see `spool-infrastructure`):

```yaml
inbox:
  custom:
    pluginName: "my-inbox"
    configuration:
      host: "localhost"
      port: "5432"
```

---

## License

Distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
