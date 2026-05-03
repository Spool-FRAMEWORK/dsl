<p align="center">
  <img src="doc/spool_logo.svg" alt="Spool Logo" width="180"/>
</p>

<h1 align="center">spool-dsl</h1>

<p align="center">
  Declarative configuration &amp; bootstrapping layer for the Spool framework.
</p>

---

Describe your entire pipeline in a single `Spool.yaml` and let `SpoolNodeDSL` wire everything up — infrastructure, crawlers, janitors and ingesters — with zero boilerplate.

## How it works

```
Spool.yaml
│
▼
SpoolNodeDSL.fromDescriptor(path)
│
├── InfrastructureDescriptor  →  eventBus · inbox · dataLake · watchdog
│
└── SpoolModuleDescriptor[]
├── CrawlerDescriptor   →  CrawlerBuilder
├── JanitorDescriptor   →  JanitorBuilder
└── IngesterDescriptor  →  IngesterBuilder
```

Each builder resolves its infrastructure ports through `PluginRegistry` using the SPI system, so no implementation detail leaks into your descriptor.

---

## Quickstart

### 1. Dependency

```xml
❮dependency❯
    ❮groupId❯software.spool❮/groupId❯
    ❮artifactId❯spool-dsl❮/artifactId❯
    ❮version❯YOUR_VERSION❮/version❯
❮/dependency❯
```

### 2. `src/main/resources/Spool.yaml`

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
      rootPath: "results"
      enrichment:
      - source: "entityName"
      poll:
          http:
          url: "https://api.example.com/data"
  schedule:
    everyMilliseconds: 60000
  eventMapping:
      namingConvention: SNAKE_CASE
      attributeList:
      - value: "entityName"

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

### 3. Boot

```java
public class Main {
    public static void main(String[] args) throws IOException {
        SpoolNodeDSL.fromDescriptor("/Spool.yaml");
    }
}
```

---

## Modules

### 🕷 Crawler
Polls a source on a schedule and publishes raw events to both the event bus and the inbox.

| Field | Description |
|---|---|
| `source.id` | Logical identifier for the source |
| `source.format` | Normalizer format (`JSON_ARRAY`, …) |
| `source.rootPath` | Dot-notation path to the records array |
| `source.enrichment` | Fields injected into every emitted event |
| `source.poll.schedule.everyMilliseconds` | Polling interval in ms |
| `eventMapping.namingConvention` | Output naming convention (`SNAKE_CASE`, …) |
| `eventMapping.attributeList` | Fields used as partition attributes |

Poll source types: `http` · `file` · `database` · `custom`

### 🧹 Janitor
Polling worker that promotes staged envelopes from the inbox onto the event bus and removes processed ones.

| Field | Description |
|---|---|
| `workers` | Number of concurrent polling workers |
| `everyMilliseconds` | Polling interval in ms |

### 📥 Ingester
Consumes events from the bus and writes them to the data lake.

| Mode | How to activate | Behaviour |
|---|---|---|
| **Buffered** | Set the `flush` block | Writes in batches — by size threshold or time window |
| **Reactive** | Omit `flush` | Writes each event immediately on arrival |

---

## Infrastructure options

Every infrastructure component supports a `custom` variant backed by the `PluginRegistry` SPI. Implement the corresponding provider interface and register it via `ServiceLoader`.

| Component | Built-in types |
|---|---|
| `eventBus` | `kafka` · `inMemory` |
| `inbox` | `s3` · `sql` · `inMemory` · `custom` |
| `dataLake` | `s3` · `sql` · `inMemory` · `custom` |

Custom plugin example:

```yaml
inbox:
    custom:
    pluginName: "my-inbox-plugin"
    configuration:
        host: "localhost"
        port: "5432"
```