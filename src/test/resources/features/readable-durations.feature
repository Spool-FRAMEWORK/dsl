Feature: Times in the descriptor are readable durations

  Every time in the descriptor is a duration, written in ISO 8601 or in the short form, and read into a
  java.time.Duration by a single reader of the DSL. Keys no longer say "milliseconds" (ADR-011).

  @DEV-79
  Scenario Outline: A time is read from ISO 8601 and from the short form
    Given the descriptor:
      """
      infrastructure:
        eventBus: {type: IN_MEMORY}
        inbox: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/inbox}}
        dataLake: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/lake}}
        quarantineRepository: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/quarantine}}
      modules:
        - janitor:
            id: janitor
            configuration: {quarantineRetention: <written>}
      """
    When the descriptor is loaded
    Then the quarantine retention of the janitor is <amount> <unit>

    Examples:
      | written | amount | unit         |
      | P30D    | 30     | days         |
      | PT5S    | 5      | seconds      |
      | 30d     | 30     | days         |
      | 2h      | 2      | hours        |
      | 10m     | 10     | minutes      |
      | 5s      | 5      | seconds      |
      | 100ms   | 100    | milliseconds |

  @DEV-79
  Scenario: Every time of the janitor takes a duration
    Given the descriptor:
      """
      infrastructure:
        eventBus: {type: IN_MEMORY}
        inbox: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/inbox}}
        dataLake: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/lake}}
        quarantineRepository: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/quarantine}}
      modules:
        - janitor:
            id: janitor
            schedule: {every: 1m}
            configuration: {threshold: 2m, ttl: 3m, quarantineTtl: 4m, quarantineRetention: 5m}
      """
    When the descriptor is loaded
    Then the janitor runs a round every 1 minute
    And the janitor republishes after 2 minutes, expires after 3 minutes and leaves the quarantine after 4 minutes

  @DEV-79
  Scenario: The ingester flushes every duration
    Given the descriptor:
      """
      infrastructure:
        eventBus: {type: IN_MEMORY}
        inbox: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/inbox}}
        dataLake: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/lake}}
        quarantineRepository: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/quarantine}}
      modules:
        - ingester:
            id: ingester
            type: FLUSH
            configuration: {size: 50, every: 2m}
      """
    When the descriptor is loaded
    Then the ingester flushes every 2 minutes or every 50 envelopes

  @DEV-79
  Scenario: The crawler polls every duration
    Given the descriptor:
      """
      infrastructure:
        eventBus: {type: IN_MEMORY}
        inbox: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/inbox}}
        dataLake: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/lake}}
        quarantineRepository: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/quarantine}}
      modules:
        - crawler:
            type: POLL
            id: crawler
            schedule: {every: 2m}
            source: {type: HTTP, mediaType: JSON_OBJECT, configuration: {sourceId: durations, url: "http://localhost/event"}}
            eventMapping: {namingConvention: SNAKE_CASE}
      """
    When the descriptor is loaded
    Then the crawler polls every 2 minutes

  @DEV-79
  Scenario: A key that still says milliseconds names the new one
    Given the descriptor:
      """
      infrastructure:
        eventBus: {type: IN_MEMORY}
        inbox: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/inbox}}
        dataLake: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/lake}}
        quarantineRepository: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/quarantine}}
      modules:
        - janitor:
            id: janitor
            configuration: {millisecondsTTL: 100000}
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "modules[0].janitor.configuration.millisecondsTTL is no longer supported: use ttl with a duration such as 100s"

  @DEV-79
  Scenario Outline: A text that is not a duration fails with the path of the value
    Given the descriptor:
      """
      infrastructure:
        eventBus: {type: IN_MEMORY}
        inbox: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/inbox}}
        dataLake: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/lake}}
        quarantineRepository: {type: FILE_SYSTEM, configuration: {path: target/spool-test/durations/quarantine}}
      modules:
        - janitor:
            id: janitor
            configuration: {quarantineRetention: "<written>"}
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "modules[0].janitor.configuration.quarantineRetention must be a duration such as 30d or P30D, got '<written>'"

    Examples:
      | written    |
      | 30 days    |
      | 2592000000 |
      | 5x         |
