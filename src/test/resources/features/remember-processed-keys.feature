Feature: The ingester and the janitor remember their processed keys for a window set in the descriptor

  How long a consumer remembers the keys it processed is set per module with processedKeysWindow. Without it,
  the module keeps its default window (ADR-007).

  @DEV-64
  Scenario: The window of the ingester and of the janitor is read from the descriptor
    Given the descriptor:
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: target/spool-test/processed-keys/inbox}}
        lake: {kind: DATA_LAKE, type: FILE_SYSTEM, configuration: {path: target/spool-test/processed-keys/lake}}
        quarantine: {kind: QUARANTINE_REPOSITORY, type: FILE_SYSTEM, configuration: {path: target/spool-test/processed-keys/quarantine}}
      modules:
        - ingester:
            type: FLUSH
            id: orders-ingester
            configuration: {size: 100, processedKeysWindow: 1h}
        - janitor:
            id: orders-janitor
            configuration: {processedKeysWindow: 2d}
      """
    When the descriptor is loaded
    Then the processed keys window of the ingester is 1 hours
    And the processed keys window of the janitor is 2 days

  @DEV-64
  Scenario: A window that is not a duration stops the load
    Given the descriptor:
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: target/spool-test/processed-keys/inbox}}
        lake: {kind: DATA_LAKE, type: FILE_SYSTEM, configuration: {path: target/spool-test/processed-keys/lake}}
        quarantine: {kind: QUARANTINE_REPOSITORY, type: FILE_SYSTEM, configuration: {path: target/spool-test/processed-keys/quarantine}}
      modules:
        - ingester:
            type: FLUSH
            id: orders-ingester
            configuration: {size: 100, processedKeysWindow: forever}
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "modules[0].ingester.configuration.processedKeysWindow"
