Feature: The infrastructure is declared as named pieces, and each module names the ones it uses

  infrastructure is a map of pieces. The key of each piece is its name; the piece says its kind, its plugin and its
  configuration. A module names in uses the pieces it uses; when it names none of a kind it needs, it uses the only
  piece of that kind. The watchdog is not a piece: it is declared at the root (ADR-018).

  @DEV-86
  Scenario: Two mounters each write to their own data mart
    Given a data lake and a data mart in temporary folders of the file system
    And the descriptor has no quarantine
    And silver has these "payments" records in the data mart "payments" and the partition "source=payments":
      | id        | amount |
      | payment-1 | 10     |
      | payment-2 | 25     |
    And the descriptor has the data marts "gold" and "reports" in temporary folders of the file system
    And the descriptor declares the mounter "gold-total" that uses "gold":
      """
      type: POLL
      schedule: {every: 1m}
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.AmountTotal
      read: {partitions: ["source=payments"]}
      write: {dataMart: payments-total, partitioning: {type: SAME_AS_SOURCE}}
      """
    And the descriptor declares the mounter "reports-total" that uses "reports":
      """
      type: POLL
      schedule: {every: 1m}
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.AmountTotal
      read: {partitions: ["source=payments"]}
      write: {dataMart: payments-report, partitioning: {type: SAME_AS_SOURCE}}
      """
    When the node is started from the descriptor
    And the mounter "gold-total" mounts once
    And the mounter "reports-total" mounts once
    Then the data mart "gold" has a single record with the total amount 35 in "payments-total"
    And the data mart "reports" has a single record with the total amount 35 in "payments-report"
    And the data mart "gold" has nothing in "payments-report"

  @DEV-86
  Scenario: With one piece of each kind, a module uses them without naming them
    Given the descriptor:
      """
      watchdog: "http://localhost:8090"
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: target/spool-test/pieces/inbox}}
      modules:
        - crawler:
            type: POLL
            id: crawler
            source: {type: HTTP, normalizer: JSON_OBJECT, mediaType: JSON, configuration: {sourceId: orders, url: "http://localhost/event"}}
            eventMapping: {namingConvention: SNAKE_CASE}
      """
    When the descriptor is loaded
    Then the module "crawler" uses the piece "inbox" as its inbox
    And the node sends its heartbeats to "http://localhost:8090"

  @DEV-86
  Scenario: A module that needs a kind with several pieces has to name one
    Given the descriptor:
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
        lake: {kind: DATA_LAKE, type: FILE_SYSTEM, configuration: {path: target/spool-test/pieces/lake}}
        gold: {kind: DATA_MART, type: FILE_SYSTEM, configuration: {path: target/spool-test/pieces/gold}}
        reports: {kind: DATA_MART, type: FILE_SYSTEM, configuration: {path: target/spool-test/pieces/reports}}
      modules:
        - mounter:
            id: total
            type: POLL
            schedule: {every: 1m}
            mode: AGGREGATION
            aggregator: software.spool.dsl.acceptance.mounter.AmountTotal
            read: {partitions: ["source=payments"]}
            write: {dataMart: payments-total, partitioning: {type: SAME_AS_SOURCE}}
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "modules[0].mounter needs a DATA_MART: name one in uses. Declared: gold, reports"

  @DEV-86
  Scenario: A name in uses that is not declared fails with the name
    Given the descriptor:
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: target/spool-test/pieces/inbox}}
      modules:
        - crawler:
            type: POLL
            id: crawler
            uses: [orders-inbox]
            source: {type: HTTP, normalizer: JSON_OBJECT, mediaType: JSON, configuration: {sourceId: orders, url: "http://localhost/event"}}
            eventMapping: {namingConvention: SNAKE_CASE}
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "modules[0].crawler.uses names 'orders-inbox', which is not declared in infrastructure. Declared: bus, inbox"

  @DEV-86
  Scenario: Two pieces of the same kind in one uses fail with both names
    Given the descriptor:
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
        lake: {kind: DATA_LAKE, type: FILE_SYSTEM, configuration: {path: target/spool-test/pieces/lake}}
        gold: {kind: DATA_MART, type: FILE_SYSTEM, configuration: {path: target/spool-test/pieces/gold}}
        reports: {kind: DATA_MART, type: FILE_SYSTEM, configuration: {path: target/spool-test/pieces/reports}}
      modules:
        - mounter:
            id: total
            uses: [gold, reports]
            type: POLL
            schedule: {every: 1m}
            mode: AGGREGATION
            aggregator: software.spool.dsl.acceptance.mounter.AmountTotal
            read: {partitions: ["source=payments"]}
            write: {dataMart: payments-total, partitioning: {type: SAME_AS_SOURCE}}
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "modules[0].mounter.uses names two DATA_MART pieces: gold, reports"

  @DEV-86
  Scenario: A piece of a kind the module does not use fails with its name
    Given the descriptor:
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: target/spool-test/pieces/inbox}}
        gold: {kind: DATA_MART, type: FILE_SYSTEM, configuration: {path: target/spool-test/pieces/gold}}
      modules:
        - crawler:
            type: POLL
            id: crawler
            uses: [gold]
            source: {type: HTTP, normalizer: JSON_OBJECT, mediaType: JSON, configuration: {sourceId: orders, url: "http://localhost/event"}}
            eventMapping: {namingConvention: SNAKE_CASE}
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "modules[0].crawler.uses names 'gold', a DATA_MART the module does not use"

  @DEV-86
  Scenario: A profile changes a piece by its name
    Given the descriptor:
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: target/spool-test/pieces/inbox}}
      modules: []
      profiles:
        local:
          infrastructure:
            inbox:
              configuration: {path: target/spool-test/pieces/local-inbox}
      """
    When the descriptor is loaded with the profile "local"
    Then the piece "inbox" has the path "target/spool-test/pieces/local-inbox"

  @DEV-86
  Scenario Outline: A piece written in the previous format says which kind to add
    Given the descriptor:
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
        <key>: {type: FILE_SYSTEM, configuration: {path: target/spool-test/pieces/old}}
      modules: []
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "infrastructure.<key> needs a kind: add kind: <kind>"

    Examples:
      | key                  | kind                  |
      | inbox                | INBOX                 |
      | dataLake             | DATA_LAKE             |
      | dataMart             | DATA_MART             |
      | quarantineRepository | QUARANTINE_REPOSITORY |

  @DEV-86
  Scenario: A piece without a kind and with a name of its own lists the kinds
    Given the descriptor:
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
        orders: {type: FILE_SYSTEM, configuration: {path: target/spool-test/pieces/orders}}
      modules: []
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "infrastructure.orders.kind is required: one of EVENT_BUS, INBOX, DATA_LAKE, DATA_MART, QUARANTINE_REPOSITORY"

  @DEV-86
  Scenario: The watchdog inside infrastructure says to move it to the root
    Given the descriptor:
      """
      infrastructure:
        watchdog: "http://localhost:8090"
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
      modules: []
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "infrastructure.watchdog is no longer supported: write watchdog: \"http://localhost:8090\" at the root of the descriptor"
