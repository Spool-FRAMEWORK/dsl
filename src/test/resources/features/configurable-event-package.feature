Feature: The descriptor says in which package the event classes of a crawler are

  A crawler names its domain events by their simple name in eventMapping.domainMappingList. The DSL looks each one
  up in the package of eventMapping.eventPackage, or in "events" when the key is missing, as it always did.

  @DEV-20
  Scenario: Without a package, the event classes are looked up in "events"
    Given the descriptor:
      """
      infrastructure:
        eventBus: {type: IN_MEMORY}
        inbox: {type: FILE_SYSTEM, configuration: {path: target/spool-test/event-package/inbox}}
      modules:
        - crawler:
            type: POLL
            id: crawler
            source: {type: HTTP, normalizer: JSON_OBJECT, mediaType: JSON, configuration: {sourceId: orders, url: "http://localhost/event"}}
            eventMapping:
              namingConvention: SNAKE_CASE
              domainMappingList: [StreamedOrder]
      """
    When the descriptor is loaded
    Then the crawler publishes the event class "events.StreamedOrder"

  @DEV-20
  Scenario: With a package, the event classes are looked up in it
    Given the descriptor:
      """
      infrastructure:
        eventBus: {type: IN_MEMORY}
        inbox: {type: FILE_SYSTEM, configuration: {path: target/spool-test/event-package/inbox}}
      modules:
        - crawler:
            type: POLL
            id: crawler
            source: {type: HTTP, normalizer: JSON_OBJECT, mediaType: JSON, configuration: {sourceId: orders, url: "http://localhost/event"}}
            eventMapping:
              namingConvention: SNAKE_CASE
              eventPackage: software.spool.dsl.acceptance.orders
              domainMappingList: [OrderShipped]
      """
    When the descriptor is loaded
    Then the crawler publishes the event class "software.spool.dsl.acceptance.orders.OrderShipped"

  @DEV-20
  Scenario: An event class that is not in the package names the package it was looked up in
    Given the descriptor:
      """
      infrastructure:
        eventBus: {type: IN_MEMORY}
        inbox: {type: FILE_SYSTEM, configuration: {path: target/spool-test/event-package/inbox}}
      modules:
        - crawler:
            type: POLL
            id: crawler
            source: {type: HTTP, normalizer: JSON_OBJECT, mediaType: JSON, configuration: {sourceId: orders, url: "http://localhost/event"}}
            eventMapping:
              namingConvention: SNAKE_CASE
              eventPackage: software.spool.dsl.acceptance.orders
              domainMappingList: [StreamedOrder]
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "modules[0].crawler.eventMapping.domainMappingList names a class that is not in package 'software.spool.dsl.acceptance.orders': 'StreamedOrder'"

  @DEV-20
  Scenario: A class that is not an event fails with its name
    Given the descriptor:
      """
      infrastructure:
        eventBus: {type: IN_MEMORY}
        inbox: {type: FILE_SYSTEM, configuration: {path: target/spool-test/event-package/inbox}}
      modules:
        - crawler:
            type: POLL
            id: crawler
            source: {type: HTTP, normalizer: JSON_OBJECT, mediaType: JSON, configuration: {sourceId: orders, url: "http://localhost/event"}}
            eventMapping:
              namingConvention: SNAKE_CASE
              eventPackage: software.spool.dsl.acceptance.orders
              domainMappingList: [OrderNotes]
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "modules[0].crawler.eventMapping.domainMappingList names a class that is not an event: 'software.spool.dsl.acceptance.orders.OrderNotes'"
