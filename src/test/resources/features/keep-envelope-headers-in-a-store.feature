Feature: The descriptor gives the data lake a store of envelope headers

  An infrastructure with an ENVELOPE_HEADERS piece keeps the header of each envelope of the data lake in that store,
  to find envelopes without reading the data lake (ADR-023). Without that piece, the data lake works as before.

  Background:
    Given an inbox, a data lake and a quarantine in temporary folders of the file system
    And a store of envelope headers that the test can look into

  @DEV-112
  Scenario: The envelopes the ingester writes to the data lake have their header in the store
    Given the node declares the store of envelope headers and an ingester
    And the inbox has the captured envelopes "order-1" and "order-2" of the source "orders"
    When the ingester writes them to the data lake
    Then the data lake has the envelopes "order-1" and "order-2"
    And the store has the header of "order-1" in the partition of the source "orders"
    And the store has the header of "order-2" in the partition of the source "orders"
    And each header is the one the data lake keeps for its envelope

  @DEV-112
  Scenario: Without a store of envelope headers the ingester writes to the data lake as before
    Given the node declares an ingester and no store of envelope headers
    And the inbox has the captured envelopes "order-1" and "order-2" of the source "orders"
    When the ingester writes them to the data lake
    Then the data lake has the envelopes "order-1" and "order-2"
    And the store has no headers

  @DEV-112
  Scenario: A record that a mounter to silver quarantines leaves the store with its envelope
    Given the node declares the store of envelope headers and a mounter to silver that validates its records
    And the data lake has the valid envelope "order-1" and the invalid envelope "order-2" of the source "orders", with their headers in the store
    When the mounter mounts once
    Then the data lake no longer has the envelope "order-2"
    And the store no longer has the header of "order-2"
    And the store still has the header of "order-1"

  @DEV-112
  Scenario: An envelope that the janitor writes back to the data lake has its header in the store
    Given the node declares the store of envelope headers and a janitor
    And the quarantine has the envelope "order-1" of the source "orders", taken out of the data lake
    When the record "order-1" is republished from the quarantine
    Then the data lake has the envelope "order-1"
    And the store has the header of "order-1" in the partition of the source "orders"

  @DEV-112
  Scenario: A store of envelope headers of an unknown type stops the load
    Given the node declares a store of envelope headers of type "CASSANDRA"
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the path "infrastructure.headers.type"
