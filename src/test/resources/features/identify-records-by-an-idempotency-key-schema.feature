Feature: A source says which fields identify its records

  A crawler stores a record once however many times it captures it, by its idempotency key. Without a schema, the key
  comes from the whole record. With source.idempotencyKeySchema, it comes only from the fields the schema names, so a
  field that changes on every call does not make the same record look new. A record without one of those fields is
  not stored, unless the schema says to key it by the whole record.

  Background:
    Given an inbox in a temporary folder of the file system
    And the secret of the "boe-sender" provider is in the environment
    And the descriptor declares the provider "boe-sender", which signs the body with a timestamp and gives the role "boe-publisher"
    And the descriptor declares a webhook crawler for the source "boe" at "/spool/webhooks/boe" that splits a JSON array

  @DEV-25
  Scenario: Without a schema, two records that differ in any field are stored apart
    When "boe-sender" delivers [{"id": "a-1", "fetchedAt": "10:01"}] signed with the secret
    And "boe-sender" delivers [{"id": "a-1", "fetchedAt": "10:02"}] signed with the secret again
    Then both deliveries are accepted
    And the inbox has 2 captured envelopes of the source "boe"

  @DEV-25
  Scenario: Two records that differ only in a field outside the schema are stored once
    Given the webhook crawler identifies its records by "id"
    When "boe-sender" delivers [{"id": "a-1", "fetchedAt": "10:01"}] signed with the secret
    And "boe-sender" delivers [{"id": "a-1", "fetchedAt": "10:02"}] signed with the secret again
    Then both deliveries are accepted
    And the inbox has 1 captured envelope of the source "boe"

  @DEV-25
  Scenario: Two records that differ in a field of the schema are stored apart
    Given the webhook crawler identifies its records by "id, version"
    When "boe-sender" delivers [{"id": "a-1", "version": 1}] signed with the secret
    And "boe-sender" delivers [{"id": "a-1", "version": 2}] signed with the secret again
    Then both deliveries are accepted
    And the inbox has 2 captured envelopes of the source "boe"

  @DEV-25
  Scenario: A field inside an object identifies the records
    Given the webhook crawler identifies its records by "meta.id"
    When "boe-sender" delivers [{"meta": {"id": "a-1"}, "fetchedAt": "10:01"}] signed with the secret
    And "boe-sender" delivers [{"meta": {"id": "a-1"}, "fetchedAt": "10:02"}] signed with the secret again
    Then both deliveries are accepted
    And the inbox has 1 captured envelope of the source "boe"

  @DEV-25
  Scenario: A record without a field of the schema is not stored
    Given the webhook crawler identifies its records by "id"
    When "boe-sender" delivers [{"title": "Ley 1/2026"}, {"id": "a-2"}] signed with the secret
    Then the inbox has 1 captured envelope of the source "boe"
    And the captured envelope holds {"id": "a-2"}

  @DEV-25
  Scenario: A record without a field of the schema is stored by the whole record when the schema says so
    Given the webhook crawler identifies its records by "id"
    And the webhook crawler keys a record without those fields by the whole record
    When "boe-sender" delivers [{"title": "Ley 1/2026", "fetchedAt": "10:01"}] signed with the secret
    And "boe-sender" delivers [{"title": "Ley 1/2026", "fetchedAt": "10:02"}] signed with the secret again
    Then both deliveries are accepted
    And the inbox has 2 captured envelopes of the source "boe"

  @DEV-25
  Scenario: A schema without fields stops the load
    Given the webhook crawler identifies its records by no field
    When the descriptor is loaded
    Then the load fails
    And the error names "source.idempotencyKeySchema.fields"
