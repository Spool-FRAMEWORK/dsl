Feature: A crawler receives data through a webhook

  A WEBHOOK crawler answers on the HTTP server of its node at the path its descriptor gives. Every delivery is
  identified by the authentication provider the descriptor names, signed with a shared secret, and the sender needs
  the role the descriptor asks for. What it receives is split into records and stored in the inbox as captured, once
  per record however many times it is sent.

  Background:
    Given an inbox in a temporary folder of the file system
    And the secret of the "boe-sender" provider is in the environment
    And the descriptor declares the provider "boe-sender", which signs the body with a timestamp and gives the role "boe-publisher"
    And the descriptor declares a webhook crawler for the source "boe" at "/spool/webhooks/boe" that splits a JSON array

  @DEV-83
  Scenario: A signed delivery is stored in the inbox
    When "boe-sender" delivers [{"id": "a-1", "title": "Ley 1/2026"}] signed with the secret
    Then the delivery is accepted
    And the inbox has 1 captured envelope of the source "boe"
    And the captured envelope holds {"id": "a-1", "title": "Ley 1/2026"}

  @DEV-83
  Scenario: A delivery with several records stores one envelope per record
    When "boe-sender" delivers [{"id": "a-1"}, {"id": "a-2"}] signed with the secret
    Then the delivery is accepted
    And the inbox has 2 captured envelopes of the source "boe"

  @DEV-83
  Scenario: The same delivery sent twice is stored once
    When "boe-sender" delivers [{"id": "a-1"}] signed with the secret
    And "boe-sender" delivers [{"id": "a-1"}] signed with the secret again
    Then both deliveries are accepted
    And the inbox has 1 captured envelope of the source "boe"

  @DEV-83
  Scenario: A delivery with a wrong signature is refused
    When someone delivers [{"id": "a-1"}] signed with another secret
    Then the delivery is refused because the sender is not identified
    And the inbox is empty

  @DEV-83
  Scenario: A delivery without a signature is refused
    When someone delivers [{"id": "a-1"}] without a signature
    Then the delivery is refused because the sender is not identified
    And the inbox is empty

  @DEV-83
  Scenario: A delivery signed too long ago is refused
    When "boe-sender" delivers [{"id": "a-1"}] signed with the secret 10 minutes ago
    Then the delivery is refused because the sender is not identified
    And the inbox is empty

  @DEV-83
  Scenario: A sender without the role of the webhook is refused
    Given the provider "boe-sender" gives the role "other-publisher" instead
    When "boe-sender" delivers [{"id": "a-1"}] signed with the secret
    Then the delivery is refused for lack of permission
    And the inbox is empty

  @DEV-83
  Scenario: A delivery bigger than the webhook accepts is refused
    Given the webhook accepts deliveries of at most 64 bytes
    When "boe-sender" delivers a JSON array of 200 bytes signed with the secret
    Then the delivery is refused because it is too large
    And the inbox is empty

  @DEV-83
  Scenario: A delivery of another media type is refused
    When "boe-sender" delivers a CSV body signed with the secret
    Then the delivery is refused because its media type is not the one of the source
    And the inbox is empty

  @DEV-83
  Scenario: A webhook without an authentication provider stops the load
    Given the webhook crawler names no authentication provider
    When the descriptor is loaded
    Then the load fails
    And the error names "webhook.authentication"

  @DEV-83
  Scenario: A webhook that names an undeclared provider stops the load
    Given the webhook crawler is authenticated by "partners"
    When the descriptor is loaded
    Then the load fails
    And the error names "webhook.authentication"
    And the error names "boe-sender"

  @DEV-83
  Scenario: A webhook without a role stops the load
    Given the webhook crawler asks for no role
    When the descriptor is loaded
    Then the load fails
    And the error names "webhook.role"

  @DEV-83
  Scenario: A webhook without a path stops the load
    Given the webhook crawler has no path
    When the descriptor is loaded
    Then the load fails
    And the error names "source.configuration.path"
