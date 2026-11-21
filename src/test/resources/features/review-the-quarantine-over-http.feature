Feature: The quarantine is reviewed and resolved over HTTP

  A janitor declared with a quarantine server answers on the HTTP server of its node: it lists what is in
  quarantine, shows one record with its envelope and its history, and resolves it. Every call is identified by the
  provider the descriptor names, and each operation needs its own role. Every resolution keeps who made it.

  Background:
    Given an inbox and a quarantine in temporary folders of the file system
    And the descriptor declares the provider "people", which gives its callers the roles:
      | quarantine-reader   |
      | quarantine-resolver |
      | quarantine-bypasser |
    And the descriptor declares a janitor with a quarantine server authenticated by "people"
    And these envelopes are in quarantine, waiting to be resolved:
      | envelope | payload        |
      | order-1  | {"amount": -5} |
      | order-2  | {"amount": -7} |
      | order-3  | {"amount": -9} |

  @DEV-77
  Scenario: The records waiting to be resolved are listed page by page
    When "ana" lists the records waiting to be resolved, 2 per page
    Then the page has "order-1" and "order-2"
    And the answer says there is a next page
    When "ana" asks for the next page
    Then the page has "order-3"
    And the answer says there is no next page

  @DEV-77
  Scenario: A record is shown with its envelope and its history
    When "ana" looks at the record "order-1"
    Then the answer shows its payload {"amount": -5}
    And the answer shows one stay in quarantine, still waiting to be resolved

  @DEV-77
  Scenario: A record is republished as it was
    When "ana" republishes "order-1"
    Then "order-1" is captured again in the inbox with the payload {"amount": -5}
    And the history of "order-1" says it was republished by "ana"

  @DEV-77
  Scenario: A record is amended with a new payload
    When "ana" amends "order-1" with the payload {"amount": 5}
    Then a new envelope with the payload {"amount": 5} is captured in the inbox
    And the new envelope says it comes from "order-1"
    And the history of "order-1" says it was corrected by "ana"

  @DEV-77
  Scenario: A record is let through with a reason
    When "ana" lets "order-1" through because "the refund is valid"
    Then the history of "order-1" says it was bypassed by "ana" because "the refund is valid"

  @DEV-77
  Scenario: A record is purged
    When "ana" purges "order-1"
    Then the history of "order-1" says it was purged by "ana"

  @DEV-77
  Scenario: Letting a record through without a reason is rejected
    When "ana" lets "order-1" through without a reason
    Then the answer says the reason is missing
    And "order-1" is still waiting to be resolved

  @DEV-77
  Scenario: Amending a record with an empty payload is rejected
    When "ana" amends "order-1" with an empty payload
    Then the answer says the payload is missing
    And "order-1" is still waiting to be resolved

  @DEV-77
  Scenario: A record that is not in quarantine is not found
    When "ana" republishes "order-9"
    Then the answer says "order-9" is not in quarantine

  @DEV-77
  Scenario: A record already resolved cannot be resolved again
    Given "ana" has purged "order-1"
    When "ana" republishes "order-1"
    Then the answer says "order-1" was already resolved

  @DEV-77
  Scenario: Two people resolving the same record at once
    When "ana" and "luis" resolve "order-1" at the same time
    Then one of them resolves it
    And the other is told "order-1" was already resolved
    And the history of "order-1" has a single resolution

  @DEV-77
  Scenario: A page bigger than the server allows is rejected
    Given the quarantine server lists at most 2 records per page
    When "ana" lists the records waiting to be resolved, 3 per page
    Then the answer says a page has at most 2 records

  @DEV-77
  Scenario: A caller who can only read cannot resolve
    Given the provider "people" only gives its callers the role "quarantine-reader"
    When "ana" lists the records waiting to be resolved, 2 per page
    And "ana" republishes "order-1"
    Then the list is shown
    And the republish is refused for lack of permission
    And "order-1" is still waiting to be resolved

  @DEV-77
  Scenario: A caller who can resolve cannot let a record through
    Given the provider "people" only gives its callers the roles:
      | quarantine-reader   |
      | quarantine-resolver |
    When "ana" lets "order-1" through because "the refund is valid"
    Then the bypass is refused for lack of permission
    And "order-1" is still waiting to be resolved

  @DEV-77
  Scenario: A call that the provider cannot identify is refused
    When someone without a valid signature lists the records waiting to be resolved
    Then the call is refused because the caller is not identified

  @DEV-77
  Scenario: A janitor without a quarantine server has no quarantine API
    Given the descriptor declares a janitor without a quarantine server
    When "ana" lists the records waiting to be resolved, 2 per page
    Then the node does not know that address

  @DEV-77
  Scenario: A quarantine server that names an undeclared provider stops the load
    Given the descriptor declares a janitor with a quarantine server authenticated by "partners"
    When the descriptor is loaded
    Then the load fails
    And the error names "quarantineServer.authentication"
    And the error names "people"
