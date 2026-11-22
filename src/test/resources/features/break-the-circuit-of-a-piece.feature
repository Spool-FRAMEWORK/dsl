Feature: A piece or a source that keeps failing, or asks to wait, stops being called for a while

  Each piece of infrastructure and each source of the descriptor has a circuit of its own. When the failures of a
  piece reach the rate of its policy, the circuit opens and calls stop reaching the piece until it has been open
  for its time. A source that answers 429 or 503 saying how long to wait opens its circuit until then. The health
  of the node shows an open circuit as UNHEALTHY, with the time it opens again.

  Background:
    Given an HTTP source "orders-api" that answers with a list of orders
    And the descriptor declares the crawler "orders-crawler" polling "orders-api" every 1s

  @DEV-122
  Scenario: A source that answers 429 with Retry-After in seconds is not polled again until then
    Given the source "orders-api" answers 429 with the header "Retry-After: 3" once
    When the node is started from the descriptor
    Then the source "orders-api" is not polled again for 3s
    And the source "orders-api" is polled again after 3s

  @DEV-122
  Scenario: A source that answers 503 with Retry-After as an HTTP date is not polled again until that date
    Given the source "orders-api" answers 503 with a "Retry-After" header 3s ahead as an HTTP date once
    When the node is started from the descriptor
    Then the source "orders-api" is not polled again for 3s

  @DEV-122
  Scenario: A source declares the header the wait comes in, and its format
    Given the source of "orders-crawler" declares:
      """
      retryAfterHeader: X-RateLimit-Reset
      retryAfterFormat: EPOCH_SECONDS
      """
    And the source "orders-api" answers 429 with a "X-RateLimit-Reset" header 3s ahead in epoch seconds once
    When the node is started from the descriptor
    Then the source "orders-api" is not polled again for 3s

  @DEV-122
  Scenario: A source declares the field of the body the wait comes in
    Given the source of "orders-crawler" declares:
      """
      retryAfterField: /error/retryInSeconds
      """
    And the source "orders-api" answers 429 with the body {"error": {"retryInSeconds": 3}} once
    When the node is started from the descriptor
    Then the source "orders-api" is not polled again for 3s

  @DEV-122
  Scenario: A wait longer than the maximum of the source is cut to that maximum
    Given the source of "orders-crawler" declares:
      """
      maxRetryAfter: 2s
      """
    And the source "orders-api" answers 429 with the header "Retry-After: 3600" once
    When the node is started from the descriptor
    Then the source "orders-api" is polled again after 2s

  @DEV-122
  Scenario: The health of the node shows a source waiting with its circuit open
    Given the source "orders-api" answers 429 with the header "Retry-After: 30" once
    When the node is started from the descriptor
    And the source "orders-api" has been polled once
    And the health of the node is asked
    Then the module "orders-crawler" reports the piece "orders-api" of type "HTTP" as UNHEALTHY
    And the reason of the piece "orders-api" says its circuit is open until a time 30s ahead

  @DEV-122
  Scenario: A source that keeps failing opens its circuit, and calls stop reaching it
    Given the source of "orders-crawler" declares:
      """
      circuitFailureRate: 100%
      circuitMinimumCalls: 2
      circuitOpenFor: 3s
      """
    And the source "orders-api" answers 500 every time
    When the node is started from the descriptor
    And the source "orders-api" has been polled 2 times
    Then the source "orders-api" is not polled again for 3s

  @DEV-122
  Scenario: After its time open, a call that works closes the circuit
    Given the source of "orders-crawler" declares:
      """
      circuitFailureRate: 100%
      circuitMinimumCalls: 2
      circuitOpenFor: 2s
      """
    And the source "orders-api" answers 500 the first 2 times
    When the node is started from the descriptor
    And the source "orders-api" has been polled 3 times
    And the health of the node is asked
    Then the module "orders-crawler" reports the piece "orders-api" of type "HTTP" as HEALTHY

  @DEV-122
  Scenario: A policy that cannot be met is rejected when the descriptor is read
    Given the source of "orders-crawler" declares:
      """
      circuitFailureRate: 150%
      """
    When the descriptor is read
    Then it is rejected with a message that names the source "orders-api" and the key "circuitFailureRate"
