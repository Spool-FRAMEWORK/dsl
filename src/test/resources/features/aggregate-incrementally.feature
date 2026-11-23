Feature: The descriptor gives a mounter of gold aggregate metadata to aggregate incrementally

  A mounter of gold that names a piece of aggregate metadata and has an incremental aggregator reads in each
  round only what it has not read yet (ADR-027). Without that piece it aggregates everything, as before.

  Background:
    Given a data lake and a data mart in temporary folders of the file system
    And aggregate metadata in a temporary folder of the file system

  @DEV-87
  Scenario: A mounter with an incremental aggregator and aggregate metadata adds only the new partitions
    Given silver has these "payments" records in the data mart "payments" and the closed partition "year=2026/month=11/day=17/hour=10":
      | id        | amount |
      | payment-1 | 10     |
      | payment-2 | 25     |
    And the descriptor declares the mounter "payments-total" that uses the aggregate metadata:
      """
      type: POLL
      schedule: {every: 1m}
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.IncrementalAmountTotal
      read: {partitions: ["source=payments"]}
      write: {dataMart: payments-total, partitioning: {type: SAME_AS_SOURCE}}
      """
    And the node is started from the descriptor
    And the mounter "payments-total" mounts once
    And silver has these "payments" records in the data mart "payments" and the closed partition "year=2026/month=11/day=17/hour=11":
      | id        | amount |
      | payment-3 | 5      |
    When the mounter "payments-total" mounts once
    Then gold has a single record with the total amount 40 in the data mart "payments-total"
    And the aggregate metadata of "payments-total" names the two partitions of silver

  @DEV-87
  Scenario: Aggregate metadata on a mounter whose aggregator is not incremental is rejected
    Given the descriptor declares the mounter "payments-total" that uses the aggregate metadata:
      """
      type: POLL
      schedule: {every: 1m}
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.AmountTotal
      read: {partitions: ["source=payments"]}
      write: {dataMart: payments-total, partitioning: {type: SAME_AS_SOURCE}}
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the module "payments-total"
    And the error says "aggregate metadata needs an incremental aggregator, and software.spool.dsl.acceptance.mounter.AmountTotal is not"

  @DEV-87
  Scenario: Aggregate metadata on a transformation mount is rejected
    Given the descriptor declares the mounter "payments-silver" that uses the aggregate metadata:
      """
      type: POLL
      schedule: {every: 1m}
      mode: TRANSFORMATION
      read: {partitions: ["source=payments"]}
      write: {dataMart: payments, partitioning: {type: SAME_AS_SOURCE}}
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error says "aggregate metadata is only used by an AGGREGATION mount"

  @DEV-133
  Scenario: A partition of silver is not aggregated until its closing margin has passed
    Given silver has these "payments" records in the data mart "payments" and the closed partition "year=2026/month=11/day=17/hour=10":
      | id        | amount |
      | payment-1 | 10     |
      | payment-2 | 25     |
    And silver has these "payments" records in the data mart "payments" and the partition of the previous hour:
      | id        | amount |
      | payment-3 | 5      |
    And the descriptor declares the mounter "payments-total" that uses the aggregate metadata:
      """
      type: POLL
      schedule: {every: 1m}
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.IncrementalAmountTotal
      read: {partitions: ["source=payments"], closingMargin: 2h}
      write: {dataMart: payments-total, partitioning: {type: SAME_AS_SOURCE}}
      """
    When the node is started from the descriptor
    And the mounter "payments-total" mounts once
    Then gold has a single record with the total amount 35 in the data mart "payments-total"

  @DEV-133
  Scenario: A closing margin of zero aggregates a partition as soon as its date has ended
    Given silver has these "payments" records in the data mart "payments" and the closed partition "year=2026/month=11/day=17/hour=10":
      | id        | amount |
      | payment-1 | 10     |
      | payment-2 | 25     |
    And silver has these "payments" records in the data mart "payments" and the partition of the previous hour:
      | id        | amount |
      | payment-3 | 5      |
    And the descriptor declares the mounter "payments-total" that uses the aggregate metadata:
      """
      type: POLL
      schedule: {every: 1m}
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.IncrementalAmountTotal
      read: {partitions: ["source=payments"], closingMargin: 0s}
      write: {dataMart: payments-total, partitioning: {type: SAME_AS_SOURCE}}
      """
    When the node is started from the descriptor
    And the mounter "payments-total" mounts once
    Then gold has a single record with the total amount 40 in the data mart "payments-total"

  @DEV-133
  Scenario: A closing margin on a mounter that does not aggregate incrementally is rejected
    Given the descriptor declares the mounter "payments-total":
      """
      type: POLL
      schedule: {every: 1m}
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.AmountTotal
      read: {partitions: ["source=payments"], closingMargin: 1h}
      write: {dataMart: payments-total, partitioning: {type: SAME_AS_SOURCE}}
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the module "payments-total"
    And the error says "closingMargin is only used by a mounter that aggregates incrementally"

  @DEV-133
  Scenario: A negative closing margin is rejected
    Given the descriptor declares the mounter "payments-total" that uses the aggregate metadata:
      """
      type: POLL
      schedule: {every: 1m}
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.IncrementalAmountTotal
      read: {partitions: ["source=payments"], closingMargin: -1h}
      write: {dataMart: payments-total, partitioning: {type: SAME_AS_SOURCE}}
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error says "read.closingMargin"
    And the error says "a duration such as 30d or P30D"
