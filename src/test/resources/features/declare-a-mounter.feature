Feature: A mounter is declared in the descriptor

  A mounter fills one data mart from partitions of the data lake. A TRANSFORMATION mount reads bronze and writes
  silver in the data lake; an AGGREGATION mount reads silver and writes gold in the data mart of the infrastructure.
  The descriptor says how the mounter runs, what it reads, where it writes each record, whether it validates what it
  promotes and how it combines the records.

  Background:
    Given a data lake and a data mart in temporary folders of the file system
    And a quarantine in a temporary folder of the file system

  @DEV-81
  Scenario: A mounter promotes a partition of bronze to silver and keeps the partition of each record
    Given the descriptor has no data mart
    And bronze has these "payments" records in the partition "source=payments/eventType=Payment":
      | id        | amount |
      | payment-1 | 10     |
      | payment-2 | 25     |
    And the descriptor declares the mounter "payments-silver":
      """
      type: POLL
      schedule:
        every: 1m
      mode: TRANSFORMATION
      read:
        partitions: ["source=payments/eventType=Payment"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    And the mounter "payments-silver" mounts once
    Then silver has the records "payment-1" and "payment-2" in the data mart "payments"
    And silver keeps each record in the partition it had in bronze

  @DEV-81
  Scenario: A mounter writes each record in the partition built from its attributes
    Given bronze has these "payments" records in the partition "source=payments/eventType=Payment":
      | id        | amount | country |
      | payment-1 | 10     | ES      |
      | payment-2 | 25     | FR      |
    And the descriptor declares the mounter "payments-silver":
      """
      type: POLL
      schedule:
        every: 1m
      mode: TRANSFORMATION
      read:
        partitions: ["source=payments/eventType=Payment"]
      write:
        dataMart: payments
        partitioning:
          type: ATTRIBUTES
          attributes: [payload.country]
      """
    When the node is started from the descriptor
    And the mounter "payments-silver" mounts once
    Then silver has the record "payment-1" in the partition "payload.country=ES" of the data mart "payments"
    And silver has the record "payment-2" in the partition "payload.country=FR" of the data mart "payments"

  @DEV-81
  Scenario: A record without a partition attribute goes to the quarantine instead of silver
    Given bronze has these "payments" records in the partition "source=payments/eventType=Payment":
      | id        | amount | country |
      | payment-1 | 10     | ES      |
      | payment-2 | 25     |         |
    And the descriptor declares the mounter "payments-silver":
      """
      type: POLL
      schedule:
        every: 1m
      mode: TRANSFORMATION
      read:
        partitions: ["source=payments/eventType=Payment"]
      write:
        dataMart: payments
        partitioning:
          type: ATTRIBUTES
          attributes: [payload.country]
      """
    When the node is started from the descriptor
    And the mounter "payments-silver" mounts once
    Then silver has the record "payment-1" in the partition "payload.country=ES" of the data mart "payments"
    And silver does not have the record "payment-2"
    And the record "payment-2" is in the quarantine
    And bronze no longer has the record "payment-2"

  @DEV-81
  Scenario: A mounter that validates keeps the rejected records out of silver
    Given the payments source rejects a negative amount
    And bronze has these "payments" records in the partition "source=payments/eventType=Payment":
      | id        | amount |
      | payment-1 | 10     |
      | payment-2 | -5     |
    And the descriptor declares the mounter "payments-silver":
      """
      type: POLL
      schedule:
        every: 1m
      mode: TRANSFORMATION
      validate: true
      read:
        partitions: ["source=payments/eventType=Payment"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    And the mounter "payments-silver" mounts once
    Then silver has the record "payment-1" in the data mart "payments"
    And silver does not have the record "payment-2"
    And the record "payment-2" is in the quarantine

  @DEV-81
  Scenario: A mounter combines the records of silver with its aggregator into gold
    Given silver has these "payments" records in the data mart "payments" and the partition "source=payments/eventType=Payment":
      | id        | amount |
      | payment-1 | 10     |
      | payment-2 | 25     |
    And the descriptor declares the mounter "payments-total":
      """
      type: POLL
      schedule:
        every: 1m
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.AmountTotal
      read:
        partitions: ["source=payments"]
      write:
        dataMart: payments-total
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    And the mounter "payments-total" mounts once
    Then gold has a single record with the total amount 35 in the data mart "payments-total"

  @DEV-96
  Scenario: A mounter promotes every partition it declares, and nothing else
    Given bronze has these "payments" records in the partition "source=payments/eventType=Payment":
      | id        | amount |
      | payment-1 | 10     |
    And bronze has these "refunds" records in the partition "source=refunds/eventType=Refund":
      | id       | amount |
      | refund-1 | -5     |
    And bronze has these "fees" records in the partition "source=fees/eventType=Fee":
      | id    | amount |
      | fee-1 | 2      |
    And the descriptor declares the mounter "movements-silver":
      """
      type: POLL
      schedule:
        every: 1m
      mode: TRANSFORMATION
      read:
        partitions: ["source=payments", "source=refunds"]
      write:
        dataMart: movements
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    And the mounter "movements-silver" mounts once
    Then silver has the records "payment-1" and "refund-1" in the data mart "movements"
    And silver does not have the record "fee-1"

  @DEV-96
  Scenario: A mounter aggregates in gold the records of two sources
    Given silver has these "payments" records in the data mart "payments" and the partition "source=payments/eventType=Payment":
      | id        | amount |
      | payment-1 | 10     |
      | payment-2 | 25     |
    And silver has these "refunds" records in the data mart "refunds" and the partition "source=refunds/eventType=Refund":
      | id       | amount |
      | refund-1 | -5     |
    And silver has these "fees" records in the data mart "fees" and the partition "source=fees/eventType=Fee":
      | id    | amount |
      | fee-1 | 100    |
    And the descriptor declares the mounter "net-total":
      """
      type: POLL
      schedule:
        every: 1m
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.AmountTotal
      read:
        partitions: ["source=payments", "source=refunds"]
      write:
        dataMart: net-total
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    And the mounter "net-total" mounts once
    Then gold has a single record with the total amount 30 in the data mart "net-total"

  @DEV-96
  Scenario: A record in two partitions that overlap is read once
    Given silver has these "payments" records in the data mart "payments" and the partition "source=payments/eventType=Payment":
      | id        | amount |
      | payment-1 | 10     |
      | payment-2 | 25     |
    And the descriptor declares the mounter "payments-total":
      """
      type: POLL
      schedule:
        every: 1m
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.AmountTotal
      read:
        partitions: ["source=payments", "source=payments/eventType=Payment"]
      write:
        dataMart: payments-total
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    And the mounter "payments-total" mounts once
    Then gold has a single record with the total amount 35 in the data mart "payments-total"

  @DEV-81
  Scenario: A reactive mounter promotes a partition when bronze reports it was written
    Given bronze has these "payments" records in the partition "source=payments/eventType=Payment":
      | id        | amount |
      | payment-1 | 10     |
    And the descriptor declares the mounter "payments-silver":
      """
      type: REACTIVE
      mode: TRANSFORMATION
      read:
        partitions: ["source=payments/eventType=Payment"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    And the ingester reports that it wrote the partition "source=payments/eventType=Payment"
    Then silver has the record "payment-1" in the data mart "payments"

  @DEV-81
  Scenario: A polling mounter without an interval is rejected
    Given the descriptor declares the mounter "payments-mounter":
      """
      type: POLL
      mode: TRANSFORMATION
      read:
        partitions: ["source=payments"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the module "payments-mounter"
    And the error says "schedule is required"

  @DEV-81
  Scenario: A mounter without the partitioning of what it writes is rejected
    Given the descriptor declares the mounter "payments-mounter":
      """
      type: POLL
      schedule:
        every: 1s
      mode: TRANSFORMATION
      read:
        partitions: ["source=payments"]
      write:
        dataMart: payments
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the module "payments-mounter"
    And the error says "write.partitioning.type is required"

  @DEV-81
  Scenario: A mounter that partitions by attributes without naming them is rejected
    Given the descriptor declares the mounter "payments-mounter":
      """
      type: POLL
      schedule:
        every: 1s
      mode: TRANSFORMATION
      read:
        partitions: ["source=payments"]
      write:
        dataMart: payments
        partitioning:
          type: ATTRIBUTES
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the module "payments-mounter"
    And the error says "write.partitioning.attributes is required"

  @DEV-96
  Scenario: A mounter that reads no partition is rejected
    Given the descriptor declares the mounter "payments-mounter":
      """
      type: POLL
      schedule:
        every: 1s
      mode: TRANSFORMATION
      read:
        partitions: []
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the module "payments-mounter"
    And the error says "read.partitions must have at least one partition"

  @DEV-81
  Scenario: A mounter to gold that validates is rejected
    Given the descriptor declares the mounter "payments-mounter":
      """
      type: POLL
      schedule:
        every: 1s
      mode: AGGREGATION
      validate: true
      read:
        partitions: ["source=payments"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the module "payments-mounter"
    And the error says "validation only applies to TRANSFORMATION"

  @DEV-81
  Scenario: A mounter to silver without a quarantine is rejected
    Given the descriptor has no quarantine
    And the descriptor declares the mounter "payments-mounter":
      """
      type: POLL
      schedule:
        every: 1s
      mode: TRANSFORMATION
      read:
        partitions: ["source=payments"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the module "payments-mounter"
    And the error says "needs a QUARANTINE_REPOSITORY, and infrastructure declares none"

  @DEV-81
  Scenario: A mounter without a data lake is rejected
    Given the descriptor has no data lake
    And the descriptor declares the mounter "payments-mounter":
      """
      type: POLL
      schedule:
        every: 1s
      mode: AGGREGATION
      read:
        partitions: ["source=payments"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the module "payments-mounter"
    And the error says "needs a DATA_LAKE, and infrastructure declares none"

  @DEV-81
  Scenario: A mounter to gold without a data mart in the infrastructure is rejected
    Given the descriptor has no data mart
    And the descriptor declares the mounter "payments-mounter":
      """
      type: POLL
      schedule:
        every: 1s
      mode: AGGREGATION
      read:
        partitions: ["source=payments"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the module "payments-mounter"
    And the error says "needs a DATA_MART, and infrastructure declares none"

  @DEV-81
  Scenario: A mounter whose aggregator does not exist is rejected
    Given the descriptor declares the mounter "payments-mounter":
      """
      type: POLL
      schedule:
        every: 1s
      mode: AGGREGATION
      aggregator: com.acme.Missing
      read:
        partitions: ["source=payments"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the module "payments-mounter"
    And the error says "aggregator com.acme.Missing is not on the class path"

  @DEV-81
  Scenario: A mounter whose aggregator does not combine records is rejected
    Given the descriptor declares the mounter "payments-mounter":
      """
      type: POLL
      schedule:
        every: 1s
      mode: AGGREGATION
      aggregator: java.lang.String
      read:
        partitions: ["source=payments"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the module "payments-mounter"
    And the error says "aggregator java.lang.String does not combine records"

  @DEV-113
  Scenario: A reactive mounter of gold mounts what a reactive mounter of silver has just mounted
    Given bronze has these "payments" records in the partition "source=payments/eventType=Payment":
      | id        | amount |
      | payment-1 | 10     |
      | payment-2 | 25     |
    And the descriptor declares the mounter "payments-silver":
      """
      type: REACTIVE
      mode: TRANSFORMATION
      read:
        partitions: ["source=payments/eventType=Payment"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    And the descriptor declares the mounter "payments-total":
      """
      type: REACTIVE
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.AmountTotal
      read:
        partitions: ["source=payments"]
      write:
        dataMart: payments-total
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    And the ingester reports that it wrote the partition "source=payments/eventType=Payment"
    Then gold has a single record with the total amount 35 in the data mart "payments-total"

  @DEV-132
  Scenario: A reactive mounter of gold mounts when a result of silver is taken out of the quarantine
    Given silver has these "payments" records in the data mart "payments" and the partition "source=payments":
      | id        | amount |
      | payment-1 | 10     |
    And the descriptor declares the mounter "payments-silver":
      """
      type: REACTIVE
      mode: TRANSFORMATION
      read:
        partitions: ["source=payments/eventType=Payment"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    And the descriptor declares the mounter "payments-total":
      """
      type: REACTIVE
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.AmountTotal
      read:
        partitions: ["source=payments"]
      write:
        dataMart: payments-total
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    And the result of "silver/payments" with the id "payment-2" and the amount 25 is taken out of the quarantine in the partition "source=payments"
    Then gold has a single record with the total amount 35 in the data mart "payments-total"

  @DEV-140
  Scenario: A mounter of gold that aggregates everything keeps a single result when its total changes
    Given silver has these "payments" records in the data mart "payments" and the partition "source=payments":
      | id        | amount |
      | payment-1 | 10     |
      | payment-2 | 25     |
    And the descriptor declares the mounter "payments-total":
      """
      type: POLL
      schedule:
        every: 1m
      mode: AGGREGATION
      aggregator: software.spool.dsl.acceptance.mounter.AmountTotal
      read:
        partitions: ["source=payments"]
      write:
        dataMart: payments-total
        partitioning:
          type: SAME_AS_SOURCE
      """
    And the node is started from the descriptor
    And the mounter "payments-total" mounts once
    And silver has these "payments" records in the data mart "payments" and the partition "source=payments":
      | id        | amount |
      | payment-3 | 5      |
    When the mounter "payments-total" mounts once
    Then gold has a single record with the total amount 40 in the data mart "payments-total"

  @DEV-140
  Scenario: A record that leaves bronze is no longer in silver after the next round
    Given bronze has these "payments" records in the partition "source=payments/eventType=Payment":
      | id        | amount |
      | payment-1 | 10     |
      | payment-2 | 25     |
    And the descriptor declares the mounter "payments-silver":
      """
      type: POLL
      schedule:
        every: 1m
      mode: TRANSFORMATION
      read:
        partitions: ["source=payments/eventType=Payment"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    And the node is started from the descriptor
    And the mounter "payments-silver" mounts once
    And the record "payment-2" leaves bronze
    When the mounter "payments-silver" mounts once
    Then silver has the record "payment-1" in the data mart "payments"
    And silver does not have the record "payment-2"
