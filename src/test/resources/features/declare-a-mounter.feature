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
    And the error says "schedule.every is required"

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

  @DEV-81
  Scenario: A mounter that reads more than one partition is rejected
    Given the descriptor declares the mounter "payments-mounter":
      """
      type: POLL
      schedule:
        every: 1s
      mode: TRANSFORMATION
      read:
        partitions: ["source=payments", "source=refunds"]
      write:
        dataMart: payments
        partitioning:
          type: SAME_AS_SOURCE
      """
    When the node is started from the descriptor
    Then the descriptor is rejected
    And the error names the module "payments-mounter"
    And the error says "read.partitions must have exactly one partition"

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
    And the error says "infrastructure.quarantineRepository is required"

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
    And the error says "infrastructure.dataLake is required"

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
    And the error says "infrastructure.dataMart is required"

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
