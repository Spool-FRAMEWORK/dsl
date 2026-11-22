Feature: A node reports the health of each piece of infrastructure its modules use

  Each module watches the pieces it names in uses: every real call to a piece, and a check of its own when the
  plugin of its type has one. The health of the node lists, for each module, every piece with its name in the
  descriptor, its type and its status. A module takes the worst status of its pieces, and the node the worst of
  its modules.

  Background:
    Given a data lake and a data mart in temporary folders of the file system
    And a quarantine in a temporary folder of the file system
    And the descriptor declares the mounter "payments-silver":
      """
      type: POLL
      schedule: {every: 1m}
      mode: TRANSFORMATION
      read: {partitions: ["source=payments"]}
      write: {dataMart: payments, partitioning: {type: SAME_AS_SOURCE}}
      """

  @DEV-7
  Scenario: The health of the node lists each piece a module uses, with its type
    When the node is started from the descriptor
    And the health of the node is asked
    Then the module "payments-silver" reports the piece "lake" of type "FILE_SYSTEM" as HEALTHY
    And the module "payments-silver" reports the piece "bus" of type "IN_MEMORY" as HEALTHY
    And the node is HEALTHY

  @DEV-7
  Scenario: A piece whose check fails makes its module and the node unhealthy
    Given the folder of the data lake cannot be written
    When the node is started from the descriptor
    And the health of the node is asked
    Then the module "payments-silver" reports the piece "lake" of type "FILE_SYSTEM" as UNHEALTHY
    And the reason of the piece "lake" names the folder of the data lake
    And the node is UNHEALTHY and answers with the status 503

  @DEV-7
  Scenario: A record of the data lake that cannot be read leaves the piece unhealthy
    Given bronze has a record in the partition "source=payments" that cannot be read
    When the node is started from the descriptor
    And the health of the node is asked
    Then the module "payments-silver" reports the piece "lake" of type "FILE_SYSTEM" as UNHEALTHY
    And the reason of the piece "lake" names the record that cannot be read

  @DEV-7
  Scenario: A piece whose type has no check of its own is judged only by its calls
    When the node is started from the descriptor
    And the health of the node is asked
    Then the piece "bus" of the module "payments-silver" has no check of its own
    And the module "payments-silver" reports the piece "bus" of type "IN_MEMORY" as HEALTHY
