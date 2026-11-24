Feature: An ingester declares how it writes, and a type that does not exist stops the descriptor

  The type of an ingester says how it writes to the data lake: FLUSH in batches, REACTIVE each envelope on arrival.
  It is required, and anything else stops the descriptor naming the value and the types there are, as with a mounter.

  @DEV-138
  Scenario Outline: An ingester of a type that exists is built
    Given a descriptor with an ingester of type "<type>"
    When the node is built from it
    Then the node has the ingester

    Examples:
      | type     |
      | FLUSH    |
      | REACTIVE |

  @DEV-138
  Scenario: An ingester of a type that does not exist stops the descriptor, naming the value and the types
    Given a descriptor with an ingester of type "SIDEWAYS"
    When the node is built from it
    Then building fails with an error that says "must be one of FLUSH, REACTIVE, got 'SIDEWAYS'"

  @DEV-138
  Scenario: An ingester without a type stops the descriptor
    Given a descriptor with an ingester without a type
    When the node is built from it
    Then building fails with an error that says "type is required"
