Feature: A descriptor read from the file system is composed like one from the classpath

  Includes are read next to the file that names them, and a profile is applied the same way, whatever the
  source of the descriptor.

  @DEV-84
  Scenario: The included files are read next to the descriptor
    Given the file "shared/infrastructure.yaml" with an event bus of type "IN_MEMORY"
    And the file "Spool.yaml" that includes "shared/infrastructure.yaml"
    When "Spool.yaml" is loaded from the file system
    Then the event bus is of type "IN_MEMORY"

  @DEV-84
  Scenario: A profile is applied to a descriptor of the file system
    Given the file "Spool.yaml" with an event bus of type "IN_MEMORY" and a profile "production" that makes it "KAFKA"
    When "Spool.yaml" is loaded from the file system with the profile "production"
    Then the event bus is of type "KAFKA"

  @DEV-84
  Scenario: A cycle between files of the file system is named
    Given the file "a.yaml" that includes "b.yaml"
    And the file "b.yaml" that includes "a.yaml"
    When "a.yaml" is loaded from the file system
    Then loading fails with the cycle "a.yaml -> b.yaml -> a.yaml"
