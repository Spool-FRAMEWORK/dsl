Feature: Variables in the descriptor

  A descriptor declares its own variables and reads environment variables, so a value is written once and no
  secret has to live in the file. Variables are replaced after the includes and the profile are combined.

  @DEV-78
  Scenario: A value uses a variable of the descriptor
    Given the descriptor:
      """
      variables:
        root: /data/spool
      infrastructure:
        eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: "${root}"}}
      modules: []
      """
    When the descriptor is loaded
    Then the inbox path is "/data/spool"

  @DEV-78
  Scenario: A reference inside a longer text
    Given the descriptor:
      """
      variables:
        root: /data/spool
      infrastructure:
        eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: "${root}/inbox"}}
      modules: []
      """
    When the descriptor is loaded
    Then the inbox path is "/data/spool/inbox"

  @DEV-78
  Scenario: A variable uses another variable
    Given the descriptor:
      """
      variables:
        root: /data/spool
        inbox: ${root}/inbox
      infrastructure:
        eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: "${inbox}"}}
      modules: []
      """
    When the descriptor is loaded
    Then the inbox path is "/data/spool/inbox"

  @DEV-78
  Scenario: A value is read from the environment
    Given the environment variable "SPOOL_ROOT" is "/mnt/spool"
    And the descriptor:
      """
      infrastructure:
        eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: "${env:SPOOL_ROOT}/inbox"}}
      modules: []
      """
    When the descriptor is loaded
    Then the inbox path is "/mnt/spool/inbox"

  @DEV-78
  Scenario: An environment variable that is not set takes its default
    Given the descriptor:
      """
      infrastructure:
        eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: "${env:SPOOL_ROOT:-/tmp/spool}/inbox"}}
      modules: []
      """
    When the descriptor is loaded
    Then the inbox path is "/tmp/spool/inbox"

  @DEV-78
  Scenario: A doubled dollar keeps a reference as plain text
    Given the descriptor:
      """
      infrastructure:
        eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: "/data/$${root}"}}
      modules: []
      """
    When the descriptor is loaded
    Then the inbox path is "/data/${root}"

  @DEV-78
  Scenario: The chosen profile overrides a variable
    Given the descriptor:
      """
      variables:
        root: /data/spool
      profiles:
        production:
          variables:
            root: /srv/spool
      infrastructure:
        eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: "${root}"}}
      modules: []
      """
    When the descriptor is loaded with the profile "production"
    Then the inbox path is "/srv/spool"

  @DEV-78
  Scenario: YAML anchors repeat a whole block
    Given the descriptor:
      """
      infrastructure:
        eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: &local {path: /data/spool}}
        dataLake: {kind: DATA_LAKE, type: FILE_SYSTEM, configuration: *local}
      modules: []
      """
    When the descriptor is loaded
    Then the data lake path is "/data/spool"

  @DEV-78
  Scenario: A variable that does not exist stops the load
    Given the descriptor:
      """
      infrastructure:
        eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: "${rot}/inbox"}}
      modules: []
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "infrastructure.inbox.configuration.path"
    And the error names "${rot}"

  @DEV-78
  Scenario: Variables that use each other in a circle stop the load
    Given the descriptor:
      """
      variables:
        a: ${b}
        b: ${a}
      infrastructure:
        eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: "${a}"}}
      modules: []
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "a -> b -> a"

  @DEV-78
  Scenario: An environment variable that is not set and has no default stops the load
    Given the descriptor:
      """
      infrastructure:
        eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM, configuration: {path: "${env:SPOOL_ROOT}"}}
      modules: []
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "infrastructure.inbox.configuration.path"
    And the error names "${env:SPOOL_ROOT}"

  @DEV-78
  Scenario: A value read from the environment never shows up in an error
    Given the environment variable "TTL" is "s3cr3t-value"
    And the descriptor:
      """
      infrastructure:
        eventBus: {kind: EVENT_BUS, type: IN_MEMORY}
        inbox: {kind: INBOX, type: FILE_SYSTEM}
      modules:
        - janitor:
            id: j1
            configuration:
              ttl: ${env:TTL}
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "${env:TTL}"
    And the error does not contain "s3cr3t-value"
