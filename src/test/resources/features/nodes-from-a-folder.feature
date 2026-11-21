Feature: Every descriptor of a folder is a node, each on its own port

  The files of a folder and its subfolders whose name ends in .spool.yaml are nodes; any other file, like one that is
  only included, is not. Each node opens its HTTP server on the port of server.port, or on 8080 when the descriptor
  does not say it, so two nodes of the same process need different ports.

  @DEV-24
  Scenario: A descriptor says the port of its node
    Given the descriptor:
      """
      server: {port: 8091}
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
      modules: []
      """
    When the descriptor is loaded
    Then the node opens its HTTP server on the port 8091

  @DEV-24
  Scenario: Without a port, the node uses 8080
    Given the descriptor:
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
      modules: []
      """
    When the descriptor is loaded
    Then the node opens its HTTP server on the port 8080

  @DEV-24
  Scenario: Every .spool.yaml of a folder and its subfolders is a node, and the files they include are not
    Given the folder has the file "shared/infrastructure.yaml":
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
      """
    And the folder has the file "boe/boe.spool.yaml":
      """
      include: [../shared/infrastructure.yaml, boe-modules.yaml]
      server: {port: 8091}
      """
    And the folder has the file "boe/boe-modules.yaml":
      """
      modules: []
      """
    And the folder has the file "bocm.spool.yaml":
      """
      include: [shared/infrastructure.yaml]
      server: {port: 8092}
      modules: []
      """
    And the folder has the file "notes.yaml":
      """
      not: a descriptor
      """
    When the folder is loaded from the file system
    Then the folder gives the nodes "bocm.spool.yaml" and "boe/boe.spool.yaml"
    And the node of "boe/boe.spool.yaml" opens its HTTP server on the port 8091
    And the node of "bocm.spool.yaml" opens its HTTP server on the port 8092

  @DEV-24
  Scenario: Every .spool.yaml of a folder of the classpath is a node
    When the folder "/descriptors/folder" is loaded from the classpath
    Then the folder gives the nodes "bocm.spool.yaml" and "boe/boe.spool.yaml"

  @DEV-24
  Scenario: Two descriptors of a folder on the same port fail with both files
    Given the folder has the file "boe.spool.yaml":
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
      modules: []
      """
    And the folder has the file "bocm.spool.yaml":
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
      modules: []
      """
    When the folder is loaded from the file system
    Then loading the folder fails
    And the error of the folder names "bocm.spool.yaml and boe.spool.yaml both open port 8080: give each node its own server.port"

  @DEV-24
  Scenario: An invalid descriptor of a folder fails with its file
    Given the folder has the file "boe.spool.yaml":
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
      modules:
        - crawler: {type: POLL, id: crawler}
      """
    When the folder is loaded from the file system
    Then loading the folder fails
    And the error of the folder names "boe.spool.yaml"
    And the error of the folder names "modules[0].crawler.source is required"

  @DEV-24
  Scenario: A folder without any .spool.yaml fails saying so
    Given the folder has the file "infrastructure.yaml":
      """
      infrastructure:
        bus: {kind: EVENT_BUS, type: IN_MEMORY}
      """
    When the folder is loaded from the file system
    Then loading the folder fails
    And the error of the folder names "has no descriptor: the descriptors of a folder end in .spool.yaml"
