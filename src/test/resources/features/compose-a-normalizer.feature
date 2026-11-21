Feature: The descriptor composes the normalizer of a source from named steps

  A source turns what it fetches into records with a normalizer. The descriptor can extend a standard normalizer at
  the stages it publishes, replace one of its steps or a whole stretch, or build one from scratch, always with named
  steps. Every combination is checked before the first record is read. The media type is declared apart and only
  labels the records the source writes.

  The test steps used here:

    | Step            | Takes                    | Gives                    | Does                                  |
    | UPPERCASE_NAMES | a JSON document          | a JSON document          | writes every item name in capitals    |
    | ADD_GREETING    | a JSON document          | a JSON document          | adds "greeting": "hello <name>"       |
    | NAMES_ONLY      | a stream of JSON records | a stream of texts        | keeps only the name of each record    |
    | TEXT_TO_BYTES   | a stream of texts        | a stream of records      | writes each text as a record          |
    | LINES           | the raw payload          | a stream of texts        | splits the payload into lines         |
    | READ_YAML       | the raw payload          | a JSON document          | reads the payload as YAML             |

  Background:
    Given the source fetches:
      """
      {"items": [{"name": "ana"}, {"name": "luis"}]}
      """

  @DEV-100
  Scenario: A source that names a standard normalizer uses it as it is
    Given the source block:
      """
      mediaType: JSON
      rootPath: items
      normalizer: JSON_ARRAY
      """
    When the descriptor is loaded
    And the source normalizes what it fetches
    Then the records are:
      | {"name":"ana"}  |
      | {"name":"luis"} |

  @DEV-100
  Scenario: A step is added after a stage of the standard normalizer
    Given the source block:
      """
      mediaType: JSON
      rootPath: items
      normalizer:
        from: JSON_ARRAY
        after:
          deserialized: [UPPERCASE_NAMES]
      """
    When the descriptor is loaded
    And the source normalizes what it fetches
    Then the records are:
      | {"name":"ANA"}  |
      | {"name":"LUIS"} |

  @DEV-100
  Scenario: Several steps after the same stage run in the order written
    Given the source block:
      """
      mediaType: JSON
      rootPath: items
      normalizer:
        from: JSON_ARRAY
        after:
          deserialized: [UPPERCASE_NAMES, ADD_GREETING]
      """
    When the descriptor is loaded
    And the source normalizes what it fetches
    Then the records are:
      | {"name":"ANA","greeting":"hello ANA"}   |
      | {"name":"LUIS","greeting":"hello LUIS"} |

  @DEV-100
  Scenario: A stretch of the standard normalizer is replaced by a chain of steps
    Given the source block:
      """
      mediaType: TEXT
      rootPath: items
      normalizer:
        from: JSON_ARRAY
        replace:
          - between: [split, serialized]
            with: [NAMES_ONLY, TEXT_TO_BYTES]
      """
    When the descriptor is loaded
    And the source normalizes what it fetches
    Then the records are:
      | ana  |
      | luis |

  @DEV-100
  Scenario: A step is replaced and the steps after it still run
    Given the source fetches:
      """
      items:
        - name: ana
        - name: luis
      """
    And the source block:
      """
      mediaType: JSON
      rootPath: items
      normalizer:
        from: JSON_ARRAY
        replace:
          - stage: deserialized
            with: READ_YAML
      """
    When the descriptor is loaded
    And the source normalizes what it fetches
    Then the records are:
      | {"name":"ana"}  |
      | {"name":"luis"} |

  @DEV-100
  Scenario: A normalizer is built from scratch
    Given the source fetches:
      """
      ana
      luis
      """
    And the source block:
      """
      mediaType: TEXT
      normalizer:
        steps: [LINES, TEXT_TO_BYTES]
      """
    When the descriptor is loaded
    And the source normalizes what it fetches
    Then the records are:
      | ana  |
      | luis |

  @DEV-100
  Scenario: A step whose output does not fit what comes next stops the load
    Given the source block:
      """
      mediaType: JSON
      rootPath: items
      normalizer:
        from: JSON_ARRAY
        replace:
          - stage: serialized
            with: NAMES_ONLY
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "source.normalizer.replace[0].with"
    And the error names "NAMES_ONLY"
    And the error names "java.util.stream.Stream<java.lang.String>"
    And the error names "java.util.stream.Stream<byte[]>"

  @DEV-100
  Scenario: A step added after a stage must give back what it takes
    Given the source block:
      """
      mediaType: JSON
      rootPath: items
      normalizer:
        from: JSON_ARRAY
        after:
          split: [NAMES_ONLY]
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "source.normalizer.after.split[0]"
    And the error names "NAMES_ONLY"

  @DEV-100
  Scenario: A step nobody declared stops the load
    Given the source block:
      """
      mediaType: JSON
      rootPath: items
      normalizer:
        from: JSON_ARRAY
        after:
          deserialized: [UPPERCASE_NAMEZ]
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "source.normalizer.after.deserialized[0]"
    And the error names "UPPERCASE_NAMEZ"
    And the error names "UPPERCASE_NAMES"

  @DEV-100
  Scenario: A stage the normalizer does not publish stops the load
    Given the source block:
      """
      mediaType: JSON
      rootPath: items
      normalizer:
        from: JSON_ARRAY
        after:
          parsed: [UPPERCASE_NAMES]
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "source.normalizer.after.parsed"
    And the error names "deserialized"

  @DEV-100
  Scenario: A normalizer that publishes no stages cannot be extended
    Given the source block:
      """
      mediaType: PDF
      normalizer:
        from: PDF
        after:
          raw: [LINES]
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "source.normalizer"
    And the error names "PDF"

  @DEV-100
  Scenario: A normalizer cannot be built from scratch and extend another one at the same time
    Given the source block:
      """
      mediaType: TEXT
      normalizer:
        from: JSON_ARRAY
        steps: [LINES, TEXT_TO_BYTES]
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "source.normalizer.steps"
    And the error names "source.normalizer.from"

  @DEV-100
  Scenario: A source without a normalizer explains how to migrate the old media type
    Given the source block:
      """
      mediaType: JSON_ARRAY
      rootPath: items
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "source.normalizer"
    And the error names "normalizer: JSON_ARRAY"
    And the error names "mediaType: JSON"

  @DEV-100
  Scenario: A source without a media type stops the load
    Given the source block:
      """
      rootPath: items
      normalizer: JSON_ARRAY
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "source.mediaType"

  @DEV-100
  Scenario: A media type that is neither a known alias nor a full media type stops the load
    Given the source block:
      """
      mediaType: JSON_ARRAY
      rootPath: items
      normalizer: JSON_ARRAY
      """
    When the descriptor is loaded
    Then the load fails
    And the error names "source.mediaType"
    And the error names "JSON_ARRAY"

  @DEV-100
  Scenario Outline: The records carry the media type the source declares
    Given the source block:
      """
      mediaType: <declared>
      rootPath: items
      normalizer: JSON_ARRAY
      """
    When the descriptor is loaded
    Then the records of the source are labelled "<label>"

    Examples:
      | declared             | label                |
      | JSON                 | application/json     |
      | application/x-ndjson | application/x-ndjson |

  @DEV-100
  Scenario: The effective normalizer is written to the log when the node starts
    Given the source block:
      """
      mediaType: JSON
      rootPath: items
      normalizer:
        from: JSON_ARRAY
        after:
          deserialized: [UPPERCASE_NAMES]
      """
    When the descriptor is loaded
    Then the log shows the normalizer of the source as:
      """
      [raw] deserialize [deserialized] UPPERCASE_NAMES [deserialized] extract [extracted] context-locate [located] split-enrich [split] serialize [serialized]
      """
