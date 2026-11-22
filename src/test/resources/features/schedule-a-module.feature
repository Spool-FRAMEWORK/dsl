Feature: The descriptor says when a module runs with schedule

  The schedule of a crawler, a janitor or a mounter is once, every interval, or a cron expression with its
  zone (ADR-015). Only one form at a time, and a cron expression always names its zone.

  @DEV-85
  Scenario Outline: Every example of ADR-015 loads and runs on the date its comment says
    Given a crawler whose schedule is <schedule>
    When the descriptor is loaded
    Then after a round that ends at "<after>" the crawler runs next at "<next>"

    Examples:
      | schedule                                               | after                     | next                      |
      | {cron: "0 8 1 * *", zone: Europe/Madrid}               | 2026-11-22T10:07:00+01:00 | 2026-12-01T08:00:00+01:00 |
      | {cron: "@hourly", zone: Europe/Madrid}                 | 2026-11-22T10:07:00+01:00 | 2026-11-22T11:00:00+01:00 |
      | {cron: "@daily", zone: Europe/Madrid}                  | 2026-11-22T10:07:00+01:00 | 2026-11-23T00:00:00+01:00 |
      | {cron: "@midnight", zone: Europe/Madrid}               | 2026-11-22T10:07:00+01:00 | 2026-11-23T00:00:00+01:00 |
      | {cron: "@weekly", zone: Europe/Madrid}                 | 2026-11-22T10:07:00+01:00 | 2026-11-29T00:00:00+01:00 |
      | {cron: "@monthly", zone: Europe/Madrid}                | 2026-11-22T10:07:00+01:00 | 2026-12-01T00:00:00+01:00 |
      | {cron: "@yearly", zone: Europe/Madrid}                 | 2026-11-22T10:07:00+01:00 | 2027-01-01T00:00:00+01:00 |
      | {cron: "@annually", zone: Europe/Madrid}               | 2026-11-22T10:07:00+01:00 | 2027-01-01T00:00:00+01:00 |
      | {cron: "*/15 * * * *", zone: UTC}                      | 2026-11-22T10:07:00Z      | 2026-11-22T10:15:00Z      |
      | {cron: "0 */6 * * *", zone: UTC}                       | 2026-11-22T10:07:00Z      | 2026-11-22T12:00:00Z      |
      | {cron: "0 9-18 * * MON-FRI", zone: Europe/Madrid}      | 2026-11-22T10:07:00+01:00 | 2026-11-23T09:00:00+01:00 |
      | {cron: "30 6 * * MON,THU", zone: Europe/Madrid}        | 2026-11-22T10:07:00+01:00 | 2026-11-23T06:30:00+01:00 |
      | {cron: "0 0 1,15 * *", zone: Europe/Madrid}            | 2026-11-22T10:07:00+01:00 | 2026-12-01T00:00:00+01:00 |
      | {cron: "0 2 * JAN,APR,JUL,OCT *", zone: Europe/Madrid} | 2026-11-22T10:07:00+01:00 | 2027-01-01T02:00:00+01:00 |
      | {cron: "0 0 1 */3 *", zone: Europe/Madrid}             | 2026-11-22T10:07:00+01:00 | 2027-01-01T00:00:00+01:00 |
      | {every: 5m}                                            | 2026-11-22T10:07:00Z      | 2026-11-22T10:12:00Z      |
      | {every: PT5M}                                          | 2026-11-22T10:07:00Z      | 2026-11-22T10:12:00Z      |

  @DEV-85
  Scenario: A crawler with schedule once runs when it starts and never again
    Given a crawler whose schedule is once
    When the descriptor is loaded
    Then the crawler runs when it starts
    And the crawler does not run again after its first round

  @DEV-85
  Scenario: A crawler without schedule runs once
    Given a crawler without schedule
    When the descriptor is loaded
    Then the crawler runs when it starts
    And the crawler does not run again after its first round

  @DEV-85
  Scenario: A janitor runs on a cron expression
    Given a janitor whose schedule is {cron: "0 3 * * *", zone: Europe/Madrid}
    When the descriptor is loaded
    Then after a round that ends at "2026-11-22T10:07:00+01:00" the janitor runs next at "2026-11-23T03:00:00+01:00"

  @DEV-85
  Scenario: A polling mounter runs on a cron expression
    Given a polling mounter whose schedule is {cron: "0 3 * * *", zone: Europe/Madrid}
    When the descriptor is loaded
    Then after a round that ends at "2026-11-22T10:07:00+01:00" the mounter runs next at "2026-11-23T03:00:00+01:00"

  @DEV-85
  Scenario Outline: A schedule that cannot be read fails with the path of the value
    Given a crawler whose schedule is <schedule>
    When the descriptor is loaded
    Then the load fails
    And the error names "<error>"

    Examples:
      | schedule                                   | error                                                                                                           |
      | {cron: "0 8 1 *", zone: Europe/Madrid}     | modules[0].crawler.schedule.cron must be a Unix cron expression of 5 fields or a macro, got '0 8 1 *'           |
      | {cron: "@reboot", zone: Europe/Madrid}     | modules[0].crawler.schedule.cron does not accept @reboot: use schedule: once to run once when the module starts |
      | {cron: "0 8 1 * *", zone: Europe/Atlantis} | modules[0].crawler.schedule.zone must be a time zone such as Europe/Madrid or UTC, got 'Europe/Atlantis'        |
      | {cron: "0 8 1 * *"}                        | modules[0].crawler.schedule.zone is required                                                                    |
      | {every: 5m, zone: Europe/Madrid}           | modules[0].crawler.schedule.zone is only allowed with cron                                                      |
      | {every: 5m, cron: "0 8 1 * *", zone: UTC}  | modules[0].crawler.schedule must have either every or cron, not both                                            |
      | {}                                         | modules[0].crawler.schedule must have every or cron                                                             |
      | twice                                      | modules[0].crawler.schedule must be once or an object with every or cron, got 'twice'                           |
      | {every: 0s}                                | modules[0].crawler.schedule.every must be a positive duration, got '0s'                                         |
