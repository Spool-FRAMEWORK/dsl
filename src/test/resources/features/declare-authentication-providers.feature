Feature: The descriptor declares named authentication providers

  Every HTTP entry of a node names the provider that identifies its callers. The providers are declared once, in
  the authentication block, and a provider the node cannot build stops the load.

  @DEV-82
  Scenario: Providers are declared once with a name
    Given the descriptor declares the provider "people" of type "OIDC"
    And the descriptor declares the provider "boe-webhook" of type "HMAC_SIGNATURE"
    When the descriptor is loaded
    Then the node knows the providers "people" and "boe-webhook"

  @DEV-82
  Scenario: A provider of an unknown type stops the load
    Given the descriptor declares the provider "people" of type "LDAP"
    When the descriptor is loaded
    Then the load fails
    And the error names "authentication.people.type"
    And the error names "OIDC"

  @DEV-82
  Scenario: A provider without a required setting stops the load
    Given the descriptor declares the provider "people" of type "OIDC" without an issuer
    When the descriptor is loaded
    Then the load fails
    And the error names "authentication.people.configuration.issuer"
