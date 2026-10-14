# Security

What is implemented, what is simplified, and what a production deployment
needs instead. Auth is **disabled by default** so the local demos work
without credentials.

## Authentication

* Bearer tokens (`Authorization: Bearer <token>`), mapped to principals in
  a JSON file passed with `-auth-config` (example:
  [deploy/auth.example.json](../deploy/auth.example.json)).
* Token comparison is constant-time across all configured keys.
* Brokers use a superuser token (`-token`) for controller and inter-broker
  calls; replication endpoints require a superuser.
* **Simplified**: static keys, no expiry or rotation, no identity provider,
  file-based secrets. Production: mTLS client certificates or OIDC/OAuth
  tokens from an identity provider, secrets in a secret manager, rotation
  without restarts.

## Authorization (ACLs)

* ACL = principal + resource (topic name, `prefix*`, or `*`) + operations
  (`produce`, `consume`, `describe`, `admin`).
* Produce permission never implies consume permission, and vice versa
  (`TestProduceDoesNotImplyConsume`): tenant A producing to `orders` does
  not let anyone read it.
* Admin operations (topic create/alter/delete, group offset reset) require
  `admin` (or superuser) and are written to the audit log.
* Consumer-group operations require `consume` on every subscribed topic.

## Tenant isolation

* Topics belong to a tenant (set from the creating principal when auth is
  enabled; a config patch cannot change it).
* A principal may access another tenant's topic only through an ACL that
  names that exact topic; wildcard and prefix ACLs never cross tenants
  (`TestTenantIsolation`).
* Quotas per tenant: produce byte rate, partition count.
* **Not isolated**: page cache, disk bandwidth, controller capacity.
  Large or sensitive tenants should get dedicated brokers or clusters.

## Payload limits

`max_event_bytes`, request size limits, header count limits and fetch size
caps bound memory per request (see [data-model.md](data-model.md#limits)).
Frame length fields are bounds-checked before allocation, so corrupt or
malicious lengths cannot trigger huge allocations (`FuzzDecode`).

## TLS

* Brokers and the controller serve HTTPS with `-tls-cert/-tls-key`;
  clients and brokers trust a CA with `-ca` / `Options.CAFile`.
* Minimum TLS 1.2 on the client side.
* **Simplified**: no mutual TLS, no certificate reloading, no per-listener
  configuration (client vs inter-broker). Production: mTLS for
  inter-broker traffic at minimum, automated certificate rotation.

## Audit logging

Structured log lines with `audit=true`: authorization denials, admin
operations (topic changes, offset resets), fault-injection and limit
changes on brokers. Production: ship audit logs to an append-only store
outside the cluster.

## Data at rest

Not encrypted by the application. Checksums detect corruption, not
tampering ([data-model.md](data-model.md#checksums)). Production: disk
encryption, and per-tenant payload encryption if tenants must not trust
operators (which also enables crypto-shredding for deletion requests).

## Admin and debug surfaces

* Fault injection and limits endpoints are admin-only when auth is
  enabled. They should be disabled entirely in production builds or
  bound to an internal network.
* `pprof` is served only when `-pprof-addr` is set, on a separate listener
  (bind it to localhost).
