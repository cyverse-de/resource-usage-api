# resource-usage-api

`resource-usage-api` is a microservice developed as part of the CyVerse Discovery
Environment. It tracks what a user has consumed: CPU hours from completed analyses,
and data store usage read from the iRODS ICAT database. Both are reported to the
`subscriptions` service, which is the system of record for quotas and usage.

This service absorbed the former `data-usage-api`, whose routes it serves unchanged.

# Build

```
make
```

The service builds as a static binary and ships in the image built by
`skaffold.yaml`.

# Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Greeting, used as the liveness and readiness probe |
| `GET` | `/summary/:username` | CPU usage, data usage, and subscription for a user |
| `GET` | `/:username/data/current` | The user's current data usage as recorded in QMS |
| `POST` | `/:username/data/update` | Recompute the user's data usage from ICAT and record it |
| `GET` | `/:username/data/overage` | Whether the user is over their data quota |

`/:username/data/current` reports `404` when QMS holds no reading for the user. That
is not an error condition: a refresh is enqueued at the same time, so a later request
will find one.

# AMQP

The service consumes three queues on the DE exchange.

| Queue | Routing keys | Purpose |
|---|---|---|
| `resource-usage-api` | `jobs.updates` | Analysis status updates, which drive CPU hour calculation |
| `data-usage-api.batch` | `index.all`, `index.usage.data`, `index.usage.data.batch.user.#` | Fan out and process data usage refreshes in batches |
| `data-usage-api.individual` | `index.usage.data.user.#` | Refresh one user's data usage |

The data usage queue names are inherited from the service this one absorbed, and are
prefixed when `amqp.queueprefix` is set.

Batch messages carry their `{"start": ..., "end": ...}` username bounds in the body.
The bounds also appear in the routing key, where they are only informational: usernames
contain dots, which are the routing key's own separator, so a key cannot say which
username a dot belongs to. Messages published before the bounds moved into the body are
still read from the key.

A message that this service cannot read is dropped. Anything that fails for a reason
another attempt may get past — the ICAT or subscriptions service being unreachable, a
query outrunning its deadline — is returned to the queue instead, and is held briefly
first if it has already come back once.

# Configuration

Settings are read from a YAML file, then a dotenv file, then the environment, in that
order of increasing precedence. Environment variables are named by upper-casing the
key, replacing `.` with `_`, and prefixing `DISCOENV_`.

| Key | Required | Default | Description |
|---|---|---|---|
| `db.uri` | yes | | DE database connection URI |
| `db.schema` | | `public` | Schema holding the DE tables |
| `icat.uri` | yes | | iRODS ICAT database connection URI |
| `icat.zone` | yes | | iRODS zone name |
| `icat.rootresources` | yes | | Root resources to count usage against |
| `users.domain` | yes | | User domain, with or without a leading `@` |
| `datausage.refreshinterval` | | `3h` | Age at which a data usage reading is refreshed |
| `amqp.uri` | yes | | Broker connection URI |
| `amqp.exchange.name` | yes | | Exchange to bind to |
| `amqp.exchange.type` | yes | | Exchange type |
| `amqp.queueprefix` | | | Prefix for the data usage queue names |
| `amqp.batchsize` | | `100` | Users per data usage refresh batch |
| `qms.enabled` | | `false` | Whether to build summaries from the subscriptions service |

Keys are lower-case and free of underscores so that every one of them can be
overridden from the environment. `icat.rootresources` is a list, which the
environment cannot express, so it has to come from the configuration file.

The service validates its configuration at startup and exits if anything required is
missing or unusable.
