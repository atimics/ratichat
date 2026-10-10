# RatiChat Matrix pilot on Fly.io

This directory contains the low-cost invited-pilot deployment for:

| Public host | Service | Fly app | Pilot size | State |
| --- | --- | --- | --- | --- |
| `matrix.rati.chat` | Continuwuity 26.8.1 | `ratichat-matrix` | shared CPU, 1 GB | 10 GB `matrix_data` volume |
| `id.rati.chat` | Pocket ID 2.14.0 | `ratichat-id` | shared CPU, 512 MB | 1 GB `pocket_data` volume |
| `chat.rati.chat` and `rati.chat` | Element Web 1.12.26 and Matrix discovery | `ratichat-chat` | shared CPU, 256 MB | stateless |

Every image has a release tag and an immutable multi-platform digest. All three
apps stay in the `sjc` region and keep one Machine active.

This change contains configuration only. It creates no Fly app, volume,
certificate, DNS record, Pocket ID account, or Matrix account.

## Identity boundary

Production Matrix IDs use `rati.chat`, such as `@alice:rati.chat`.
Continuwuity stores this value as `server_name`. Changing it requires a database
wipe. Run early experiments with the local Compose file or a separate test
domain and test database.

Keep these values stable for the life of the service:

- Matrix server name: `rati.chat`
- Matrix service URL: `https://matrix.rati.chat`
- Pocket ID issuer and WebAuthn origin: `https://id.rati.chat`
- Element Web URL: `https://chat.rati.chat`
- Continuwuity signing key and Pocket ID user subject values

Continuwuity has no supported Synapse database migration. Approve Continuwuity
as the homeserver for this identity domain before the first external account.

## Login flow

Element Web uses native Matrix OAuth with Continuwuity. Continuwuity uses OIDC
with Pocket ID. Pocket ID verifies the user's passkey.

Delegated OIDC puts Continuwuity in OAuth-only mode. Test every supported Matrix
client before inviting users. RatiChat uses an admin-issued service access token
and stores it as a secret. A passkey restores login access. Element encryption
key recovery remains a separate user flow.

## Local validation

Run the static checks:

```sh
./scripts/validate-matrix-pilot.sh
```

Run a local container smoke test:

```sh
./scripts/validate-matrix-pilot.sh --smoke
```

The smoke test uses temporary Docker volumes, disables federation and account
registration, checks all three HTTP endpoints, and removes its volumes when it
finishes. It tests service startup. Production OIDC and passkeys require the
public HTTPS hosts.

## Before creating resources

1. Choose the Fly organization.
2. Confirm that the three app names in the table are available. If a name
   changes, update its `fly.toml` and the validation constants in the same PR.
3. Store the Pocket ID encryption key in a team password vault.
4. Assign an operator for the first Pocket ID and Matrix admin accounts.
5. Confirm that `rati.chat` is the final Matrix identity domain.

## Create the Fly apps and volumes

These commands make live resources. Run them only during the approved launch
window. Replace `<fly-org>` with the chosen organization.

```sh
fly apps create ratichat-id --org <fly-org>
fly apps create ratichat-matrix --org <fly-org>
fly apps create ratichat-chat --org <fly-org>

fly volumes create pocket_data --app ratichat-id --region sjc --size 1
fly volumes create matrix_data --app ratichat-matrix --region sjc --size 10
```

Each stateful app uses one Machine and one local Fly Volume. Its deployment
strategy stops the old Machine before starting the replacement with the same
volume.

## Set secrets

Generate a random Pocket ID encryption key with a trusted password manager or
an operating-system random generator. Keep a recovery copy outside Fly. Set it
before the first Pocket ID deploy:

```sh
fly secrets set ENCRYPTION_KEY='<random-32-byte-value>' --app ratichat-id
```

Deploy Pocket ID from the repository root:

```sh
fly deploy deploy/matrix/pocket-id \
  --config deploy/matrix/pocket-id/fly.toml
```

Add the certificate and route `id.rati.chat` only after the app is healthy:

```sh
fly certs add id.rati.chat --app ratichat-id
```

Open `https://id.rati.chat/setup` and create the first admin passkey. Configure
user signup as **Signup with token**. Keep anonymous email one-time access off.
Follow the [self-service signup setup](pocket-id/README.md) to assign the chat
group automatically and return new users to chat after passkey setup. That
guide also covers the operator's choice to open public registration.
Create a Pocket ID OIDC client named `RatiChat Matrix` with this exact callback:

```text
https://matrix.rati.chat/_continuwuity/oidc/complete
```

Give the client the `openid` scope. Then assign its allowed users:

1. Open **User Groups** in Pocket ID. Create a group with the name
   `ratichat-members` and the friendly name **RatiChat members**.
2. Add the operator's Pocket ID account to this group. Add each invited account
   when its chat access is approved.
3. Open **OIDC Clients**, edit **RatiChat Matrix**, and expand
   **Allowed User Groups**.
4. Select **RatiChat members** and save. Keep the client restricted to this
   group.
5. Confirm that the client list shows **Allowed Group Count: 1** and
   **Restricted: Yes**.

Pocket ID v2 creates each OIDC client with restricted access. Its
[allowed-group rules](https://pocket-id.org/docs/configuration/allowed-groups)
require this assignment before a group member can sign in to Matrix. The
operator's admin account also needs membership in the selected group.

Copy the client's ID and secret into Continuwuity's Fly secrets:

```sh
fly secrets set \
  CONTINUWUITY_OAUTH__OIDC__CLIENT_ID='<pocket-id-client-id>' \
  CONTINUWUITY_OAUTH__OIDC__CLIENT_SECRET='<pocket-id-client-secret>' \
  --app ratichat-matrix
```

Fly secrets hold the active values. Keep an encrypted recovery copy of the
Pocket ID encryption key and OIDC client secret outside the mounted volumes.

### Repair the access error

If Pocket ID displays **You are not allowed to access this service**, check
**OIDC Clients → RatiChat Matrix → Allowed User Groups**. A restricted client
with an allowed group count of zero produces this error. Assign the approved
chat group, save, and confirm that the affected account belongs to that group.

Start a fresh sign-in from `https://chat.rati.chat` after saving the rule.
Confirm that the browser completes the return to Matrix and opens the chat
client. Service health checks and passkey authentication each cover part of
this flow; successful chat entry is the access check.

## Deploy Matrix and Element Web

Deploy Continuwuity only after Pocket ID discovery works:

```sh
curl --fail --silent --show-error \
  https://id.rati.chat/.well-known/openid-configuration >/dev/null

fly deploy deploy/matrix/continuwuity \
  --config deploy/matrix/continuwuity/fly.toml
fly certs add matrix.rati.chat --app ratichat-matrix
```

Deploy Element Web after the Matrix versions endpoint is healthy. The image
also serves the Matrix discovery files for the `rati.chat` identity domain:

```sh
curl --fail --silent --show-error \
  https://matrix.rati.chat/_matrix/client/versions >/dev/null

fly deploy deploy/matrix/element-web \
  --config deploy/matrix/element-web/fly.toml
fly certs add chat.rati.chat --app ratichat-chat
fly certs add rati.chat --app ratichat-chat
```

Route `chat.rati.chat` to `ratichat-chat.fly.dev`. Route the apex `rati.chat`
A and AAAA records to the addresses returned by:

```sh
fly ips list --app ratichat-chat
```

Wait for both certificates to become ready:

```sh
fly certs check chat.rati.chat --app ratichat-chat
fly certs check rati.chat --app ratichat-chat
```

The apex now opens Element Web and owns Matrix discovery. Keep both public
names on the same Fly app so a future web-host change also moves the discovery
files as one tested unit.

No workflow deploys these apps. Keep deployment manual until the production
secrets, external backups, and recovery checks are ready.

## Publish Matrix discovery

The Element image serves these committed files:

- `element-web/well-known/matrix/client` as `/.well-known/matrix/client`
- `element-web/well-known/matrix/server` as `/.well-known/matrix/server`

Its Nginx template serves both as `application/json`. The client response also
includes:

```text
Access-Control-Allow-Origin: *
```

After DNS and certificates are ready, check the public headers and values:

```sh
curl --fail --silent --show-error --include \
  https://rati.chat/.well-known/matrix/client
curl --fail --silent --show-error --include \
  https://rati.chat/.well-known/matrix/server
```

The expected values are:

```json
{
  "m.homeserver": {
    "base_url": "https://matrix.rati.chat"
  }
}
```

```json
{
  "m.server": "matrix.rati.chat:443"
}
```

## Launch checks

Complete each check before sending an external invite:

1. `https://id.rati.chat/healthz` returns a successful HTTP status
   (the pinned Pocket ID 2.14.0 image returns HTTP 204).
2. `https://matrix.rati.chat/_matrix/client/versions` returns HTTP 200.
3. `https://chat.rati.chat/config.json` points only to `matrix.rati.chat`.
4. Both `rati.chat` discovery files return the committed JSON with
   `Content-Type: application/json`.
5. The client discovery response includes `Access-Control-Allow-Origin: *`.
6. Matrix federation reaches `matrix.rati.chat` on port 443.
7. An invited user enrolls two independent passkeys on separate authenticators.
8. The user belongs to **RatiChat members**, and **RatiChat Matrix** allows
   this group. Either passkey completes Element's OAuth login and opens chat.
9. A new browser restores the user's Matrix encryption keys and encrypted
   history through the selected Element recovery method.
10. RatiChat joins, reads, and replies in an approved room with its service
    token.
11. Health checks stay green through one restart of each app.
12. A full backup restores into separate test Machines within the recovery
    target.

## Backup and recovery

Fly Volume snapshots are the first recovery layer. The external backup must
also include the data needed to keep the identity stable.

For Continuwuity:

1. Send `!admin server backup-database` in its admin room.
2. Confirm completion with `!admin server list-backups`.
3. Copy the complete selected generation from `/data/backups`.
4. Copy `/data/database/media` and record the deployed image digest and
   configuration.
5. Include the server signing key from the data directory.
6. Encrypt the archive before uploading it to a private Tigris bucket.

For Pocket ID:

1. Use Pocket ID's `export` command to create a portable archive.
2. Keep the automatic Fly Volume snapshot as the full-data recovery layer.
3. Record the deployed image digest and public `APP_URL`.
4. Keep the matching `ENCRYPTION_KEY` in the separate secrets recovery record.
5. Encrypt the export before uploading it to the private backup bucket.

Pocket ID marks its export and import feature as experimental. Test both the
portable export and a full-volume recovery before launch. Automate the copy and
alert on a missed backup before inviting users. Run a restore drill before
launch and once each month. Continuwuity's online backup has an operator-led
restore procedure, so follow the upstream maintenance guide for the pinned
release.

## Updates

Open a PR for every image update. In that PR:

1. Change both the release tag and digest.
2. Read the upstream release and security notes.
3. Run the static check and local smoke test.
4. Back up the stateful service.
5. Deploy to a test app and complete the login and federation checks.
6. Deploy one production service at a time.
7. Record the final digest and health results.

## Primary references

- [Continuwuity Docker deployment](https://continuwuity.org/deploying/docker)
- [Continuwuity delegated authentication](https://continuwuity.org/guides/oidc)
- [Continuwuity split-domain delegation](https://continuwuity.org/guides/delegation)
- [Continuwuity backup guide](https://continuwuity.org/maintenance)
- [Pocket ID installation](https://pocket-id.org/docs/setup/installation)
- [Pocket ID configuration](https://pocket-id.org/docs/configuration/environment-variables)
- [Pocket ID data export and import](https://pocket-id.org/docs/setup/data-export-import)
- [Pocket ID user management](https://pocket-id.org/docs/setup/user-management)
- [Element Web configuration](https://github.com/element-hq/element-web/blob/develop/docs/config.md)
- [Fly app configuration](https://fly.io/docs/reference/configuration/)
- [Fly Volumes](https://fly.io/docs/volumes/overview/)
