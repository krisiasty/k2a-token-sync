# k2a-token-sync

`k2a-token-sync` — Kubernetes to ArgoCD, token sync — keeps ArgoCD's registrations for downstream
Kubernetes clusters valid, using short-lived ServiceAccount tokens it mints and rotates itself.

It replaces the permanent, non-expiring credential `argocd cluster add` leaves behind, without changing anything about
how ArgoCD connects.

## Why this is needed

Registering a cluster with ArgoCD means putting a credential for it in a Secret, and ArgoCD has no way to renew one. So
the credential has to be long-lived: a ServiceAccount token that never expires, or a client certificate that lasts a
year and then breaks every Application on that cluster at once.

A permanent cluster-admin token sitting at rest is also the pattern Kubernetes itself is retiring — the TokenRequest API
is the endorsed mechanism, auto-generated ServiceAccount token Secrets were removed in 1.24, and recent releases track
last-use on legacy tokens and clean up unused ones. And a credential with no expiry satisfies no rotation policy: it
cannot even be revoked selectively, since deleting the ServiceAccount is the only lever.

`k2a-token-sync` keeps each cluster registered with a **short-lived** ServiceAccount token that it mints and rotates
itself, from outside ArgoCD. Nothing about ArgoCD changes: it is the same `argocd-manager` identity and the same cluster
Secret format, so this replaces the credential half of `argocd cluster add` and leaves the rest alone.

## How it works

Two paths, and keeping them apart is the whole design:

- **Control path** — k2a-token-sync connects to each downstream cluster with its own narrowly-scoped credential to mint
  ArgoCD's token. Each cluster is reconciled every five minutes, and almost every pass finds nothing to do.
- **Request path** — ArgoCD connects straight to the cluster's own endpoint with the credential k2a-token-sync published.

If k2a-token-sync is down, reconciliation pauses; ArgoCD keeps working. With the default 30-day token lifetime
reissued at half life, k2a-token-sync can be down for two weeks before anything degrades.

Per cluster, every pass:

1. Connects to the cluster's own endpoint — the same address ArgoCD uses — with the credential provisioned at
   bootstrap, replacing that credential if it is a day old.
2. Reads the cluster CA from the `kube-root-ca.crt` ConfigMap. This is the bundle ArgoCD will be given.
3. Probes that endpoint's TLS certificate, verifying it against the bundle just read and checking the SANs cover the
   configured address.
4. Checks that the `argocd-manager` ServiceAccount and its `cluster-admin` binding are still there, restoring either if
   it is not. Two reads when nothing is wrong.
5. Applies everything in the ArgoCD cluster Secret except the credential. The apply's response is also how it learns
   whether ArgoCD still holds one, since it cannot read that Secret. Nothing changed means nothing written.

Step 3 is not what establishes that the endpoint works — step 1 already did. That connection goes to the same address,
so its own TLS handshake has to verify against the CA stored with the credential and match the endpoint's name. A
certificate that is untrusted or does not cover the endpoint fails the pass at step 1, with the API server's TLS error,
and the probe never runs at all.

What the probe adds is the two things a client connection never reports: how long the certificate has left, and whether
it verifies against the CA the cluster publishes **now**. That second one is the question that matters, because that
bundle is what ArgoCD is about to be handed — and it can differ from the CA stored at bootstrap if the cluster's CA has
been rotated since. Connecting successfully with yesterday's bundle says nothing about whether today's still works.

During bootstrap the same probe genuinely is a pre-flight. There the administrative connection often arrives through a
management proxy at an entirely different address, so nothing has touched the direct endpoint yet — which is why it is
checked before any identity is created.

Step 4 runs every pass rather than only when a credential is due, because ArgoCD's identity is the one thing this tool
depends on that a person can delete, and the damage is otherwise invisible. A bound token carries the ServiceAccount's
UID, so deleting that account stops every token issued for it from authenticating — including the one ArgoCD is holding
— while the published Secret still contains a bearer token and still matches what was recorded. Checked only at reissue,
that would have gone unnoticed for the two weeks until the next one, with the log reporting the credential as current
throughout. A recreated ServiceAccount therefore forces an immediate reissue; a recreated binding does not, since the
existing token still authenticates and has simply regained its permissions.

ArgoCD's credential is reissued only once it is past half its lifetime, or when something it depends on has drifted —
so the great majority of passes stop there, having written nothing at all. When one is due, the pass continues:

1. Mints a bound token via the TokenRequest API for that identity — the same one `argocd cluster add` installs —
   honouring whatever lifetime the API server grants.
2. Uses that token, against that endpoint, with that CA bundle, to ask the API server what it is allowed to do. This
   is the only check that exercises all four together, which is what ArgoCD depends on.
3. Writes the credential, then re-applies the registration. That order means ArgoCD never sees a cluster it cannot
   authenticate to, not even briefly between two applies. It re-reads cluster Secrets on every reconcile, so credentials
   swap with no restart of any ArgoCD component.

### How drift is noticed

k2a-token-sync holds **no read permission** on the Secrets it writes, which is deliberate: it cannot read ArgoCD's own
Secrets either. It therefore never watches what it published — it learns the state from what its own apply returns, since
an apply hands back the object it produced and needs only `patch`.

The consequence is worth stating plainly: something that changes those Secrets from outside is noticed at the next pass,
not when it happens. Delete a generated Secret and it comes back within five minutes; the same goes for one emptied or
edited by hand. That window is the reconciliation interval, and it is short precisely because an unchanged pass writes
nothing — so there is no cost to looking often.

The credential's own expiry is never at risk from this, being reissued at half its lifetime, days ahead of any deadline.

### Certificate expiry

k2a-token-sync uses bearer tokens, so no client certificate expires. But the API server's **serving** certificate still
matters: once it expires, or if it never covered the endpoint, ArgoCD's TLS handshake fails no matter how fresh the
token is.

So k2a-token-sync **observes** it — probing the endpoint each pass, verifying the presented chain against the CA it
publishes as ArgoCD's `caData`, and reporting expiry in `/status` and in its logs. It warns from 90 days out by default.

It never rotates anything. Reissuing a serving certificate needs node access and restarts control-plane components, so
it belongs to whatever manages the cluster — worth automating with your existing configuration management, one
control-plane node at a time. The cluster CA is deliberately out of scope too: rotating it would invalidate every
kubeconfig and every ArgoCD `caData` at once.

## Credentials

Two credentials exist per cluster, and they are not interchangeable.

| | ArgoCD's credential | k2a-token-sync's credential |
| --- | --- | --- |
| Identity | `argocd-manager` in `kube-system` | `k2a-token-sync`, same namespace |
| Permissions | `cluster-admin` — ArgoCD applies arbitrary manifests | four rules, see below |
| Form | bound token (TokenRequest) | bound token (TokenRequest) |
| Lifetime | `tokenTTL`, default 720h (30d) | `selfTokenTTL`, default 2160h (90d) |
| Renewed | at half its granted life, ~15d | daily, or at half its granted life if that comes first |
| Stored in | `cluster-<name>` in ArgoCD's namespace | `<name>-credentials` in k2a-token-sync's namespace |
| Used by | ArgoCD, connecting straight to the endpoint | k2a-token-sync, to mint the other one |
| Created by | k2a-token-sync | bootstrap |
| On expiry | k2a-token-sync mints another | k2a-token-sync is locked out; bootstrap again |

**Why two.** k2a-token-sync never needs `cluster-admin`. Its own identity holds four rules: get and create ServiceAccounts,
create `serviceaccounts/token`, get and create ClusterRoleBindings, and get exactly one ConfigMap — `kube-root-ca.crt`.
That is enough to maintain ArgoCD's identity and read the cluster CA, and nothing else.

Reusing ArgoCD's token for both would be simpler and worse. k2a-token-sync would hold `cluster-admin` on every cluster
permanently, and it would not even remove the permanence: a credential you can always renew *is* a permanent credential,
with extra steps. What would change is only the blast radius, in the wrong direction.

**Lifecycle.** Bootstrap creates both identities and mints the first token for its own. From then on k2a-token-sync
mints ArgoCD's tokens and renews its own, verifying each replacement against the cluster before storing it —
overwriting a working credential with a broken one would lock it out, and that is the one failure self-renewal could
introduce.

**When renewal fails.** The credential in hand keeps working, so ArgoCD is unaffected and the pass carries on
publishing as normal — abandoning that work would turn a problem with this tool's credential into an outage of the thing
it exists to serve. What changes is that the `SelfCredentialValid` condition goes `False` from the very first failure,
naming which step failed and when the credential in use runs out. Renewal is retried on every pass, five minutes apart.
Once the remaining lifetime falls below a quarter of what was granted — or below one renewal interval, whichever is
longer, so a credential the API server capped short counts as urgent immediately — `Ready` goes `False` too, because at
that point the question is no longer freshness but whether this cluster is about to need bootstrapping by hand.

A credential carrying no readable `expires-at` counts as urgent from the first failure. Reading one is tolerant of a
missing expiry precisely because the next renewal replaces it with a deadline that is known; if that renewal is what
fails, the tool holds something that works and cannot say whether it has ninety days or ninety seconds left, and an
unanswerable question about losing a cluster is answered pessimistically.

**The downtime budget.** `selfTokenTTL` measured from the *last renewal*, which happens daily, so about 90 days by
default. Nothing else breaks meanwhile: ArgoCD's own token stays valid until its own expiry. Past that, bootstrap the
cluster again — the `kubectl get ccon` output will say `CredentialExpired` rather than reporting a bare 401.

**Revocation.** `TokenRequest` tokens cannot be revoked individually. Deleting the ServiceAccount invalidates all of its
tokens, which is the only lever, and for its own identity implies bootstrapping again.

**One nuance worth knowing.** RBAC's escalation check means its own identity cannot create the `argocd-manager`
cluster-admin binding: creating a binding requires holding the role's permissions or `bind` on it, and it has neither. In
steady state the binding already exists and is only read, so this never comes up. If someone deletes it, reconciliation
fails with *"attempting to grant RBAC permissions not currently held"* until bootstrap is re-run. That is the escalation
check working exactly as intended, and a confusing error to meet cold.

## Prerequisites

- ArgoCD, and k2a-token-sync, running on a cluster that can reach each downstream API server directly.
- One-time bootstrap access per cluster, see [Bootstrap](#bootstrap).

**Check the API server's certificate SANs first.** A serving certificate normally covers the node's own addresses,
`127.0.0.1`, `localhost` and the in-cluster names — but not a VIP, load balancer or FQDN unless that name was included
when the certificate was issued. If the endpoint you point ArgoCD at is missing from the SANs, TLS verification fails no
matter which credential is used, and a kubeconfig taken from a control-plane node will not reveal it, because that
connects to `127.0.0.1`.

k2a-token-sync checks this explicitly and refuses to publish a registration ArgoCD could never use, reporting the
certificate's actual SANs. Add the missing name to the API server's serving certificate — how depends on your
distribution — and restart or reissue it.

## Deployment

### Helm

The chart installs k2a-token-sync, its RBAC and the ClusterConnection CRD. It does not
manage cluster objects — those are separate manifests, see
[Adding a cluster](#adding-a-cluster).

```bash
helm install k2a-token-sync ./charts/k2a-token-sync \
  --namespace k2a-token-sync --create-namespace \
  --set image.tag=v0.11.0
```

#### Key chart values

| Value | Default | Description |
| --- | --- | --- |
| `image.repository` | `ghcr.io/krisiasty/k2a-token-sync` | Image repository |
| `image.tag` | **required** | Released version to deploy, e.g. `v0.11.0`. Rendering fails if unset |
| `argocdNamespace` | `argocd` | Namespace of the ArgoCD instance served; all cluster Secrets go here |
| `health.port` | `8080` | Port for `/livez`, `/readyz`, `/status` and `/metrics` |

There is nothing here about clusters, and that is the point: the inventory lives in the API, so adding one needs no
release upgrade.

#### Upgrading

```bash
# 1. the CRD, if this release changed it — Helm installs crds/ and never touches it again
kubectl apply -f charts/k2a-token-sync/crds/

# 2. the release
helm upgrade k2a-token-sync ./charts/k2a-token-sync \
  --namespace k2a-token-sync \
  --set image.tag=v0.11.0 --wait --timeout 5m
```

**The CRD comes first, and only when it changed.** Helm applies `crds/` at install and ignores it ever after, so a
schema change is yours to apply. Order matters whenever a release adds status fields: the API server prunes anything the
schema does not know, so a new binary writing to an old CRD loses exactly the state it just computed, and loses it
silently. `kubectl diff -f charts/k2a-token-sync/crds/` answers whether there is anything to do — it exits 0 when there
is not, which is the common case.

**Name the version.** `image.tag` has no default, so an upgrade that omits it fails to render rather than quietly
keeping the running one. That is deliberate, and it is why `--reuse-values` is the wrong habit here: the version you are
deploying should be stated, not inherited from whatever was set last time.

**What it does to reconciliation.** One replica and a `Recreate` strategy — two instances would race to mint and publish
the same credentials — so there is a gap of a few seconds where nothing reconciles. Nothing depends on that gap: ArgoCD
holds credentials with days or weeks left and never talks to this tool.

**`--wait` is worth more here than usual.** Readiness is not "the container started": it passes once every cluster in the
inventory has completed a pass *in the new process*. So a green `helm upgrade --wait` means the new version has actually
reconciled the whole fleet, which is the thing you wanted to know. The corollary is that one unhealthy cluster will hold
the rollout open until the timeout — usually what you want, occasionally not.

`--atomic` will roll back automatically if that happens. Worth it when nobody is watching; against it, a rollback also
destroys the evidence of why the new version could not reconcile.

Afterwards, `kubectl -n k2a-token-sync get ccon` should show every cluster `Ready` with a recent `lastSyncTime`, and the
pod should be running the tag you named.

**Rolling back** is `helm rollback k2a-token-sync`. The CRD is not rolled back and does not need to be: fields an older
binary never writes are inert, so a newer schema is safe under an older release.

#### ArgoCD

Note the bootstrap ordering: ArgoCD manages k2a-token-sync on the local cluster, and k2a-token-sync in turn maintains ArgoCD's
registrations for every downstream cluster.

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: k2a-token-sync
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/krisiasty/k2a-token-sync
    targetRevision: HEAD
    path: charts/k2a-token-sync
    helm:
      values: |
        image:
          tag: "v0.11.0"      # required; the release to deploy
  destination:
    server: https://kubernetes.default.svc
    namespace: k2a-token-sync
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

A second Application can own the ClusterConnection objects if you want the fleet declared in git — optional, and
discussed under [Deployment paths](#deployment-paths).

### Without Helm

There is no second set of hand-maintained manifests, deliberately: a parallel copy would have to reproduce the RBAC and
the generated CRD by hand, and would drift. Render the chart instead, and apply or commit the output:

```bash
helm template k2a-token-sync ./charts/k2a-token-sync \
  --namespace k2a-token-sync --set image.tag=v0.11.0 --include-crds > k2a-token-sync.yaml
```

## Configuration

k2a-token-sync takes three settings from the environment. Everything else about a cluster lives in its ClusterConnection.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `POD_NAMESPACE` | yes | | Namespace k2a-token-sync runs in; its inventory and credentials live here |
| `ARGOCD_NAMESPACE` | no | `argocd` | Namespace of the ArgoCD instance served |
| `HEALTH_PORT` | no | `8080` | Port for `/livez`, `/readyz`, `/status` and `/metrics` |

One instance serves one ArgoCD, so `ARGOCD_NAMESPACE` is a process setting rather than a per-cluster one. Point a
second release at a second ArgoCD if you need that.

### The ClusterConnection

One object per cluster, in k2a-token-sync's namespace. The minimal form is a name and an endpoint; everything else has a
default from the CRD schema, so `kubectl explain clusterconnection.spec` is the authoritative field reference.

```yaml
apiVersion: k2a-token-sync.io/v1alpha1
kind: ClusterConnection
metadata:
  name: standalone-1
  namespace: k2a-token-sync
spec:
  endpoint: 10.1.0.10          # :6443 assumed
```

[`examples/cluster-connection.yaml`](examples/cluster-connection.yaml) spells out every field, and the test suite parses
it so it cannot drift from the API.

Two things a schema cannot check are checked by k2a-token-sync and reported on the object: a name long enough to make the
Secret names derived from it invalid, and two connections claiming one `secretName`, which would have them silently
overwrite each other.

A contested `secretName` stops **every** claimant, not just the newcomer. Picking one would mean picking by list order,
which is alphabetical and has nothing to do with ownership: a connection added later but named earlier would take over a
Secret another cluster is registered under, republish it against its own endpoint, and leave the dispossessed one
excluded from reconciliation with its status frozen. ArgoCD would go on trusting a registration now pointing somewhere
else. There is no answer to which claimant should win that this tool can safely invent, so it declines to choose and
writes nothing until one claim remains. That stalls a cluster; the alternative misdirects one.

Both verdicts are written to the object rather than only to this process's `/status`, because the object is where you
will look. A spec that does not resolve gets `Ready=False` with reason `InvalidSpec`; a contested `secretName` gets
`Ready=False` with reason `SecretNameConflict` and a separate `Conflict` condition whose message names the other
claimants. `observedGeneration` records the generation that was rejected, so a spec you have since edited is
distinguishable from one the verdict still applies to. Fixing the object clears both — including the conflict case, where
the fix is deleting the *other* object and this one's spec never changes at all.

### Adding a cluster

```bash
k2a-token-sync bootstrap --cluster standalone-1 --endpoint 10.1.0.10 \
  --from-kubeconfig ./standalone-1.kubeconfig
```

That is the whole thing. Bootstrap prepares the downstream cluster, stores the credential, and applies the
ClusterConnection; k2a-token-sync publishes ArgoCD's credential within one poll, roughly 30 seconds. No release upgrade,
no restart, and no RBAC change — the chart's Role does not name individual Secrets, so nothing about it depends on how
many clusters exist.

It is safe to re-run. Every step is idempotent, including the object, which is written with server-side apply.

If you keep the ClusterConnections in git instead, see [Keeping the objects in
git](#keeping-the-objects-in-git). Order never matters either way: an object applied before its credential exists reports
`Ready=False` with reason `AwaitingCredential`, which makes `kubectl get ccon` a worklist of what is still outstanding.

Editing works the same way: `kubectl edit ccon standalone-1`, and the change takes effect within a poll. k2a-token-sync
compares the spec's generation against the one recorded in status, so an edit made while it was down is noticed too.

### Removing a cluster

```bash
kubectl delete ccon standalone-1
kubectl -n argocd delete secret cluster-standalone-1     # not done for you
```

Deleting the ClusterConnection (ccon) object stops maintenance. It deliberately does **not** delete the generated Secret:
k2a-token-sync holds no delete permission in ArgoCD's namespace, so removing a registration ArgoCD is actively using
stays an explicit act. Left alone, the credential in it expires within `tokenTTL`.

Two more objects outlive the ClusterConnection, and can be removed once ArgoCD no longer needs the cluster: the
`argocd-manager` and `k2a-token-sync` ServiceAccounts downstream, along with their bindings.

With no `list` permission in ArgoCD's namespace, k2a-token-sync cannot detect a Secret left behind by a connection
that was removed while it was down. That is the cost of the RBAC posture below, and it is why cleanup is a documented
step rather than a promise.

## Bootstrap

k2a-token-sync has no way into a cluster until an identity exists for it there, and it deliberately never holds
administrative material of its own. Something therefore has to establish the first foothold, using a credential no
repository should contain — which makes bootstrap an imperative step by nature. `argocd cluster add` has the same seam;
the difference is what the act leaves behind.

```bash
k2a-token-sync bootstrap --cluster standalone-1 \
  --endpoint standalone-1.example.com:6443 \
  --from-kubeconfig ./standalone-1.kubeconfig
```

```text
Bootstrapping standalone-1

Downstream cluster — standalone-1
  reached via           https://standalone-1.example.com:6443
  ArgoCD endpoint       https://standalone-1.example.com:6443
  endpoint certificate  valid until 2027-07-31 (364 days left)
  identities            kube-system/argocd-manager, kube-system/k2a-token-sync
  verified              the new credential works against the endpoint

Cluster running ArgoCD — context gitops (https://gitops.example.com:6443)
  credential            k2a-token-sync/standalone-1-credentials, expires 2026-10-30
  registration          k2a-token-sync/standalone-1

Done. k2a-token-sync publishes ArgoCD's credential within 30 seconds:
  kubectl -n k2a-token-sync get ccon standalone-1
```

Output is grouped by cluster, because "where did that happen" is the first question when one command writes to two of
them. Registering the cluster ArgoCD itself runs on is a normal case, and the second heading then says "the same cluster
as above" rather than leaving you to compare two addresses.

**The bootstrap does, in order:**

- resolve both clusters before changing anything;
- read the cluster CA;
- **probe the endpoint** and refuse if its certificate does not cover it;
- install the two identities;
- store the credential;
- use that credential once against the endpoint to prove the whole path works;
- apply the ClusterConnection.

The pre-flight is there because a certificate that does not cover the endpoint is the most common reason direct access
fails, and it is far cheaper to learn before two identities exist than after. The verification is a warning rather than
an error — the endpoint may be reachable from the cluster k2a-token-sync runs on but not from your desk.

Downstream access is a **file**, not a context: files are what you can copy off a control-plane node.
`--from-kubeconfig` takes a path to a file with downstream cluster kubeconfig, `--from-context` selects context within it
or within your ambient kubeconfig. Separate files are supported on purpose, because merging kubeconfigs is unsafe when
both define the same context name for different clusters.

The credential never passes through your terminal — it goes from the downstream cluster into k2a-token-sync's namespace
directly. Progress goes to stderr, so `--print` can send the manifest to stdout without anything else in the way.

### Modes

| Mode | Prepares the cluster | Stores the credential | ClusterConnection |
| --- | --- | --- | --- |
| default | yes | yes | applied |
| `--print` | yes | yes | written to stdout, not applied |
| `--dry-run` | no | no | shown as a preview |

Every mode checks the endpoint first: that a certificate is served there, that it covers the address ArgoCD will use, and
that it verifies against the cluster's own CA. A missing SAN is the usual reason direct access fails, so `--dry-run` is
worth running against a new cluster before anything is created — it changes nothing and still answers that question,
along with when the certificate expires.

`k2a-token-sync bootstrap --help` lists the rest.

### Keeping the objects in git

`--print` provisions the cluster and stores the credential as usual, then writes the manifest to stdout instead of
applying it:

```bash
k2a-token-sync bootstrap --cluster standalone-1 \
  --endpoint standalone-1.example.com:6443 \
  --from-kubeconfig ./standalone-1.kubeconfig \
  --print > clusters/standalone-1.yaml
```

Commit that and let ArgoCD apply it, or apply it yourself. The value is review and audit — the object is spec-only, so a
repository is a reasonable home for it. Note that the declaration alone does nothing until the credential exists, which
bootstrap has already handled by the time the manifest is printed.

### Provisioning it yourself

The CLI is a convenience, not a requirement. Anything with administrative access can prepare a cluster by satisfying the
same contract:

- create the `argocd-manager` ServiceAccount and bind it to `cluster-admin`;
- create the `k2a-token-sync` ServiceAccount and bind it to a ClusterRole with the four rules above;
- mint a token for the second one;
- write `<name>-credentials` in k2a-token-sync's namespace with keys `token`, `ca.crt` and `expires-at`.

`expires-at` is RFC 3339 and may be omitted; the deadline is then treated as unknown and replaced with a known one at the
next renewal. That contract is worth implementing wherever your clusters are built, so a new cluster arrives ready and no
administrative credential ever moves.

### Never manage the credential Secrets declaratively

Do **not** create `<name>-credentials` with External Secrets, an ArgoCD Application, Sealed Secrets, or anything else that
reconciles toward a stored value. Those Secrets belong to k2a-token-sync, which rewrites each one daily to keep
the remaining lifetime within a day of the full `selfTokenTTL`.

On the ArgoCD side the same mistake is caught rather than silent: k2a-token-sync records a digest of the credential it
published and compares it against what the apply response reports on every pass, so a `cluster-<name>` Secret written
over by anything else is noticed within one pass and replaced with a credential this tool can actually renew. The Secret
below has no such protection, because it is the one k2a-token-sync reads rather than writes blind.

A second writer turns that into a silent fault. Every renewal is undone on the next reconcile, so the credential stops
advancing; about ninety days later the stored copy expires and gets pushed over a working token, and k2a-token-sync locks
itself out of the cluster. The symptom appears three months after the cause.

There is also nothing to gain. The credential is disposable: it is regenerated in seconds by re-running bootstrap, and
k2a-token-sync replaces it daily regardless, so a vaulted copy is stale almost immediately. What deserves protecting is
the administrative kubeconfig bootstrap consumes — which you should already manage somewhere.

## Deployment paths

Two methods work, and they differ only in who applies the chart.

**Helm or Ansible, then bootstrap.** Install the chart, then run bootstrap once per cluster. Nothing per-cluster lives in
git; the inventory lives in the API.

**ArgoCD owns the chart, then bootstrap.** An Application deploys the chart from git — CRD, RBAC, Deployment, no secrets,
fully declarative. Bootstrap still runs out-of-band, because the credential cannot be in git.

So the part GitOps cannot express is exactly **one credential per cluster**, and the natural place to create it is
wherever administrative access already exists: the automation that builds the cluster. Implement the contract above in
that automation and there is no separate onboarding step at all.

If you also want the ClusterConnections in git, add a second Application for them and use `--print`. That is optional
rather than implied: an Application that renders only the chart never prunes bootstrap-created objects, because ArgoCD
prunes only what it tracks.

## Health and observability

| Endpoint | Purpose |
| --- | --- |
| `/livez` | Liveness — fails if the reconciliation loop has stalled |
| `/readyz` | Readiness — passes once every cluster in the inventory has reconciled in *this* process |
| `/status` | JSON detail per cluster, including observed certificate expiry. Carries no credential material |
| `/metrics` | Prometheus metrics: per-cluster deadlines, plus the standard Go and process collectors |

### What liveness actually checks

Clusters reconcile on their own schedules, up to four at a time, and the poll that starts a pass never waits for it to
finish. That is about more than throughput. A cluster whose API server accepts a connection and then stops answering
used to hold the whole loop for its five-minute timeout: nothing else became due, an edit went unnoticed, and `/livez`
reported health the entire time, because it excused any pass in progress. The one state a restart would have fixed was
the one the probe could not see.

`/livez` fails on either of two things now — no poll has completed for five minutes, or some pass has outlived its own
five-minute timeout by a minute, which means it is not slow but wedged, since every pass runs under that deadline. The
first window is several polls wide on purpose: an API server that is briefly unreachable is not something restarting
this pod fixes, so it must not cause one.

Nothing is dropped when more than four clusters are due at once; the rest queue. Raising that bound is the answer if a
fleet ever grows large enough for the queue to matter. A queued pass rechecks its cluster when its turn comes rather
than trusting what was true when it was queued — in between, another ClusterConnection may have claimed the same Secret,
or the object may have been deleted, and both of those are decided by refusing to write.

`/readyz` deliberately means "reconciled by *this* process", which is why the emphasis is in the table above. Every
object carries a `Ready` condition from whichever process last wrote it, so a restarted pod finds `Ready=True`
everywhere before it has done anything at all — and reporting readiness off the back of that would tell a rollout the
new pod is serving while every pass was still queued. The same applies after an edit: the condition on the object
describes the spec as it was before the edit, so readiness drops until the new one has actually been through a pass.

Per-cluster state is on the objects themselves, which is the readable view:

```console
$ kubectl -n k2a-token-sync get ccon
NAME           ENDPOINT         READY   SELF VALID   TOKEN EXPIRES          SELF EXPIRES           CERT DAYS   AGE
downstream-1   10.0.0.10:6443   True    True         2026-08-31T16:25:26Z   2026-10-30T16:25:26Z   364         12d
standalone-1   10.0.1.10:6443   True    False        2026-08-24T09:12:44Z   2026-10-30T17:01:09Z   211         12d
standalone-2   10.2.0.10:6443   False   <none>       <none>                 <none>                 <none>      2m
```

Both expiries are timestamps rather than "in 29 days" for a reason worth knowing if you ever add a column: kubectl
renders a date column as time *elapsed*, which for anything in the future is negative and prints as `<invalid>`.

`SELF VALID` is the one to watch when everything else looks fine: `standalone-1` above is serving ArgoCD perfectly well
while k2a-token-sync has lost the ability to renew its own credential, which is a problem with a deadline rather than a
symptom.

When a cluster is not `Ready`, three things answer it, in this order. The listing above says which cluster and for how
long. `kubectl describe ccon <name>` gives the reason — `AwaitingCredential` for one that has not been bootstrapped,
`CredentialExpired` for one whose own credential lapsed, `CertificateInvalid` for an endpoint whose certificate cannot
work, `InvalidSpec` or `SecretNameConflict` for one that is not being reconciled at all — alongside `lastAction`, which
says what the most recent pass actually did, or why there was not one. The logs then carry the underlying error, usually
the downstream API server's own words.

Generated cluster Secrets also carry annotations you can read with `kubectl`:

```bash
kubectl -n argocd get secret cluster-downstream-1 \
  -o jsonpath='{.metadata.annotations}' | jq
```

`k2a-token-sync.io/token-expires-at`, `k2a-token-sync.io/serving-cert-expires-at` and `k2a-token-sync.io/cluster`.

There is deliberately no last-sync annotation, and leaving it out is what makes the rest of this work. Every value
written here describes something configured or observed, so a pass over an unchanged cluster writes nothing at all: the
apply finds no difference, the Secret's `resourceVersion` holds still, and ArgoCD sees no event. A timestamp of the
last pass would change every time, making every pass a write — which is what would force the interval back to something
long. When a pass last ran is recorded in the ClusterConnection's `status.lastSyncTime` instead.

Runtime telemetry is sampled every second. Every ten minutes one structured log reports the current and maximum values
seen during that interval for uptime, goroutines, OS threads, allocated and in-use heap, in-use stack, memory reserved by
the Go runtime and live heap objects. The interval maximum resets after it is logged. That log is for reading after the
fact, when there is no query engine to ask.

`/metrics` deliberately does not restate any of it. The standard Go and process collectors registered there export every
one of those numbers already, and export them read at scrape time rather than from a sample up to a second old. What the
endpoint adds is what nothing else knows — one series per cluster, labelled `cluster`:

| Metric | Meaning |
| --- | --- |
| `k2a_token_sync_cluster_ready` | 1 when ArgoCD holds a current registration, 0 otherwise |
| `k2a_token_sync_cluster_token_expiration_timestamp_seconds` | when the credential published to ArgoCD expires |
| `k2a_token_sync_cluster_self_credential_expiration_timestamp_seconds` | when this tool's own credential expires |
| `k2a_token_sync_cluster_serving_cert_expiration_timestamp_seconds` | when the observed serving certificate expires |
| `k2a_token_sync_cluster_last_sync_timestamp_seconds` | when a pass last *succeeded* |

Deadlines are absolute timestamps rather than seconds remaining, because remaining is only true at the instant it is
scraped. Alert on the difference — `k2a_token_sync_cluster_token_expiration_timestamp_seconds - time() < 86400` is
correct whenever it runs. A deadline that is not yet known is **absent rather than zero**: zero reads as 1970, which
would fire every expiry alert at once for a cluster that has simply never published.

The chart renders a ClusterIP Service, so the endpoint has an address that survives a rollout:
`http://<release>.<namespace>.svc.cluster.local:8080/metrics`. That is the whole of the scrape setup, and it is
deliberate — there are no `prometheus.io/scrape` annotations and no ServiceMonitor. Both exist to feed Prometheus's own
service discovery, so both are inert for a collector that takes a static endpoint list, and shipping both would invite
two scrapers finding the same pod twice. Under prometheus-operator, a ServiceMonitor selecting this Service is the
addition to make.

Logs are JSON via `log/slog`. Credential material is never logged.

### Events

`kubectl describe ccon <name>` is the first place most people look, so the handful of things that are genuinely *events*
rather than states are recorded there too.

| Reason | Type | When |
| --- | --- | --- |
| `CredentialReissued` | Normal | A new credential was published for ArgoCD, carrying the same reason as `lastAction` |
| `IdentityRestored` | Warning | ArgoCD's downstream ServiceAccount or its binding was missing and has been recreated |
| `RenewalMintFailed`, `RenewalUnverified`, `RenewalNotStored` | Warning | This tool cannot renew its own credential |
| `RenewalRecovered` | Normal | It can again, after one of those |
| `InvalidSpec`, `SecretNameConflict` | Warning | The object is not being reconciled, and why |
| `ReconciliationResumed` | Normal | That reason is resolved |

The reasons are the condition reasons wherever one already fits, so the Events section and the `Ready` condition never
name one situation two different ways.

**An unchanged pass records nothing**, and neither does an ordinary pass failure. The great majority of passes write
nothing at all — that is what makes a five-minute cadence affordable — and an event every five minutes per cluster would
drown the rare ones while putting API writes back into a loop built to avoid them. A renewal that is failing records one
event when it starts and another if the reason changes, not one per pass; `Ready=False` for an unreachable endpoint or an
expiring certificate records none, because the condition already carries it and backoff would repeat it.

Events expire from the API server on its own schedule, usually an hour. They are where you find the sequence, not where
state lives: conditions and `lastAction` remain authoritative for how a connection stands now.

## Security notes

k2a-token-sync holds no cluster-scoped permissions on the cluster it runs in. Its objects are namespaced and only its own
namespace is listed, so a Role suffices everywhere. In that namespace it also holds `create` on Events, and only
`create`: each one is a new object, and it neither reads them back nor aggregates them into a series.

**In ArgoCD's namespace it holds `create` and `patch` on Secrets, and nothing else.** It cannot read — not the Secrets it
writes, and not ArgoCD's own. It learns the result of its own writes from what an apply returns, and keeps everything
else in each ClusterConnection's status.

Those two verbs are namespace-wide because they have to be: cluster names are not known when the Role is created, which
is the whole point of an inventory that changes at runtime, and RBAC cannot scope `resourceNames` by prefix. Be
clear-eyed about what that permits — `patch` on any Secret in that namespace means k2a-token-sync *could* overwrite ArgoCD's
repository credentials. Two things bound it. `secretName` must begin with `cluster-`, so no connection can be aimed at
them; and the absence of `delete` means nothing there can be removed.

The cost of that posture is stated under [Removing a cluster](#removing-a-cluster): with no `list`, k2a-token-sync cannot
notice an orphaned Secret.

Downstream, the `k2a-token-sync` identity is granted only what it needs: get/create ServiceAccounts, create ServiceAccount
tokens, get/create ClusterRoleBindings, and read the `kube-root-ca.crt` ConfigMap. It holds no access to Secrets at all.

Be equally clear-eyed there. The right to mint a token for a `cluster-admin` ServiceAccount is equivalent to
`cluster-admin` by one hop — the narrow grant is for auditability and to avoid blanket Secret access, not because the
identity is unprivileged. What the design genuinely achieves is that **no non-expiring credential exists anywhere in
the system**: ArgoCD's token lasts 30 days and k2a-token-sync's own 90, both renewed automatically. That bounds the
value of any leak, which a permanent `cluster-admin` JWT does not.

An existing `ClusterRoleBinding` that points at a different role, or omits the expected ServiceAccount, is reported as
an error rather than silently rewritten — an unannounced privilege change is not something this tool should make.

## Building

```bash
# Local image build
docker build -t k2a-token-sync:latest .

# Tests and linting
go test ./...
golangci-lint run

# Regenerate and validate dependency license notices
./hack/gen-notices.sh

# image.tag is required, so the chart needs one to render — any value will do
# when you are only checking the templates
helm lint charts/k2a-token-sync --set image.tag=v0.11.0
```

The versions in the examples above are illustrative — deploy whatever is current, which is listed on the
[releases page](https://github.com/krisiasty/k2a-token-sync/releases).

Releases are published to `ghcr.io/krisiasty/k2a-token-sync` via GitHub Actions using GoReleaser. Multi-arch images
(`linux/amd64`, `linux/arm64`) are built and published as a combined manifest, alongside archives for running the
`bootstrap` subcommand from a workstation:

- `linux` and `darwin` — tar.gz archives
- `windows` — ZIP archives (`.exe` executable)

Every binary embeds the dependency license texts and attribution notices generated from its released build graphs.
Run `k2a-token-sync licenses` to print them. Release archives also contain `LICENSE`, `NOTICE` and
`THIRD_PARTY_NOTICES` as separate files.

Everything that release path pulls in is pinned to something that cannot move: actions to full commit SHAs, images to
digests, and downloaded release tools to exact versions and committed SHA-256 checksums. A tag or release asset can be
replaced by its owner, and this is a binary that holds `cluster-admin` on every cluster it manages — so what goes into it
should only ever change in a commit somebody reviewed. Dependabot proposes action and Dockerfile image bumps weekly;
release-tool versions and checksums, and the BuildKit image pin in the workflow, are bumped by hand.

### Windows users

Windows archives are published as `.zip` files containing `k2a-token-sync.exe`. To use the Windows binary:

1. Download the archive for your architecture (`windows_amd64` or `windows_arm64`)
2. Verify the checksum against the `checksums.txt` file in the release
3. Extract the ZIP archive
4. Run the executable from PowerShell or Command Prompt

Example PowerShell session:

```powershell
# Download the archive (replace vX.Y.Z with the actual version)
Invoke-WebRequest `
  -Uri "https://github.com/krisiasty/k2a-token-sync/releases/download/vX.Y.Z/k2a-token-sync_X.Y.Z_windows_amd64.zip" `
  -OutFile "k2a-token-sync.zip"

# Verify checksum (compare with checksums.txt)
$hash = Get-FileHash -Algorithm SHA256 -Path "k2a-token-sync.zip"
echo $hash.Hash

# Extract the archive
Expand-Archive -Path "k2a-token-sync.zip" -DestinationPath "k2a-token-sync"

# Run the bootstrap command
.\k2a-token-sync\k2a-token-sync.exe `
  bootstrap --cluster my-cluster `
  --endpoint my-cluster.example.com:6443 `
  --from-kubeconfig C:\path\to\kubeconfig
```

**Note:** The Windows executable is an administrative CLI client for running the `bootstrap` subcommand. It is not a
supported Windows container or controller deployment — the controller remains Linux-only.

### Cutting a release

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

That is the whole release. GoReleaser builds and publishes the images, archives and GitHub release. Then set
`image.tag` to the new version where you deploy — your values file, or the ArgoCD Application — and the upgrade
rolls out.

Before tagging, check whether anything under `charts/` changed since the last release and bump the chart's own
`version` if so. It moves with the chart's contents rather than with the application, so a release that adds a template
and leaves the version alone ships two different charts under one number — which is the one thing the version is there
to prevent. `git diff <last-tag>..main -- charts/` answers it in a line.

Nothing in ordinary CI exercises that path, so after changing anything in it, run the **Release** workflow by hand — on
the branch that changed it, before merging rather than after:

```bash
gh workflow run Release --ref my-branch
```

That takes about four minutes and publishes nothing: the same checkout and the same pinned tools, the same binaries and
the same multi-arch images, built by a job with no write permissions and no registry login. It is worth doing because a
release that fails half way through is the expensive kind — GoReleaser pushes images before it creates the GitHub
release, so an interrupted one leaves `ghcr.io` tags, including `latest`, pointing at a version that has no release
behind it.

### Versioning

The chart and the application are versioned independently, which is what Helm's two fields are for:

- **Chart `version`** moves only when the chart changes: a new resource, a renamed or removed value, a changed default.
  Ship five application releases without touching the templates and it stays put.
- **The application version is `image.tag`**, supplied in deployment values. There is deliberately no `appVersion` in
  `Chart.yaml`.

That separation matters rather than being cosmetic. If `image.tag` fell back to chart metadata, the deployed version
would live in a file inside the tagged tree — and since the tag is what triggers the build, the chart could never name
the image that release produced. Keeping it in deployment values means it is set after the release, and the version you
are running is stated explicitly instead of inferred. Rendering fails with an actionable message if it is missing, so
the requirement cannot be discovered as a mystery `ImagePullBackOff`.

## Limitations

- Serving certificates are observed, never rotated. Reissuing one needs node access, so it belongs to whatever manages
  the cluster.
- One replica, `Recreate` strategy. Two instances reconciling the same clusters would race to publish credentials.
- The CA bundle is never rotated. Rotating a cluster CA is a deliberate, disruptive operation and out of scope.
- Token lifetime is capped by the downstream API server's `--service-account-max-token-expiration`. k2a-token-sync
  logs a warning when it is granted materially less than it requested — which matters more for its own credential than
  for ArgoCD's, since that lifetime is the outage it can survive. Both credentials are then renewed against what was
  actually granted rather than what was asked for, so a capped cluster works; it just has a shorter downtime budget.
- k2a-token-sync cannot detect a generated Secret left behind by a cluster removed while it was down, because it holds no
  `list` permission in ArgoCD's namespace. Cleanup is a documented step.
- ArgoCD's credential is exercised when it is minted, never afterwards. Holding no read access in ArgoCD's namespace,
  k2a-token-sync cannot recover a published token to re-test it — that is the same restriction that makes the design
  safe, and it is not worth trading away. Later breakage is caught indirectly instead, by checking the identity behind
  the token on every pass.
- Whether ArgoCD itself can reach an endpoint is not observable from here. The verification above runs from
  k2a-token-sync's pod, which is usually the same network path but not necessarily: NetworkPolicies, egress rules or a
  service mesh can differ between workloads in the same cluster.
