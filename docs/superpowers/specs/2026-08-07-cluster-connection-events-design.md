# Kubernetes Events for what a pass did

Design for [#47](https://github.com/krisiasty/k2a-token-sync/issues/47).

## Problem

`kubectl describe ccon` has an empty Events section. Conditions and `lastAction` describe the object as it stands now;
the sequence that produced it — credential reissued, downstream identity restored, renewal failed and then recovered —
exists only in the daemon's logs. The events worth having are the rare ones, and those are exactly what someone is trying
to reconstruct after the fact.

## What is emitted

Six events, hung off the places that already log these facts. Four reuse a condition reason from `api/v1alpha1`
verbatim; the other four names are added to that same package rather than to a vocabulary of their own.

| Event | Type | Reason | Site |
| --- | --- | --- | --- |
| credential reissued | Normal | `CredentialReissued` (new) | `reconcile`, beside the existing success log |
| downstream identity restored | Warning | `IdentityRestored` (new) | `reconcile`, where `repairs.Any()` warns |
| renewal failed | Warning | `RenewalMintFailed` / `RenewalUnverified` / `RenewalNotStored` | `maintainSelfCredential` |
| self-credential renewal recovered | Normal | `RenewalRecovered` (new) | `maintainSelfCredential` |
| verdict written | Warning | `InvalidSpec` / `SecretNameConflict` | `scheduler.reportRejected` |
| verdict cleared | Normal | `ReconciliationResumed` (new) | `scheduler.updateSchedule` |

The renewal-failure event fires on transition only, and again if the failure reason changes. Emitting it every pass would
be one event per cluster per five minutes for as long as renewal stayed broken, which buries the one that said it
started. `verdictFor` and the verdict-cleared transition are already edge-triggered, so those are transition-only for
free.

Nothing is emitted for an ordinary pass failure — `EndpointUnreachable`, `CertificateInvalid`, `CredentialRejected`.
Those are states, they repeat under backoff, and the `Ready` condition already carries them. Nothing at all is emitted by
an unchanged pass, which is what keeps the five-minute cadence free of API writes.

## Architecture

**`internal/events`** holds a `Recorder` that writes `corev1.Event` objects into k2a-token-sync's own namespace with the
typed client already in hand.

Deliberately not `k8s.io/client-go/tools/record`. That needs a `runtime.Scheme` with `ClusterConnection` registered,
which `api/v1alpha1/doc.go` refuses on purpose, and a broadcaster goroutine whose aggregation exists to tame recorders
that fire constantly. Neither earns its keep for six events that fire on transitions.

The legacy core/v1 event shape — `source` plus `firstTimestamp`/`lastTimestamp`/`count`, rather than `eventTime` with a
reporting controller and an action — because that is what the API server validates without further required fields and
what `kubectl describe` renders. The series machinery the newer shape exists for has nothing here to aggregate.

**Object reference.** `kubectl`'s Events section field-selects on `involvedObject.uid`, so an event carrying only a name
and a namespace is accepted and then never shown by the one command it exists for. The recorder resolves the reference
through a one-method `ObjectReferencer`, implemented by `inventory.Client.Reference` as a single `Get`. That keeps every
existing signature intact and costs one extra read per event, which is nothing at this frequency. A failed lookup drops
the event with a warning.

**Seam.** `internal/reconcile` and the scheduler each declare a two-method recorder interface next to the code that uses
it, matching how `clusterInventory` and `clusterReconciler` are already declared. No method returns an error: an event
records work that has already happened, so failing to write one must not be able to fail the work.

**Event names** are `<cluster>.<reason>.<hex nanos>`. Unique by construction — one cluster reconciles serially and emits
at most one event per reason per pass — and deterministic, which `generateName` is not, since the fake clientset does not
implement it.

## RBAC

One rule added to the existing namespace `Role`:

```yaml
- apiGroups: [""]
  resources: ["events"]
  verbs: ["create"]
```

Namespace-scoped, in the namespace the objects live in. `create` only: each event is a new object, and nothing here reads
or aggregates them.

## Testing

- An unchanged pass emits nothing, asserted against a full `Reconciler.Cluster` pass — a stored credential, a downstream
  fake, and a real TLS listener whose certificate is signed by the test CA, so the serving-cert probe is genuinely
  exercised rather than stubbed.
- The same harness, with the token past half its lifetime, emits exactly one `CredentialReissued`.
- A deleted ServiceAccount emits `IdentityRestored` as a Warning, and the message distinguishes it from a recreated
  binding, because only the former invalidates ArgoCD's token.
- Renewal failure emits once across repeated failing passes, and again when the reason changes; recovery emits
  `RenewalRecovered` only when a failure preceded it.
- The verdict events fire on the transition and not on the polls that follow, using the existing `fakeInventory`.
- The recorder itself: the written event carries the UID, kind, namespace and type; a reference that cannot be resolved
  is dropped rather than propagated.

## Documentation

A short Events subsection in the README beside the existing verdict discussion, plus the `events`/`create` line in the
RBAC section, which documents every other permission the tool holds.
