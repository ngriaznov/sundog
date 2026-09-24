# Deployment

sundog runs inside your service, so deploying it means deploying your
service with two extra ports open and a way for nodes to find each other.

## Discovery

| Mechanism | Default | What it does | Use it on |
|---|---|---|---|
| `Mdns` | yes | registers `_sundog._udp.local.` and browses for peers continuously | a LAN or anywhere multicast works |
| `Static` | when `.seeds(..)` is called, or `SUNDOG_SEEDS=host:port,host:port` is set and nothing else is configured | dials a fixed seed list, re-resolving hostnames every 30 seconds | VPCs, Docker networks, tests |
| `DnsSrv` | no, `.discovery(DnsSrv::new(..))` | polls SRV records for a name every 30 seconds, falling back to A and AAAA records | Kubernetes, private DNS zones |

Seeds only bootstrap. A node that reaches one member learns the rest of
the cluster through gossip. Discovery keeps running after startup, so a
cluster whose every node restarted at once finds itself again.

## Ports

A node binds two ports: gossip over UDP and the data plane over TCP. Both
default to port 0, a free port chosen at startup, which suits a LAN with
mDNS. Anywhere a firewall, security group or network policy sits between
nodes, fix both:

```rust
{{#include ../cookbook/src/deploy.rs:fixed_ports}}
```

Seeds and DNS records name the gossip port only. Peers learn each other's
data-plane port through gossip.

## The advertised address

A node tells its peers which IP to dial. With the default bind address it
probes the interface the OS routes outbound traffic through and advertises
that address. On a LAN, in a VPC and in a Kubernetes pod, that is the
right address, and nothing needs setting.

Set `ClusterConfig::advertise_ip` when peers must dial a different address
than the one the node sees on its own interface: behind NAT, behind a
container port mapping, or when the process binds a specific address that
peers do not route to. The override covers both ports.

## Docker

mDNS does not cross Docker's default bridge network. Give each container
its seeds through `SUNDOG_SEEDS` or `.seeds(..)`, and with published ports
set `advertise_ip` to the address other containers dial.

## A cloud VPC

AWS, GCP and Azure VPCs route unicast and drop multicast. Fix the ports as
above, open both between the service's instances with a self-referencing
security group rule, and seed through a few stable private addresses:

```rust
{{#include ../cookbook/src/deploy.rs:vpc}}
```

`DnsSrv` against a name in a private DNS zone works the same way. Several
availability zones behave as one network with a few milliseconds of extra
latency.

A peered VPC in another region routes too, at tens of milliseconds of round
trip. The failure detector defaults target detection within five seconds
on a LAN, so raise `gossip_interval`, `phi_threshold` and `fetch_timeout`
for a cluster that spans regions. [Tuning](tuning.md) lists each one.

## Kubernetes

sundog runs inside your service's pods. Add a headless Service that
selects the same pods and names the gossip port:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: myservice-gossip
spec:
  clusterIP: None
  selector:
    app: myservice
  ports:
    - name: gossip
      port: 7946
      protocol: UDP
```

Point `DnsSrv` at it, with the gossip port as the fallback so plain A
records suffice, and declare both fixed ports as container ports:

```rust
{{#include ../cookbook/src/deploy.rs:kubernetes}}
```

The pod IP is what the node advertises. Two hooks connect sundog to the
pod's lifecycle. Readiness holds traffic off a pod until its `Replicated`
caches have pulled their snapshot:

```rust
{{#include ../cookbook/src/deploy.rs:readiness}}
```

With the `prometheus` feature, `ClusterBuilder::prometheus_listen` serves
the same check at `GET /readyz`, with `GET /healthz` for liveness.

On SIGTERM, leave the cluster after the HTTP server drains, so peers learn
the node left on purpose:

```rust
{{#include ../cookbook/src/deploy.rs:shutdown}}
```

## Mutual TLS

With the `tls` feature, `ClusterBuilder::tls` wraps every data-plane
connection in mutual TLS. A `TlsConfig` holds the node's certificate
chain, its private key and the root CAs it trusts. Every certificate
carries the fixed name `sundog-mesh.internal` as a DNS subject alternative
name instead of the node's IP, so one certificate profile serves every
node and survives address changes. A TLS node and a plaintext node refuse
each other's connections instead of downgrading. Gossip itself stays
plaintext UDP and carries membership only: node ids, addresses and cache
modes, never keys or values.

## Rolling upgrades

Every node states its wire protocol version in the hello that opens each
connection and in its gossip state. A node answers a peer only with
message kinds that peer's version decodes, so a release interoperates with
the one before it, and a cluster upgrades one node at a time with
replication and repair running throughout. A container test runs the
previous release's node against the current one in both directions on
every change.

Roll one node at a time. For a `Distributed` cache, wait until
`Cluster::health()` on the restarted node reports the cache warm, meaning
its owned buckets have landed, before stopping the next node, so no bucket
loses two owners inside one rebalance window. Readiness alone does not
cover this: it waits only on `Replicated` caches.
