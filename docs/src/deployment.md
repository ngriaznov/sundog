# Deployment

sundog runs inside your service, so deploying it means deploying your
service with two extra ports open and a way for nodes to find each other.

## Discovery

| Mechanism | Default | What it does | Use it on |
|---|---|---|---|
| `Mdns` | yes | registers `_sundog._udp.local.` and browses for peers continuously | a LAN or anywhere multicast works |
| `Static` | when `.seeds(..)` is called, or `SUNDOG_SEEDS=host:port,host:port` is set and nothing else is configured | dials a fixed seed list, re-resolving hostnames every 30 seconds | VPCs, Docker networks, Kubernetes through a headless Service's name, tests |
| `DnsSrv` | no, `.discovery(DnsSrv::new(..))` | polls SRV records for a name every 30 seconds, falling back to A and AAAA records | Kubernetes, private DNS zones |

Seeds only bootstrap. A node that reaches one member learns the rest of
the cluster through gossip. Discovery keeps running after startup, so a
cluster whose every node restarted at once finds itself again. `Mdns` runs
its own daemon thread, which stops when the cluster or observer stops, or
when its build fails.

## Ports

A node binds two ports: gossip over UDP and the data plane over TCP. Both
default to port 0, a free port chosen at startup, which suits a LAN with
mDNS: each node announces the gossip port it bound, and, when it has an
explicit `advertise_ip` or binds a specific gossip IP, that address alone;
with neither, it announces the addresses of its interfaces. Seeds and DNS records
name a gossip port instead, so every node they name must bind that port.
With the default ports nothing listens at the address a seed names, the
nodes never reach each other, and each runs as a cluster of one, with no
error. Fix the gossip port on every node a seed list or DNS record names,
and both ports wherever a firewall, security group or network policy sits
between nodes:

```rust
{{#include ../cookbook/src/deploy.rs:fixed_ports}}
```

Or fix them from the environment, with no code: `SUNDOG_GOSSIP_PORT` and
`SUNDOG_DATA_PORT` give a `Cluster` the gossip and data-plane ports its
config leaves at 0. A port the config sets stays, with a warning when the
variable names another, and `build` fails on a value that is not a port
number. An `Observer` does not read them and binds the port its config
names.

Peers learn each other's data-plane port through gossip, so a free one
works wherever nodes reach each other on any port.

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

mDNS does not cross Docker's default bridge network, and most Wi-Fi networks
block it too with access-point isolation. Give each container its seeds
through `SUNDOG_SEEDS` or `.seeds(..)`. A container the seeds name fixes its
gossip port at the number they name, through `SUNDOG_GOSSIP_PORT` or the
config. With published ports, fix both ports, publish each at the number the
container binds, since a node advertises the ports it binds, and set
`advertise_ip` to the address other containers dial.

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
  publishNotReadyAddresses: true
  selector:
    app: myservice
  ports:
    - name: gossip
      port: 7946
      protocol: UDP
```

`publishNotReadyAddresses` lists a pod in the Service's DNS before its
readiness check passes, so pods that start together find each other while
their caches warm instead of each warming alone. The Service carries gossip
only, so a pod it lists early takes no client traffic.

The pod spec then sets everything sundog needs, and the service calls
`Cluster::builder("prod").build()` with no configuration:

```yaml
containers:
  - name: myservice
    env:
      - name: SUNDOG_SEEDS
        value: myservice-gossip.my-ns.svc.cluster.local:7946
      - name: SUNDOG_GOSSIP_PORT
        value: "7946"
      - name: SUNDOG_DATA_PORT
        value: "7947"
    ports:
      - name: gossip
        containerPort: 7946
        protocol: UDP
      - name: data
        containerPort: 7947
        protocol: TCP
```

`SUNDOG_SEEDS` names the headless Service, whose name resolves to every pod
it lists. The node re-resolves it every 30 seconds, so a pod that starts
later is found too. A network policy between the pods allows both ports.

Alternatively, in code, `DnsSrv` polls the Service's SRV records, with the
gossip port as the fallback so plain A records suffice:

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
the same check at `GET /readyz`, with `GET /healthz` for liveness. Open the
telemetry port to scrapers and probes only: the listener serves at most 64
connections at once and closes a connection over that, so a client that holds
connections open takes the slots a scraper needs.

On SIGTERM, leave the cluster after the HTTP server drains, so peers learn
the node left on purpose:

```rust
{{#include ../cookbook/src/deploy.rs:shutdown}}
```

## Mutual TLS

With the `tls` feature, `ClusterBuilder::tls` wraps every data-plane
connection in mutual TLS, whether it comes before or after
`ClusterBuilder::config`, and takes precedence over `ClusterConfig::tls`. A
`TlsConfig` holds the node's certificate chain, its private key and the root
CAs it trusts. Every certificate carries the fixed name
`sundog-mesh.internal` as a DNS subject alternative name instead of the
node's IP, so one certificate profile serves every node and survives address
changes. A TLS node and a plaintext node refuse each other's connections
instead of downgrading. Gossip itself stays
plaintext UDP and carries membership only: node ids, addresses and cache
modes, never keys or values.

## Over a WireGuard network

A WireGuard network encrypts and authenticates every packet between its
hosts, gossip and the data plane alike, with no change to sundog: kernel
WireGuard between hosts, a Tailscale network, or a Kubernetes network
plugin's WireGuard mode, such as Cilium's or Calico's, which encrypts pod
traffic between Kubernetes nodes. It is the alternative to the `tls`
feature where gossip must not cross the network in the clear, since `tls`
covers the data plane only.

WireGuard authenticates hosts, not sundog nodes: any process on a host in
the network reaches both ports. Add the `tls` feature where each node must
prove itself to its peers.

Between hosts, peers must dial each other's tunnel address. The default
bind address advertises the address on the route to the internet, which on
most hosts is outside the tunnel, so bind both ports to the host's
WireGuard address or set `advertise_ip` to it, and name tunnel addresses or
their DNS names in the seeds. WireGuard carries no multicast, so mDNS finds
nothing across it: seed through `SUNDOG_SEEDS` or `.seeds(..)`. A network
plugin's WireGuard mode keeps pod IPs, so the Kubernetes setup above needs
no change.

## The data-plane port

Open the data-plane port between the service's instances and nowhere else.
A plaintext node takes a peer's word for who it is in the hello that opens a
connection, so a network that carries other traffic needs the `tls` feature:
with it, a connection presents a certificate from the shared root CA before
its hello is read.

A node limits the connections that have not yet sent a hello. A connection
has 10 seconds, plus any wait for the node to schedule its handler, to finish
its TLS handshake, when the node runs TLS, and send a complete hello. The
node closes it after that. Until the hello arrives, a frame is at most
1 KiB. At most 512 connections from addresses that are not a peer's wait for
their hello at once, and the node closes any further one as it accepts it. A
known peer's connections are not capped. A node whose peer table is empty, as
a node that has just started is, and a peer behind NAT or an `advertise_ip`
override that dials from an address other than the one it advertises, meet the
cap like any other source.

A connection from an address that is not a peer's holds a slot among at most
2048 such open connections for as long as it lives, and the node closes any
further one as it accepts it. Such a connection that sends no frame for 60
seconds after its hello is closed, and a peer behind NAT redials its idle
broadcast link at that rate. A connection from a peer's address waits for a
frame without limit, as an idle broadcast link does. Every connection that
stops inside a frame, its length prefix received and the rest not arriving,
for 30 seconds is closed; the 30 seconds restart whenever more of the frame
arrives. The limits are fixed.
`sundog_accept_dropped_total` counts the closes; see
[Operations](operations.md#data-plane-connections-close-before-the-hello).

Set the process's descriptor limit well above 512 plus the descriptors the
peer connections and the spill files use: `LimitNOFILE=65536` under systemd,
for example, or `ulimit -n 65536` in a shell.

## Rolling upgrades

Every node states its wire protocol version in the hello that opens each
connection and in its gossip state. A node answers a peer only with
message kinds that peer's version decodes, so a release interoperates with
the one before it, and a cluster upgrades one node at a time with
replication and repair running throughout. A container test runs the
previous release's node against the current one in both directions on
every change.

The hello also names the cluster, and a node drops a connection from
another cluster's node. Addresses move between clusters sharing a network,
as pod IPs do in Kubernetes: a node still dialing a departed peer's address
reaches whatever holds it now, and the check keeps its writes out of that
node's caches. A node of a release before the check names no cluster and
is accepted.

A `Distributed` cache ranks whole buckets while any eligible node runs a
release before part ownership, and switches to ranking each part once the
last such node leaves. Most parts change owners at that switch, so the
last step of that upgrade runs one large rebalance: every lost part is
served through its disown grace and handed to its new owners before it is
dropped. Take that step outside peak load.

A `Distributed` cache holds a forwarded write that an owner refuses for
clock skew, and applies it once the owner's clock is within `max_clock_skew`
of its stamp (see [guarantees](guarantees.md)). An owner on a release without
that hold drops such a write: the caller saw `Ok`, an insert is never stored,
a remove leaves the key live and a merge contribution is gone, and the old
owner's `sundog_clock_skew_rejected_total` is the only sign. The hold exists
from 0.6.8. A release before 0.6.10 holds a refused forward only when its
writer owns none of the part in the owner's view, and drops one that a node
relays, such as a closing owner's hand-off of the forwards it held, when the
owner's view lists the relaying node as an owner of the part. The sender does
not check the owner's release, so a mixed cluster keeps the older behaviour for
these writes. A co-owner on the current release holds its own copy of each
forward and offers it to the old owner by anti-entropy once the old owner's
clock is within the bound; a write is lost only when every owner of its part
is on an older release, or the current one that holds it stops before it
applies. Keep node clocks within `max_clock_skew` of each other from the first
node's upgrade to the last, and wait for `sundog_clock_skew_held` to read 0
before stopping a node for an upgrade or a rollback: the hand-off of its held
forwards goes to owners that may be older and drop it.
No other write is affected: `Replicated` writes, anti-entropy pushes and
transfers to an older node keep the sender's copy.

Roll one node at a time. For a `Distributed` cache, wait until
`Cluster::health()` on the restarted node reports the cache warm, meaning
its owned parts have landed, before stopping the next node, so no part
loses two owners inside one rebalance window. Readiness alone does not
cover this: it waits only on `Replicated` caches.
