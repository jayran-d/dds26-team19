# Gateway Report

## Short Verdict

The current gateway is acceptable as a simple local ingress, but it is not
fully kill-proof, not fully recovery-proof, and not horizontally scalable.

It is stateless, so it is easy to restart. That is good.
It is also a single Nginx container with static upstream resolution. That is
the main weakness.

## What the Nginx Error Means

Example:

```text
upstream prematurely closed connection while reading response header from upstream
```

This means Nginx forwarded the request to a backend service, but the backend
closed the TCP connection before Nginx received a complete HTTP response header.

In this project that usually happens when:

- `order-service` is killed or restarted while a checkout request is in flight
- the backend crashes during a chaos test
- the backend worker exits before returning the response

In recovery testing this is expected. It does not automatically mean data
corruption. It usually means:

- that particular HTTP request failed
- the transaction may still continue in the background
- the client should poll status or retry carefully

That matches the current design, where `order-service` now treats uncertain
orchestrator startup failures more carefully.

## Current Strengths

- stateless ingress
- simple and easy to reason about
- keepalive is enabled for upstream connections
- generous worker connection limits for local testing
- fine for one-node Compose deployment and course-scale load

## Current Weaknesses

### Single point of failure

There is only one gateway container.
If the professor kills it:

- in-flight requests fail
- new requests fail until Docker restarts it

Data consistency should survive because the gateway is stateless, but
availability still drops briefly.

### No self-healing upstream DNS refresh

The config uses static upstream blocks like:

- `server order-service:5000;`
- `server stock-service:5000;`
- `server payment-service:5000;`

Nginx resolves those names when it starts. If a backend container is recreated
with a new IP, the gateway can hold a stale upstream address until it is
restarted. That is why the recovery tests bounce the gateway after some service
restarts.

This is the biggest recovery limitation in the current gateway.

### No gateway health check

Compose starts the gateway after backends become healthy, but the gateway
itself has no health check. That makes it harder to reason about whether the
ingress is actually ready from Docker's point of view.

### Not horizontally scalable

The gateway is one container on one port.
There is no:

- second gateway replica
- front load balancer
- active-active ingress setup

So the gateway is not a scalable HA tier yet.

## Recommendation for the Report / Interview

The honest claim is:

- the transaction layer is fault-tolerant against participant and coordinator
  failures better than before
- the gateway is still a simple local ingress, not a high-availability ingress

That is a defensible answer.

## If You Want to Harden It Next

The next gateway improvements should be:

1. Add Docker-DNS-based dynamic upstream resolution instead of relying on
   startup-only name resolution.
2. Add a gateway health check in `docker-compose.yml`.
3. Consider a second gateway replica plus an outer load balancer if you want
   to claim true ingress availability.
4. Add retry rules only for clearly safe idempotent GET paths, not blindly for
   checkout POSTs.

## Bottom Line

For local development and project benchmarking:

- good enough
- simple
- fast enough

For strong claims like "kill-proof ingress" or "scalable gateway":

- not yet
