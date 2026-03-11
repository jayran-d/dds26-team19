# Saga Implementation Explained

This document explains how the Kafka Saga in this repository works and why some of the less obvious implementation choices exist.

## Overview

The system implements an **orchestrated Saga**:

- the **order service** is the orchestrator
- the **stock service** and **payment service** are participants
- communication between services happens through Kafka commands and events
- each service keeps its own local Redis state

The high-level flow is:

1. order starts checkout and writes a durable Saga record
2. order publishes `RESERVE_STOCK`
3. stock applies the reservation locally and publishes either `STOCK_RESERVED` or `STOCK_RESERVATION_FAILED`
4. if stock succeeded, order publishes `PROCESS_PAYMENT`
5. payment applies the charge locally and publishes either `PAYMENT_SUCCESS` or `PAYMENT_FAILED`
6. if payment failed, order publishes `RELEASE_STOCK`
7. stock compensates locally and publishes `STOCK_RELEASED`

## Why The Order Service Is The Orchestrator

Only one service should decide what happens next in a distributed transaction. Here that is the order service.

That gives the order service three responsibilities:

- remember the current Saga state
- decide the next command to send
- decide when compensation is required

This is why the order service has:

- a Saga record in Redis
- startup recovery
- a timeout scanner

Participants do not try to coordinate the whole workflow. They only perform their own local step safely and reply.

## The Important Identifiers

Three identifiers matter most:

- `order_id`: the business object the user sees
- `tx_id`: one Saga attempt for one checkout
- `message_id`: one Kafka message instance

Why they are separate:

- the same `order_id` may be retried later with a new `tx_id`
- duplicate Kafka deliveries may reuse the same `tx_id` but have the same or different transport behavior
- deduplication and stale-event filtering need more than just `order_id`

In practice:

- stale events are rejected by comparing `order_id` and current active `tx_id`
- duplicate events are rejected by `message_id`
- participant idempotency is keyed by `(tx_id, action_type)`

## Why The Order Service Writes State Before Publish

In the orchestrator, every state transition happens before the next Kafka command is published.

Example:

- on `PAYMENT_FAILED`, order first transitions to `compensating`
- only after that does it publish `RELEASE_STOCK`

Reason:

- if the service crashes after persisting the new state but before the publish, recovery can resend the missing command
- if it published first and crashed before persisting, restart would not know what was supposed to happen next

This is the main recovery rule on the orchestrator side:

- **persist intent first**
- **publish second**

## Why Participants Use A Ledger

Each participant stores a ledger entry per `(tx_id, action_type)`.

That ledger tracks:

- whether the command was received
- whether the local business effect was applied
- whether the reply was already published
- the exact reply message to republish on restart

Without that ledger, duplicate Kafka delivery could:

- subtract stock twice
- charge a user twice
- release stock twice

The ledger makes each participant step idempotent.

## Why We Store The Full Reply Message

The participant ledger stores `reply_message`, not only a success/failure flag.

Reason:

- if the service crashes after local apply but before reply publish, restart can republish the exact same event
- no message reconstruction is needed
- replay keeps the same structure the orchestrator already expects

This is why `_republish(...)` is very small in the participant Saga files: it simply re-sends the stored message.

## Why Redis `pipeline()` / `WATCH` / `MULTI` / `EXEC` Is Used

The participant services use Redis transactions to keep the local business effect and the participant ledger in sync.

This is what happens in functions like:

- `_apply_process_payment_atomically(...)`
- `_apply_refund_payment_atomically(...)`
- `_reserve_stock_atomically(...)`
- `_release_stock_atomically(...)`

### `WATCH`

`WATCH` gives optimistic locking.

The code watches:

- the ledger key
- the business key or keys involved in the step

If any watched key changes before commit, Redis raises `WatchError` and the code retries.

Reason:

- decisions should not be made from stale reads
- concurrent changes to stock or credit must invalidate the attempt and force a re-read

### `MULTI` / `EXEC`

`MULTI` queues writes and `EXEC` commits them together.

That is used so that:

- the business change
- and the ledger transition to `APPLIED`

happen as one local atomic step.

Example:

- payment debit and `PROCESS_PAYMENT` ledger update commit together
- stock deductions for all items and the `RESERVE_STOCK` ledger update commit together

Reason:

- after restart, `APPLIED` must mean the local business effect definitely happened
- without this, a crash could leave “user charged but ledger still says received”

## Why Some Compensation Steps Are No-Ops But Still Recorded

In both payment refund and stock release, compensation may discover that the original forward action never actually succeeded.

Examples:

- refund requested but the original payment never succeeded
- release requested but the original reserve never succeeded

In that case the compensation is treated as a **no-op success** and still recorded as `APPLIED` / `REPLIED`.

Reason:

- compensation must stay idempotent
- future retries must not keep re-running the same command forever
- the participant should still produce a final response to unblock the orchestrator

## Why Kafka Offsets Are Committed In The Event/Consumer Loops

Kafka offset commit is deliberately placed in the worker loops, after handler success.

The pattern is:

1. poll a Kafka message
2. run the Saga handler
3. if the handler returns successfully, call `commit()`
4. if the handler raises, do not commit

Reason:

- Kafka must not forget the message before the service has durably handled it
- if a crash happens before durable handling completed, Kafka redelivery is desirable

So the commit boundary means:

- “everything needed for safe restart is already durable”

That is why commit does not happen inside the low-level Kafka client, and not before the handler.

## Why The Order Service Marks Events Seen Only After Handling

The order service tracks processed `message_id`s.

But it marks an event as seen only after:

- the Saga transition succeeded
- any next command was published successfully

Reason:

- if the message were marked seen too early and the service crashed, Kafka redelivery would be suppressed even though the Saga had not advanced yet

So the order side uses:

- dedupe to drop already handled events
- delayed “mark seen” to avoid losing progress

## Why Only The Order Service Has A Periodic Timeout Scanner

Timeout ownership belongs to the orchestrator.

Reason:

- the order service knows what command it sent and what reply it is waiting for
- participants do not know whether silence means delay, crash, replay, or replacement by a newer `tx_id`

So the split is:

- order service: periodic timeout detection and command replay
- participants: durable local execution plus startup replay of missing replies

This keeps participants simpler and avoids conflicting autonomous recovery decisions.

## Recovery Model

Recovery happens in two places.

### Order Service Recovery

On startup, the order service scans active Saga records and republishes the last intended command.

Timeout scanning does the same thing later for Sagas that waited too long.

### Participant Recovery

On startup, payment and stock scan for ledger entries that are `APPLIED` but not `REPLIED`.

For those entries, they republish the stored reply event.

This covers the crash window:

- local work committed
- reply not yet sent

## What The Logging Tries To Show

The logs are designed to make one `tx_id` traceable through all three services.

Typical happy path:

1. stock logs `RESERVE_STOCK success`
2. order logs `event=STOCK_RESERVED`
3. payment logs `PROCESS_PAYMENT success`
4. order logs `event=PAYMENT_SUCCESS`

Typical payment-failure path:

1. stock logs `RESERVE_STOCK success`
2. order logs `event=STOCK_RESERVED`
3. payment logs `PROCESS_PAYMENT failure`
4. order logs `event=PAYMENT_FAILED`
5. stock logs `RELEASE_STOCK done`
6. order logs `event=STOCK_RELEASED`

These sequences are what the automated recovery tests and manual log-watching guide are trying to validate.

## Limits Of What This Guarantees

Passing tests and reading clean logs is strong evidence, but not a formal proof.

Things still rely on operational assumptions such as:

- Kafka and Redis actually persisting state the way the deployment expects
- process restarts happening within the tested recovery windows
- no untested edge case outside the covered scenarios

So the intended claim is:

- the implementation is designed to be idempotent and restart-tolerant
- the tested failure scenarios behave correctly

That is the engineering goal for this project.
