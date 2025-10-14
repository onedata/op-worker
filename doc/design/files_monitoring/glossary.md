# Space Files Monitoring Glossary

A comprehensive glossary of terms used in the Space Files Monitoring system.

---

## Behind

A state where a client's `Last-Event-Id` is older than the **Main Monitor's** current 
sequence. The client is "behind" and needs a **Catching Monitor** to replay missed events 
before subscribing to the main monitor.
Learn more: [Reconnection](reconnection.md#routing-decision).

## Catching Monitor

A temporary monitor process that replays historical events for reconnecting clients 
who are **Behind**. It streams events from `SinceSeq` to `UntilSeq`, then proposes 
**Takeover** to the **Main Monitor** and terminates. One catching monitor is created 
per reconnecting client that is behind.
Learn more: [Monitors](monitors.md#catching-monitor).

## Caught Up

A state where a client's `Last-Event-Id` is at or ahead of the **Main Monitor's** 
current sequence. The client can subscribe directly to the main monitor without 
needing a **Catching Monitor**.
Learn more: [Reconnection](reconnection.md#routing-decision).

## Couchbase Changes Stream

The underlying mechanism that delivers document change notifications from the Couchbase 
database. The system monitors changes to `file_meta`, `times`, and `file_location` 
documents to generate file events.
Learn more: [Event Streaming](event_streaming.md#observable-documents).

## Direct Children

Only files that are immediate children of **Observed Directories** are monitored. 
There is no recursive monitoring of subdirectories. To monitor nested directories, 
each level must be explicitly included in the subscription.
Learn more: [Event Streaming](event_streaming.md#filtering-logic).

## File Changed/Created Event

An event sent to subscribers when a file in an **Observed Directory** is created or 
modified. Contains the file ID, parent ID, and requested file attributes. 
Generated from changes to `file_meta`, `times`, or `file_location` documents.
Learn more: [Event Streaming](event_streaming.md#event-types).

## File Deleted Event

An event sent to subscribers when a file in an **Observed Directory** is deleted. 
Contains only the file ID and parent ID (no attributes). Generated from changes 
to deleted `file_meta` documents.
Learn more: [Event Streaming](event_streaming.md#event-types).

## Gap

The difference between the **Main Monitor's** current sequence and a client's 
`Last-Event-Id`. When `Gap > 0`, the client is **Behind** and needs to catch up 
via a **Catching Monitor**.
Learn more: [Reconnection](reconnection.md#routing-decision).

## Handler

An HTTP connection handler process (e.g., `space_file_events_stream_handler`) that 
owns the client SSE connection. The handler is linked to its current **Monitor** 
and receives events via Erlang messages.
Learn more: [Architecture](architecture.md#process-relationships).

## Heartbeat Event

A periodic event sent to clients to update their `Last-Event-Id` during inactivity. 
Prevents clients from having stale sequence numbers when no changes occur in their 
**Observed Directories**, even though changes happen elsewhere in the space. Sent 
when the gap between current sequence and client's last seen sequence exceeds 
the threshold (default: 100).
Learn more: [Reconnection](reconnection.md#heartbeat-mechanism).

## Last-Event-Id

An SSE (Server-Sent Events) standard HTTP header sent by clients on reconnect. 
Contains the Couchbase sequence number of the last event the client received. 
Used to determine if the client is **Caught Up** or **Behind**.
Learn more: [Reconnection](reconnection.md#last-event-id-support).

## Last Seen Sequence

The most recent Couchbase sequence number for which an **Observer** received an event. 
Tracked per observer and used to determine when to send **Heartbeat Events**. 
Updated after every event sent (file change, deletion, or heartbeat).
Learn more: [Reconnection](reconnection.md#heartbeat-mechanism).

## Main Monitor

The primary, long-lived monitor process for a space. Streams live events from the 
current Couchbase sequence and accepts or rejects client subscriptions based on 
their `Last-Event-Id`. One main monitor exists per actively monitored space. 
Times out after inactivity when no **Observers** are connected.
Learn more: [Monitors](monitors.md#main-monitor).

## Monitoring Specification

The client's subscription configuration specifying which directories to watch 
(`observed_dirs`) and which file attributes to include in events (`observed_attrs_per_doc`). 
Each **Observer** has its own monitoring specification.
Learn more: [Event Streaming](event_streaming.md#filtering-logic).

## Observer

A client process (handler PID) subscribed to a monitor, with associated session ID 
and **Monitoring Specification**. In practice, an observer is the handler process 
that owns the client's HTTP connection. Multiple observers can subscribe to the 
same monitor, each with their own authorization and attribute preferences.
Learn more: [Monitors](monitors.md#shared-logic).

## Observed Attributes

The file attributes requested by a client for each document type (`file_meta`, 
`times`, `file_location`). Only attributes the client has permission to read 
are included in events. Part of the **Monitoring Specification**.
Learn more: [Event Streaming](event_streaming.md#filtering-logic).

## Observed Directories

The list of directory GUIDs that a client wants to monitor. Only **Direct Children** 
of these directories generate events. Part of the **Monitoring Specification**.
Learn more: [Event Streaming](event_streaming.md#filtering-logic).

## Sequence Number

A monotonically increasing integer assigned by Couchbase to each document change. 
Used as event IDs in SSE streams and for determining if clients are **Caught Up** 
or **Behind**. Also called "seq" in the codebase.
Learn more: [Reconnection](reconnection.md#sequence-continuity).

## Since Seq

The starting Couchbase sequence number for a monitor or subscription. For catching 
monitors, this is `Last-Event-Id + 1`. For main monitors without reconnection, 
this is the current database sequence.
Learn more: [Reconnection](reconnection.md#routing-decision).

## SSE (Server-Sent Events)

An HTTP-based protocol for streaming real-time events from server to client. 
Supports automatic reconnection with `Last-Event-Id` header. Used to deliver 
file change notifications to clients.
Learn more: [Overview](_overview.md).

## Subscription

An opaque data structure managed by `files_monitoring_manager` that tracks a 
client's connection to monitors. Hides monitor PIDs and state from handlers. 
Contains the monitor type (main or catching) and relevant process IDs.
Learn more: [Architecture](architecture.md#manager-abstraction).

## Takeover

The protocol by which a **Catching Monitor** transfers its client to the 
**Main Monitor** once caught up. The catching monitor proposes takeover with 
observer details, the main monitor atomically links the handler and adds the 
observer, and the catching monitor terminates with `{shutdown, caught_up}`. 
Ensures no events are lost or duplicated during transition.
Learn more: [Reconnection](reconnection.md#takeover-protocol).

## Until Seq

The target Couchbase sequence number for a **Catching Monitor**. Fetched from 
the **Main Monitor** at catching start. When the catching monitor reaches this 
sequence, it proposes **Takeover**. If the main advances meanwhile, the catching 
monitor updates its target and continues.
Learn more: [Reconnection](reconnection.md#catching-monitor-lifecycle).

