# Space Files Monitoring - Reconnection

Complete guide to Last-Event-Id support, catching monitor lifecycle, takeover 
protocol, and heartbeat mechanism.

---

## Overview

The reconnection system ensures clients never miss events when disconnecting and 
reconnecting. Using the SSE standard `Last-Event-Id` header, clients can resume 
from exactly where they left off, with the system seamlessly handling historical 
replay and transition to live streaming.

```mermaid
stateDiagram-v2
    [*] --> Connect: Client connects
    
    Connect --> CheckSequence: Manager checks<br/>Last-Event-Id
    
    CheckSequence --> MainDirect: Caught up or<br/>no Last-Event-Id
    CheckSequence --> StartCatching: Behind<br/>(Gap > 0)
    
    MainDirect --> Streaming: Receive live events
    
    StartCatching --> Replay: Catching monitor<br/>replays history
    Replay --> ProposeTakeover: Reached UntilSeq
    
    ProposeTakeover --> TakeoverAccepted: Main accepts
    ProposeTakeover --> ContinueReplay: Main ahead,<br/>new target
    
    ContinueReplay --> Replay
    
    TakeoverAccepted --> Streaming: Seamless transition<br/>to main monitor
    
    Streaming --> [*]: Client disconnects
```

## Last-Event-Id support

The `Last-Event-Id` header is part of the SSE specification, designed exactly 
for this use case: resumable event streams.

### SSE standard

When an SSE client receives an event:
```
id: 12345
event: changedOrCreated
data: {...}
```

The client stores `id: 12345` as the last event it received.

On reconnect, the client sends:
```http
GET /api/v3/oneprovider/spaces/{spaceId}/events/files HTTP/1.1
Last-Event-Id: 12345
```

The server interprets this as "I last received sequence 12345, resume from 12346".

### Event IDs are sequence numbers

In this system, event IDs are **Couchbase sequence numbers** converted to strings:
```erlang
#file_changed_or_created_event{
    id = str_utils:to_binary(ChangedDoc#document.seq),
    ...
}
```

**Properties of Sequence Numbers**:
- Monotonically increasing integers
- Globally unique per space
- Persistent across provider restarts
- No gaps within a single document type
- May have gaps across document types (other docs changed)

**Example**:
```
Sequence 1000: file_meta changed → event id: "1000"
Sequence 1001: times changed → event id: "1001"
Sequence 1002: unrelated doc → no event
Sequence 1003: file_location changed → event id: "1003"
```

Client receives events 1000, 1001, 1003. Gap at 1002 is normal (not an observable document).

### Parsing Last-Event-Id

Server parses the header value as an integer:
```erlang
case SubscribeReq#subscribe_req.since_seq of
    undefined -> % No Last-Event-Id - first connection
        ok;
    SinceSeq when is_integer(SinceSeq) ->
        % Client wants to resume from SinceSeq + 1
        check_if_behind(SinceSeq)
end.
```

**Validation**:
- Must be a non-negative integer
- Parsed during request validation
- Invalid values rejected with `400 Bad Request`

## Routing decision

The manager coordinates with the main monitor to determine if a client needs 
historical replay:

```mermaid
sequenceDiagram
    participant C as Client
    participant Mgr as Manager
    participant Main as Main Monitor
    participant CS as Catching Supervisor
    participant Catch as Catching Monitor
    
    C->>Mgr: Subscribe with Last-Event-Id=900
    Mgr->>Mgr: Ensure space tree exists
    Mgr->>Main: try_subscribe(SinceSeq=900)
    
    alt Main CurrentSeq <= SinceSeq (Caught up)
        Main->>Main: add_observer(client)
        Main-->>Mgr: ok
        Mgr-->>C: Subscription (main)
        Note over C,Main: Stream live events
        
    else Main CurrentSeq > SinceSeq (Behind)
        Main-->>Mgr: {error, {main_ahead, 1000}}
        Mgr->>CS: start_catching_monitor(SinceSeq=900, UntilSeq=1000)
        CS->>Catch: Start
        Catch->>Catch: add_observer(client)
        Catch-->>Mgr: {ok, CatchingPid}
        Mgr-->>C: Subscription (catching)
        Note over C,Catch: Replay historical events
    end
```

### Routing logic

**Main Monitor Decision** (`try_subscribe`):
```erlang
handle_call(#subscribe_req{since_seq = SinceSeq}, _From, State) 
        when is_integer(SinceSeq) andalso CurrentSeq > SinceSeq ->
    % Client is behind
    reply({error, {main_ahead, CurrentSeq}}, State);

handle_call(SubscribeReq = #subscribe_req{}, _From, State) ->
    % Client is caught up (or no SinceSeq)
    {ok, NewMonitoring} = add_observer(State#state.monitoring, SubscribeReq),
    reply(ok, State#state{monitoring = NewMonitoring}).
```

**Decision Matrix**:

| Client SinceSeq | Main CurrentSeq | Gap | Decision | Next Step |
|---|---|---|---|---|
| undefined | 1000 | N/A | Caught up | Connect to main |
| 1000 | 1000 | 0 | Caught up | Connect to main |
| 1001 | 1000 | -1 | Ahead | Connect to main (treat as caught up) |
| 900 | 1000 | 100 | Behind | Start catching monitor |
| 500 | 1000 | 500 | Behind | Start catching monitor |

**Why treat "ahead" as caught up?**

Client claims to be ahead (SinceSeq=1001, CurrentSeq=1000). This can happen if:
- Clock skew or sequence rollback (rare)
- Client connected to different provider with slightly different sequence
- Race condition during takeover

Safest approach: Connect to main and stream from current sequence forward. Client 
won't receive duplicate events (sequence only increases).

## Catching monitor lifecycle

When a client is behind, the manager creates a catching monitor to replay missed events.

### Replay phase

Catching monitor processes documents identically to main monitor:
- Filter observable documents
- Check if files are in observed directories
- Perform live authorization checks
- Generate and send events to the single observer

```mermaid
graph LR
    subgraph "Catching Monitor Replay"
        Start[SinceSeq<br/>900]
        
        subgraph "Document Processing"
            D901[Doc 901]
            D902[Doc 902]
            D999[Doc 999]
        end
        
        End[UntilSeq<br/>1000]
        
        Start --> D901
        D901 --> D902
        D902 -.-> D999
        D999 --> End
    end
    
    End --> Takeover[Propose Takeover]
    
    style Start fill:#e1f5fe
    style End fill:#fff3e0
    style Takeover fill:#e8f5e8
```

## Takeover protocol

The takeover protocol transfers a client from catching monitor to main monitor 
without gaps or duplicates in the event stream.

### Protocol steps

```mermaid
sequenceDiagram
    participant Catch as Catching Monitor
    participant Main as Main Monitor
    participant Handler as Client Handler
    
    Catch->>Catch: Reached UntilSeq=1000
    Catch->>Main: try_subscribe(ObserverDetails, SinceSeq=1000)
    
    alt Main CurrentSeq == UntilSeq (Perfect match)
        Main->>Main: link(HandlerPid)
        Main->>Main: add_observer
        Main-->>Catch: ok
        Catch->>Catch: EXIT {shutdown, caught_up}
        Catch-xHandler: EXIT {shutdown, caught_up}
        Handler->>Handler: Update subscription to main
        Main->>Handler: Event 1001
        Main->>Handler: Event 1002
        Note over Handler,Main: Seamless transition
        
    else Main CurrentSeq > UntilSeq (Main advanced)
        Main-->>Catch: {error, {main_ahead, 1050}}
        Catch->>Catch: Update UntilSeq=1050
        Note over Catch: Continue replaying to 1050
        
    else Other error
        Main-->>Catch: {error, Reason}
        Note over Catch: Retry on next batch
    end
```

### Sequence continuity proof

**Catching Range**: `[SinceSeq, UntilSeq)` - **exclusive** upper bound
```
SinceSeq=900, UntilSeq=1000
Catching streams: 900, 901, 902, ..., 999
```

**Main Range**: `[UntilSeq, ∞)` - **inclusive** lower bound
```
UntilSeq=1000
Main streams: 1000, 1001, 1002, ...
```

**Disjoint Ranges**: No overlap
- Catching's last event: 999
- Main's first event: 1000
- No gap: 999 + 1 = 1000 ✓
- No duplicate: 999 ≠ 1000 ✓

**Mathematical Guarantee**:
```
∀ seq ∈ ℤ⁺:
  (seq < UntilSeq → seq in Catching's range) XOR 
  (seq ≥ UntilSeq → seq in Main's range)
```

### Handler EXIT interpretation

When catching monitor dies with `{shutdown, caught_up}`, handler interprets this:
```erlang
handle_monitor_exit(
    CatchingPid,
    {shutdown, caught_up},
    Subscription = #subscription{catching_pid = CatchingPid}
) ->
    % Takeover successful - switch to main
    {ok, Subscription#subscription{
        monitor_type = main,
        catching_pid = undefined
    }}.
```

**No Explicit Handoff Message**: Takeover completion is signaled via process EXIT, 
not a custom message. This is simpler and more robust than a two-message protocol.

### Retry on main ahead

If main monitor advances between catching reaching UntilSeq and proposing takeover:

```
1. Catching reaches UntilSeq=1000 at time T1
2. Meanwhile, new changes arrive at main
3. Main's CurrentSeq advances to 1050 at time T2
4. Catching proposes takeover at time T3
5. Main checks: CurrentSeq(1050) > SinceSeq(1000)
6. Main replies: {error, {main_ahead, 1050}}
7. Catching updates: UntilSeq := 1050
8. Catching continues streaming 1000-1049
9. Catching proposes again when reaching 1050
```

**Convergence**: Eventually catching will catch up to main's sequence (new changes 
can't arrive faster than catching processes old ones, assuming bounded load).

### Graceful termination

Catching monitor's terminate callback:
```erlang
terminate(Reason, State) ->
    couchbase_changes:cancel_stream(ChangesStreamPid),
    ?log_terminate(Reason, State).
```

Cancel the bounded Couchbase stream to free resources.

**Automatic Cleanup**: Catching monitor's supervisor detects termination and 
removes the child spec (temporary restart strategy).

## Heartbeat mechanism

Heartbeat events solve the "stale Last-Event-Id" problem during periods of 
inactivity.

### The problem

**Scenario**:
1. Client observes `/data/input/` directory
2. No files change in `/data/input/` for 24 hours
3. Meanwhile, changes happen in `/data/output/`, `/data/temp/`, etc.
4. Space sequence advances from 1000 to 50000 (49000 changes elsewhere)
5. Client disconnects at sequence 1000
6. Client reconnects with Last-Event-Id: 1000
7. System replays 1001-50000 → 49000 events
8. Client filters and discards 49000 events (none in `/data/input/`)
9. Wasted bandwidth, processing, time

**Root Cause**: Client's Last-Event-Id becomes stale when observed directories 
are inactive, even though the space is active overall.

### The solution

Periodically send "heartbeat" events with current sequence number to keep clients 
up-to-date:
```
id: 15000
event: heartbeat
data: null
```

Client stores `15000` as Last-Event-Id. On reconnect, replay starts from 15001 
instead of 1001.

**Per-Observer**: Each observer has independent `last_seen_seq`, so heartbeats 
are sent individually based on each observer's activity pattern.

**Example**:
```
Observer A observes /active/ (receives events often)
Observer B observes /inactive/ (rarely receives events)

Time T1: Both at seq 1000
Time T2: Events 1001-1050 in /active/
  → Observer A: last_seen_seq = 1050
  → Observer B: last_seen_seq = 1000 (no events in /inactive/)

Time T3: CurrentSeq = 1100, process batch
  Gap for A: 1100 - 1050 = 50 < 100 → No heartbeat
  Gap for B: 1100 - 1000 = 100 >= 100 → Send heartbeat

Observer B receives heartbeat with id=1100
Observer B: last_seen_seq = 1100
```

### When heartbeats are sent

**After Each Batch**: Heartbeats are checked and sent synchronously after processing 
each document batch.

**No Timer**: Heartbeats are **opportunistic**, triggered by document processing 
activity, not by a timer.

**Rationale**: 
- Simpler: No timer management, no timer cancellation
- Natural: Heartbeats piggybacked on existing document processing
- Efficient: No wakeups when system is truly idle
- Sufficient: If space is active (advancing sequence), heartbeats will be sent

**Trade-off**: If space becomes completely idle (no documents changing anywhere), 
no heartbeats are sent. This is acceptable - clients will have fresh Last-Event-Id 
up to the point of space-wide inactivity.

## Flow scenarios

### Scenario 1: First Connection

```mermaid
sequenceDiagram
    participant C as Client
    participant M as Manager
    participant Main as Main Monitor
    
    C->>M: Subscribe (no Last-Event-Id)
    M->>M: Ensure space tree exists
    M->>Main: try_subscribe(SinceSeq=undefined)
    Main->>Main: add_observer
    Main-->>M: ok
    M-->>C: Subscription (main)
    
    loop Live Events
        Main->>C: Event 1001
        Main->>C: Event 1002
        Main->>C: Event 1003
    end
```

**Key Points**:
- No Last-Event-Id header → `SinceSeq = undefined`
- Main always accepts undefined SinceSeq
- Streaming starts from current sequence
- Client builds up Last-Event-Id history

### Scenario 2: Slow Reconnect (Behind)

```mermaid
sequenceDiagram
    participant C as Client
    participant M as Manager
    participant Main as Main Monitor
    participant Catch as Catching Monitor
    
    Note over C: Connected at seq 800
    C->>C: Last event: 800
    Note over C: Disconnect for extended period
    
    Note over Main: Many events...
    Main->>Main: CurrentSeq = 1000
    
    C->>M: Subscribe (Last-Event-Id: 800)
    M->>Main: try_subscribe(SinceSeq=800)
    Main->>Main: Check: CurrentSeq(1000) > SinceSeq(800)
    Main->>Main: Gap = 200 (large)
    Main-->>M: {error, {main_ahead, 1000}}
    
    M->>Catch: Start (SinceSeq=800, UntilSeq=1000)
    Catch->>Catch: add_observer
    Catch-->>M: {ok, CatchingPid}
    M-->>C: Subscription (catching)
    
    loop Replay 801-999
        Catch->>C: Event 801
        Catch->>C: Event 802
        Catch->>C: ...
        Catch->>C: Event 999
    end
    
    Catch->>Catch: Reached UntilSeq=1000
    Catch->>Main: try_subscribe (takeover)
    Main->>Main: add_observer
    Main-->>Catch: ok
    Catch->>Catch: EXIT {shutdown, caught_up}
    Catch-xC: EXIT {shutdown, caught_up}
    C->>C: Update subscription to main
    
    loop Live Events
        Main->>C: Event 1000
        Main->>C: Event 1001
    end
```

**Key Points**:
- Gap of 200 triggers catching monitor
- Catching replays 199 events (801-999)
- Seamless takeover to main at 1000
- No gaps: Catching's last (999) + 1 = Main's first (1000)
- No duplicates: Ranges are disjoint

### Scenario 3: Concurrent Writes During Catching

```mermaid
sequenceDiagram
    participant C as Client
    participant Catch as Catching Monitor
    participant Main as Main Monitor
    participant DB as Couchbase
    
    Note over C,Catch: Catching started with UntilSeq=1000
    
    par Catching replays
        Catch->>DB: Stream 800-1000
        DB->>Catch: Events 800-850
        DB->>Catch: Events 851-900
    and New writes to main
        DB->>Main: Events 1001-1010
        Main->>Main: CurrentSeq = 1010
    end
    
    DB->>Catch: Events 901-950
    DB->>Catch: Events 951-999
    
    Catch->>Catch: Reached UntilSeq=1000
    Catch->>Main: try_subscribe(SinceSeq=1000)
    Main->>Main: Check: CurrentSeq(1010) > SinceSeq(1000)
    Main-->>Catch: {error, {main_ahead, 1010}}
    
    Catch->>Catch: Update UntilSeq=1010
    Catch->>DB: Continue streaming 1000-1010
    DB->>Catch: Events 1000-1009
    
    Catch->>Catch: Reached UntilSeq=1010
    Catch->>Main: try_subscribe(SinceSeq=1010)
    Main->>Main: Check: CurrentSeq(1010) == SinceSeq(1010)
    Main-->>Catch: ok
    
    Catch-xC: EXIT {shutdown, caught_up}
    Main->>C: Event 1010
```

**Key Points**:
- Concurrent writes during replay are normal
- Catching automatically extends replay range
- Eventually converges (catching is faster than new writes)
- Client receives all events in order without gaps

### Scenario 4: Heartbeat During Inactivity

```mermaid
sequenceDiagram
    participant C as Client
    participant Main as Main Monitor
    participant DB as Couchbase
    
    Note over C: Observing /data/input/
    C->>C: last_seen_seq = 1000
    
    Note over DB: 150 changes in /data/output/
    DB->>Main: Events 1001-1150
    Main->>Main: Process events
    Note over Main: None match /data/input/<br/>No events sent to client
    
    Main->>Main: CurrentSeq = 1150
    Main->>Main: Check: 1150 - 1000 = 150 >= 100
    Main->>C: Heartbeat (id=1150)
    C->>C: last_seen_seq = 1150
    
    Note over C: Disconnect at seq 1150
    Note over DB: 50 more changes in /data/output/
    DB->>Main: Events 1151-1200
    
    C->>Main: Reconnect (Last-Event-Id: 1150)
    Main->>Main: Check: CurrentSeq(1200) > SinceSeq(1150)
    Main->>Main: Gap = 50 (manageable)
    
    alt Quick reconnect optimization
        Main->>C: Accept, stream from 1150
    else Start catching
        Note over C: Catching replays 1151-1200
    end
```

**Impact**: Without heartbeat, client would have Last-Event-Id=1000 and would 
replay 1001-1200 (200 events). With heartbeat, client has Last-Event-Id=1150 
and replays only 50 events.

## Related documentation

- **[Architecture](architecture.md)** - Supervisor hierarchy and lifecycle
- **[Monitors](monitors.md)** - Main and catching monitor implementations
- **[Event Streaming](event_streaming.md)** - Event types and generation
- **[Glossary](glossary.md)** - Term definitions
