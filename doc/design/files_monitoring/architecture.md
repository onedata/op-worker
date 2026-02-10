# Space Files Monitoring Architecture

Deep dive into the supervision tree, process relationships, and lifecycle management 
of the space files monitoring system.

---

## Overview

The space files monitoring architecture is built around a 3-level supervisor hierarchy 
that cleanly models the domain: a top-level supervisor manages per-space monitoring 
trees, each space tree consists of a main monitor and a replay monitors supervisor, 
and the replay supervisor dynamically manages temporary replay monitors.

This design ensures proper fault isolation (one space's failure doesn't affect others), 
automatic dependency management (main monitor death terminates replay monitors), 
and efficient resource cleanup (trees terminate on inactivity).

## 3-level supervisor hierarchy

```mermaid
graph TB
    subgraph "Level 1: Top-Level Supervisor"
        TopSup[files_monitoring_sup<br/>one_for_one]
        Manager[files_monitoring_manager<br/>worker]
    end
    
    subgraph "Level 2: Per-Space Supervisors"
        SpaceSup1[space_files_monitoring_sup<br/>space1<br/>rest_for_one]
        SpaceSup2[space_files_monitoring_sup<br/>space2<br/>rest_for_one]
    end
    
    subgraph "Level 3: Space1 Monitoring Processes"
        Main1[space_files_main_monitor<br/>space1]
        ReplaySup1[replay_monitors_sup<br/>space1<br/>simple_one_for_one]
        Replay1a[replay_monitor_1]
        Replay1b[replay_monitor_2]
    end
    
    subgraph "Level 3: Space2 Monitoring Processes"
        Main2[space_files_main_monitor<br/>space2]
        ReplaySup2[replay_monitors_sup<br/>space2<br/>simple_one_for_one]
    end
    
    TopSup --> Manager
    TopSup --> SpaceSup1
    TopSup --> SpaceSup2
    
    SpaceSup1 --> Main1
    SpaceSup1 --> ReplaySup1
    ReplaySup1 --> Replay1a
    ReplaySup1 --> Replay1b
    
    SpaceSup2 --> Main2
    SpaceSup2 --> ReplaySup2
    
    style TopSup fill:#e1f5fe
    style Manager fill:#f3e5f5
    style SpaceSup1 fill:#fff3e0
    style SpaceSup2 fill:#fff3e0
    style Main1 fill:#e8f5e8
    style Main2 fill:#e8f5e8
    style ReplaySup1 fill:#ffe0b2
    style ReplaySup2 fill:#ffe0b2
```

### Level 1: Top-level supervisor

**Module**: `files_monitoring_sup`  
**Strategy**: `one_for_one`  
**Responsibility**: Manages per-space supervisor trees and the monitoring manager

The top-level supervisor uses `one_for_one` strategy because each space's monitoring 
is independent - if one space's supervisor fails, others continue unaffected.

**Children**:
1. `files_monitoring_manager` - Coordinates lifecycle and routing (started first)
2. Per-space supervisors - Created dynamically when clients connect

**Key Functions**:
- `ensure_monitoring_tree_for_space/1` - Atomically creates or returns existing 
  space supervisor
- `find_sup_for_space/1` - Looks up existing space supervisor (returns `undefined` if not found)

### Level 2: Per-space supervisors

**Module**: `space_files_monitoring_sup`  
**Strategy**: `rest_for_one`  
**Responsibility**: Manages main monitor and replay monitors supervisor for one space

The `rest_for_one` strategy is critical here - it models the dependency relationship 
where replay monitors depend on the main monitor. When the main monitor terminates, 
the replay monitors supervisor (and all its children) must also terminate.

**Children (in order)**:
1. `space_files_main_monitor` - Primary event stream (first child)
2. `space_files_replay_monitors_sup` - Dynamic replay monitors (second child)

**Why rest_for_one?**

The order matters:
- **Main terminates → Replay dies**: Correct! Replay monitors replay to the main's 
  sequence. If main is gone, replay has nowhere to hand off clients.
- **Replay terminates → Main unaffected**: Correct! Individual replay monitor 
  failures don't impact the main stream.

**Restart Strategy**: `temporary` - Space supervisor doesn't restart automatically. 
Reconnecting clients trigger recreation through the manager.

### Level 3: Monitor processes

#### Main Monitor (Worker)

**Module**: `space_files_main_monitor`  
**Strategy**: N/A (worker, not supervisor)  
**Restart**: `permanent`  
**Responsibility**: Stream live events, accept/reject subscriptions, accept takeovers

One main monitor per space. Streams from current Couchbase sequence. Accepts 
subscriptions if clients are caught up, rejects if behind (triggering replay 
monitor creation).

#### Replay Monitors Supervisor

**Module**: `space_files_replay_monitors_sup`  
**Strategy**: `simple_one_for_one`  
**Responsibility**: Dynamically manage temporary replay monitors

Creates one replay monitor per reconnecting client that is behind. All children 
are the same type (`space_files_replay_monitor`), added and removed dynamically.

#### Replay Monitors (Workers)

**Module**: `space_files_replay_monitor`  
**Strategy**: N/A (worker, not supervisor)  
**Restart**: `temporary`  
**Responsibility**: Replay historical events, propose takeover, terminate

Each replay monitor:
- Replays events from `SinceSeq` to `UntilSeq`
- Proposes takeover when reaching target sequence
- Dies with `{shutdown, caught_up}` after successful takeover

Restart is `temporary` because replay monitors are one-time use - once they 
complete their job (takeover or failure), they shouldn't restart.

## Manager abstraction

The `files_monitoring_manager` provides a clean abstraction layer between HTTP 
handlers and the monitoring system.

```mermaid
graph LR
    subgraph "Handler Layer"
        Handler[HTTP Handler<br/>SSE Connection]
    end
    
    subgraph "Manager Layer"
        Manager[files_monitoring_manager<br/>Routing Logic]
    end
    
    subgraph "Monitor Layer"
        Main[Main Monitor]
        Replay[Replay Monitor]
    end
    
    Handler -->|try_subscribe| Manager
    Handler -->|handle_monitor_exit| Manager
    
    Manager -->|ensures tree exists| Main
    Manager -->|creates if needed| Replay
    Manager -.->|opaque subscription| Handler
    
    Main -->|events| Handler
    Replay -->|events| Handler
    
    style Manager fill:#f9f,stroke:#333
    style Handler fill:#9ff,stroke:#333
```

### Manager responsibilities

**Atomic Tree Creation**:
```erlang
try_subscribe(SpaceId, SubscribeReq) ->
    SpaceSupPid = files_monitoring_sup:ensure_monitoring_tree_for_space(SpaceId),
    do_try_subscribe(SpaceSupPid, SubscribeReq).
```

The manager ensures the space monitoring tree exists before attempting subscription, 
handling races between concurrent first subscribers.

**Routing Decision**:
```erlang
do_try_subscribe(SpaceSupPid, SubscribeReq) ->
    MainMonitorPid = get_main_monitor_pid(SpaceSupPid),
    case space_files_main_monitor:try_subscribe(MainMonitorPid, SubscribeReq) of
        ok ->
            {ok, #subscription{monitor_type = main, ...}};
        {error, {main_ahead, UntilSeq}} ->
            start_replay_monitor(SpaceSupPid, MainMonitorPid, ...)
    end.
```

Based on main monitor's response, routes client to appropriate monitor type.

**EXIT Interpretation**:
```erlang
handle_monitor_exit(Pid, Reason, Subscription) ->
    case {Pid, Reason} of
        {MainPid, _} when MainPid == Subscription#subscription.main_pid ->
            stop;  % Main died - fatal
        {ReplayPid, {shutdown, caught_up}} ->
            {ok, Subscription#subscription{monitor_type = main}};  % Takeover success
        {ReplayPid, _} ->
            stop  % Replay died before takeover - fatal
    end.
```

Interprets EXIT signals from monitors, distinguishing between normal takeover 
and failure cases.

**Opaque Subscription**:

The manager returns an opaque `#subscription{}` record to handlers:
```erlang
-record(subscription, {
    monitor_type :: main | replay,
    main_pid :: pid(),
    replay_pid :: pid() | undefined
}).
```

Handlers don't need to know about supervisor structure or routing logic - they 
just receive events and call `handle_monitor_exit/3` when receiving EXIT signals.

## Process relationships

### Handler-monitor linking

```mermaid
sequenceDiagram
    participant H as Handler
    participant M as Main/Replay Monitor
    participant DB as Couchbase
    
    H->>M: Subscribe (with link)
    activate M
    M->>M: erlang:link(HandlerPid)
    M-->>H: ok
    
    loop Event Stream
        DB->>M: Document change
        M->>M: Process & filter
        M->>H: Event message
    end
    
    alt Handler dies
        H-xM: EXIT signal
        M->>M: remove_observer
        Note over M: Continue serving<br/>other observers
    else Monitor dies
        M-xH: EXIT signal
        H->>H: Reconnect or stop
    end
    
    deactivate M
```

**Bidirectional Links**: Handlers and monitors are linked bidirectionally:
- Handler dies → Monitor receives EXIT, removes observer, continues
- Monitor dies → Handler receives EXIT, reconnects or terminates

**Why linking?**
- Automatic cleanup when handler dies (no orphaned subscriptions)
- Immediate notification when monitor dies (enables reconnection)
- No manual cleanup code needed

### Replay monitor lifecycle

```mermaid
stateDiagram-v2
    [*] --> Created: Manager starts<br/>replay monitor
    
    Created --> Replaying: Link handler<br/>Start Couchbase stream
    
    Replaying --> ProposingTakeover: Reached UntilSeq
    
    ProposingTakeover --> TakeoverAccepted: Main accepts<br/>try_subscribe(ok)
    ProposingTakeover --> Replaying: Main ahead<br/>New UntilSeq
    ProposingTakeover --> [*]: Error or<br/>handler died
    
    TakeoverAccepted --> [*]: Exit {shutdown, caught_up}
    
    Replaying --> [*]: Handler died<br/>or stream error
```

**Creation**: Manager creates replay monitor when main rejects subscription
```erlang
{error, {main_ahead, UntilSeq}} = space_files_main_monitor:try_subscribe(...),
{ok, ReplayPid} = space_files_replay_monitors_sup:start_replay_monitor(...)
```

**Replay Phase**: Replay monitor processes documents from `SinceSeq` to `UntilSeq`

**Takeover Proposal**: When reaching target, proposes takeover to main
```erlang
has_reached_target_seq(State) -> CurrentSeq >= UntilSeq
propose_takeover(State) -> space_files_main_monitor:try_subscribe(MainPid, ...)
```

**Termination**: Dies with `{shutdown, caught_up}` after successful takeover, 
triggering handler to update subscription

## Lifecycle management

### Tree creation

**Trigger**: First client connects to a space

**Flow**:
1. Handler calls `files_monitoring_manager:try_subscribe(SpaceId, ...)`
2. Manager ensures tree exists: `files_monitoring_sup:ensure_monitoring_tree_for_space(SpaceId)`
3. Supervisor starts per-space tree: main monitor + replay supervisor
4. Main monitor initializes Couchbase stream from current sequence

**Idempotency**: Multiple concurrent first subscribers race-safely - 
`ensure_monitoring_tree_for_space/1` handles `{already_started, Pid}` return.

### Inactivity timeout

**Trigger**: Main monitor has no observers AND replay supervisor has no children

**Main Monitor Timeout Logic**:
```erlang
is_inactive(State) -> not has_observers(State#state.monitoring).

check_inactivity_timer(State) ->
    case is_inactive(State) of
        true -> schedule_inactivity_shutdown(State);
        false -> cancel_inactivity_shutdown(State)
    end.
```

After `?INACTIVITY_PERIOD_MS` (default: 30 seconds) of inactivity:
1. Main monitor sends `notify_inactive(SpaceId)` to manager
2. Manager double-checks: no observers AND no replay monitors
3. Manager terminates space supervisor tree
4. Supervisor cleanup removes child spec

**Why double-check?**

Race condition: Between main timeout firing and manager checking, a new subscriber 
might have connected. The double-check prevents premature termination.

### Replay monitor prevents timeout

Even if main has no direct observers, it doesn't timeout while replay monitors exist:

```erlang
should_terminate_monitoring_tree(SpaceMonitoringSup) ->
    ReplayCount = get_active_children_count(ReplaySup),
    case ReplayCount of
        0 -> verify_main_inactive();
        _ -> false  % Active replay monitors - don't terminate
    end.
```

**Rationale**: Replay monitors will soon transfer their observers to main via 
takeover. Terminating main would break that handoff.

### Tree termination

**Cascading Shutdown**: When manager terminates space tree, `rest_for_one` ensures 
proper cleanup:
1. Manager calls `supervisor:terminate_child(files_monitoring_sup, SpaceSupChildId)`
2. Space supervisor terminates replay supervisor (rest_for_one)
3. Replay supervisor terminates all replay monitors
4. Space supervisor terminates main monitor
5. All Couchbase streams are canceled
6. Manager deletes child spec: `supervisor:delete_child(...)`

**Cleanup**: 
```erlang
terminate(Reason, State) ->
    couchbase_changes:cancel_stream(ChangesStreamPid),
    ok.
```

Each monitor cancels its Couchbase stream in terminate callback.

## Design rationale

### Why 3 levels?

**Domain Modeling**: The hierarchy directly reflects the domain:
- System has multiple spaces (level 1)
- Each space has one main monitor and many replay monitors (level 2)
- Replay monitors are dynamically created/destroyed (level 3)

**Fault Isolation**: Space failures don't affect other spaces (one_for_one at top level)

**Dependency Management**: Main-replay dependency is automatic (rest_for_one at level 2)

### Why rest_for_one for Per-Space?

**Alternative Considered**: `one_for_all`

If main dies, restart both main and replay supervisor → All replay monitors 
lose their clients anyway (handlers are linked to replay monitors).

**Why rest_for_one is better**:
- More precise: Only dependent children restart
- Replay monitors dying doesn't affect main (correct isolation)
- Matches the actual dependency: replay depends on main, not vice versa

### Why simple_one_for_one for Replay?

**Dynamic Creation**: Replay monitors are created on-demand per reconnecting client

**Uniform Type**: All children are `space_files_replay_monitor` with same spec

**Temporary Restart**: Replay monitors are one-time use, shouldn't restart on failure

**Clean Removal**: Replay monitors self-terminate after takeover, automatic cleanup

### Why manager as separate process?

**Alternative Considered**: Handlers talk directly to supervisors and monitors

**Why manager is better**:
- **Atomic operations**: Tree creation and subscription are a single operation
- **Abstraction**: Handlers don't need to understand supervisor structure
- **Race handling**: Manager centralizes inactivity vs. new subscription races
- **Clean API**: Single entry point for all monitoring operations
- **EXIT interpretation**: Manager encodes knowledge of shutdown semantics

## Related documentation

- **[Monitors](monitors.md)** - Main and replay monitor implementations
- **[Reconnection](reconnection.md)** - Takeover protocol and lifecycle
- **[Glossary](glossary.md)** - Term definitions
