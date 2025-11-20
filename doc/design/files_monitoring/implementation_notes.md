# Space Files Monitoring - Implementation Notes

Critical caveats, edge cases, performance considerations, and client implementation 
guidelines.

---

## Important caveats

Understanding these limitations is essential for correct client implementation. 
They arise from the underlying architecture based on Couchbase changes feed and 
the distributed nature of Onedata.

### Document field granularity

**Limitation**: The system monitors **document-level changes**, not field-level 
changes. You cannot know which specific field within a document was modified.

**Scenario**:
```
Document: file_meta for "data.csv"
Fields: {name: "data.csv", mode: 0644, owner: "user1", ...}

Change 1: mode 0644 → 0755
  → Document changed at sequence 1000

Observer requests: [name]
  → Receives event with name="data.csv" (unchanged field!)

Client must compare: Did name actually change? No.
```

**Implication**: You may receive events for files where your requested attributes 
didn't actually change. The system only knows *some* field changed, not which one.

**Client Responsibility**: 
```javascript
// Client-side change detection
onEvent(event) {
    const fileId = event.data.fileId;
    const newAttrs = event.data.attributes;
    const oldAttrs = this.cache.get(fileId);
    
    // Compare old vs new to detect actual changes
    if (JSON.stringify(oldAttrs) !== JSON.stringify(newAttrs)) {
        // Attributes actually changed
        this.cache.set(fileId, newAttrs);
        this.updateUI(fileId, newAttrs);
    } else {
        // False positive - different field changed
        // Ignore or log
    }
}
```

**Why This Design?**

This limitation stems from how Couchbase changes feed works - it reports document-level 
changes, not field-level changes. The current document structure doesn't track individual 
fields separately. Adding field-level tracking would require:
- Storing previous document state
- Comparing old vs. new on every change  
- Significant performance overhead

The current design prioritizes performance and simplicity over fine-grained change detection.

### Event ordering and race conditions

**Limitation**: The Couchbase changes feed does not guarantee strict ordering 
across different document types.

**Scenario**:
```
Time T1: User deletes "file.txt"
  → file_meta marked deleted
  → times document still exists
  → file_location document still exists

Time T2: System garbage collects times document
Time T3: System garbage collects file_location document

Changes feed order (non-deterministic):
  Sequence 1000: file_meta (deleted) → deleted event sent
  Sequence 1001: times (deleted) → skipped (deleted docs filtered out)
  OR
  Sequence 1001: times (change before deletion) → changedOrCreated event sent
  Sequence 1002: file_meta (deleted) → deleted event sent
```

**Implication**: You may receive `changedOrCreated` event for a file **after** 
receiving a `deleted` event for the same file.

**Client Responsibility**:
```javascript
onEvent(event) {
    const fileId = event.data.fileId;
    
    if (event.event === 'deleted') {
        this.cache.delete(fileId);
        this.removeFromUI(fileId);
    } else if (event.event === 'changedOrCreated') {
        // CRITICAL: Verify file still exists before processing
        if (this.cache.has(fileId) && this.cache.get(fileId).deleted) {
            // File was already deleted - ignore this event
            return;
        }
        
        // Alternative: Query filesystem to verify existence
        // api.getFileAttrs(fileId).then(attrs => {
        //     // File exists - process change
        // }).catch(err => {
        //     // File doesn't exist - ignore event
        // });
        
        this.updateFile(fileId, event.data.attributes);
    }
}
```

**Recommended Pattern**: Always verify file existence before processing 
`changedOrCreated` events, especially if you've received a `deleted` event 
for that file previously.

### Duplicate deletion events

**Limitation**: You may receive multiple `deleted` events for the same file.

**Scenario**:
```
Sequence 1000: file_meta marked deleted → deleted event
  File is deleted, but file_meta document still exists (marked deleted)

Sequence 1005: file_meta updated (reference counts, cleanup metadata)
  → deleted event again (file_meta still marked deleted)

Sequence 1010: file_meta updated again (final cleanup)
  → deleted event again
```

**Why This Happens**: The `file_meta` document may be modified multiple times 
during cleanup. Each modification appears in the changes feed, and since the document 
is marked deleted, each generates a `deleted` event.

**Client Responsibility**:
```javascript
const deletedFiles = new Set();

onEvent(event) {
    const fileId = event.data.fileId;
    
    if (event.event === 'deleted') {
        if (deletedFiles.has(fileId)) {
            // Already processed this deletion - ignore
            return;
        }
        
        deletedFiles.add(fileId);
        this.cache.delete(fileId);
        this.removeFromUI(fileId);
    }
}
```

**Idempotent Deletion**: Design your deletion handlers to be idempotent - 
deleting an already-deleted file should be a no-op.

### "False Positive" Change Notifications

**Limitation**: You may receive `changedOrCreated` events even when your requested 
attributes didn't change.

**Example Scenarios**:

**Scenario 1: Different Field Changed**
```
Observer requests: [name, size]
Change: mode 0644 → 0755 (permission change)
  → file_meta document changed
  → changedOrCreated event sent with {name, size}
  → Both unchanged from client's perspective
```

**Scenario 2: Internal Metadata**
```
Observer requests: [name]
Change: Internal hard link count updated
  → file_meta document changed
  → changedOrCreated event sent with {name}
  → Name unchanged
```

**Scenario 3: Multiple Document Types**
```
Observer requests: {file_meta => [name], file_location => [size]}
Change: times document updated (mtime changed)
  → times document changed
  → But observer doesn't request times attributes
  → No event sent (correct)

Change: file_meta document updated (unrelated field)
  → file_meta document changed
  → Observer requests file_meta.name
  → Event sent with {name} (unchanged)
```

**Client Strategy**: Always compare received attributes with cached state to 
determine if meaningful changes occurred. Don't assume every event represents 
a visible change.

## Client implementation guidelines

For practical client implementation patterns and examples, see the dedicated 
**[Client Implementation Guide](../../guides/file_monitoring/client.md)**.

The guide covers:
- Quick start examples (curl, Python)
- Essential client patterns
- Complete working implementations
- Troubleshooting common issues

### Summary of key pitfalls

**Always verify file existence**: Events may arrive out of order

**Handle duplicate deletions idempotently**: Same file may be deleted multiple times

**Compare received data with cache**: Don't assume every event represents a meaningful change

**Maintain local state**: Essential for handling out-of-order events and detecting actual changes

See the **[Client Implementation Guide](../../guides/file_monitoring/client.md)** for 
detailed implementations and complete working examples.

## Performance considerations

### Parallelized authorization checks

Authorization checks run in parallel (up to 20 concurrent checks by default) to 
prevent blocking when multiple observers watch the same directory:

```erlang
-define(MAX_AUTHZ_VERIFY_PROCS, 20).

lists_utils:pfiltermap(AuthorizeFun, Observers, ?MAX_AUTHZ_VERIFY_PROCS)
```

**Tuning**: Adjust `space_files_observers_max_authorize_procs` environment variable 
if you have:
- Many observers per directory (increase for better throughput)
- Limited system resources (decrease to reduce load)

### Couchbase stream throttling

The monitor uses a call/reply pattern with Couchbase changes stream to prevent 
flooding:

```erlang
% Couchbase stream calls monitor
notify_monitor_callback(MonitorPid, {ok, Docs}) ->
    call_monitor(MonitorPid, #docs_change_notification{docs = Docs}),
    ok.

% Monitor replies immediately, then processes
handle_call(#docs_change_notification{docs = Docs}, From, State) ->
    gen_server2:reply(From, ok),  % Couchbase can prepare next batch
    NewMonitoring = process_docs(Docs, State#state.monitoring),  % Stream blocks
    {noreply, State#state{monitoring = NewMonitoring}}.
```

**Benefits**:
- Natural backpressure - stream waits for monitor to finish processing
- Parallel preparation - stream prepares next batch while monitor processes current
- No flooding - monitor never gets more than one batch ahead

### Heartbeat threshold

Default heartbeat threshold is 100 sequence numbers. This balances:
- Reducing replay size on reconnect (lower threshold = more heartbeats)
- Minimizing event overhead (higher threshold = fewer heartbeats)

**Tuning**: Adjust `space_files_observers_last_seen_seq_heartbeat_threshold` if:
- Frequent reconnects with large gaps (decrease threshold)
- High event rate with many observers (increase threshold to reduce overhead)

## Future optimizations

These optimizations are **not currently implemented** but may be added in the future.

### Document buffer for fast reconnects

**Idea**: Maintain a circular buffer of last N documents (e.g., 100) in main monitor 
to handle short reconnects without starting a replay monitor.

**How It Would Work**:
```erlang
-record(state, {
    ...,
    recent_docs = queue:new() :: queue:queue(datastore:doc())
}).

handle_call(#subscribe_req{since_seq = SinceSeq}, From, State) ->
    case is_seq_in_buffer(SinceSeq, State#state.recent_docs) of
        {true, RelevantDocs} ->
            % Fast path - replay from buffer
            reply({ok, buffer_replay, RelevantDocs}, State);
        false ->
            % Standard path - reject and start replay monitor
            reply({error, {main_ahead, CurrentSeq}}, State)
    end.
```

**Benefits**:
- Fast reconnect for typical short disconnections (~10ms vs ~100ms)
- No replay monitor process needed for small gaps
- Less load on Couchbase (no additional changes stream)
- Reduced latency for common case

**Trade-offs**:
- Memory overhead: ~10-50MB per node (N docs × M spaces)
- Additional complexity in `try_subscribe` logic
- Buffer size tuning required
- Need to measure if this optimization is actually needed

**Decision**: Implement only if metrics show frequent replay monitor starts 
for small gaps (< 100 events).

### Adaptive heartbeat threshold

**Idea**: Dynamically adjust heartbeat threshold based on space activity patterns.

**How It Would Work**:
```erlang
% High activity space: Increase threshold to reduce heartbeat overhead
AvgEventsPerMinute > 1000 -> Threshold = 500

% Low activity space: Decrease threshold for better reconnect experience
AvgEventsPerMinute < 100 -> Threshold = 50
```

**Benefits**:
- Better balance between heartbeat overhead and reconnect experience
- Adapts to changing space activity patterns

**Trade-off**: Additional complexity, need metrics collection

**Decision**: Implement if user feedback indicates heartbeat tuning is important.

### Smarter replay monitor reuse

**Idea**: If multiple clients reconnect with similar `Last-Event-Id`, share a 
single replay monitor.

**How It Would Work**:
```erlang
% Client A reconnects: Last-Event-Id = 900
% Client B reconnects: Last-Event-Id = 905
% Share single replay monitor from 900-1000
% Split to separate observers at 905
```

**Benefits**:
- Reduced Couchbase load
- Reduced memory usage
- Better handling of mass reconnects (e.g., network partition)

**Trade-off**: Complex coordination logic, edge cases

**Decision**: Implement only if metrics show frequent concurrent reconnects.

## Related documentation

- **[Overview](_overview.md)** - System introduction and guarantees
- **[Architecture](architecture.md)** - Supervisor hierarchy
- **[Monitors](monitors.md)** - Monitor implementations
- **[Event Streaming](event_streaming.md)** - Event types and authorization
- **[Reconnection](reconnection.md)** - Takeover protocol
- **[Glossary](glossary.md)** - Term definitions
