# Space Files Monitoring - Client Implementation Guide

A practical guide for developers integrating real-time file change notifications 
into their applications using Server-Sent Events (SSE).

---

## Quick start

### Example with curl

```bash
# Connect to monitor a directory
curl -N -H "X-Auth-Token: YOUR_ACCESS_TOKEN" \
     -H "Accept: text/event-stream" \
     -H "Content-Type: application/json" \
     -d '{
       "observedDirectories": ["DIR_OBJECT_ID"],
       "observedAttributes": ["name", "size", "mtime"]
     }' \
     https://oneprovider.example.com/api/v3/oneprovider/spaces/SPACE_ID/events/files
```

**Key flags**:
- `-N` / `--no-buffer`: Disable output buffering (critical for SSE)
- `-H "Accept: text/event-stream"`: Request SSE format

## Understanding events

### Event types

The system sends three types of events:

**1. Changed or Created Event** (`changedOrCreated`)
```
id: 12346
event: changedOrCreated
data: {"fileId":"abc123","parentFileId":"parent456","attributes":{"name":"data.csv","size":1024,"mtime":1704067200}}
```

Sent when a file is created or modified in an observed directory.

**2. Deleted Event** (`deleted`)
```
id: 12347
event: deleted
data: {"fileId":"abc123","parentFileId":"parent456"}
```

Sent when a file is deleted from an observed directory. Note: no attributes included.

**3. Heartbeat Event** (`heartbeat`)
```
id: 12450
event: heartbeat
data: {}
```

Sent periodically to update your `Last-Event-Id` during inactivity. Prevents 
unnecessary replay on reconnect.

### Event structure

All events share common SSE fields:

- **`id`**: Unique sequence number (monotonically increasing integer as string)
- **`event`**: Event type (`changedOrCreated`, `deleted`, or `heartbeat`)
- **`data`**: JSON payload (structure varies by event type)

### Parsing SSE format

SSE is line-based text format. Each event consists of lines starting with field names:

```
id: 12346
event: changedOrCreated
data: {"fileId":"abc123",...}

```

Note the blank line separating events. Most SSE libraries handle parsing automatically.

## Implementing reconnection

### Storing Last-Event-Id

Always track the last event ID you received:

```
class FileMonitor {
    last_event_id = null
    
    function process_event(event) {
        // Store event ID for reconnection
        last_event_id = event.id
        
        // Persist to disk/database for crash recovery
        save_to_storage("last_event_id", last_event_id)
        
        // Process event...
        if (event.type == 'changedOrCreated') {
            handle_change(event)
        }
    }
}
```

**Important**: Persist the `last_event_id` (e.g., to disk or database) so reconnection 
works even after application restart.

### Reconnecting with Last-Event-Id

Include the `Last-Event-Id` header when reconnecting:

```
function connect(reconnect = false) {
    headers = {
        "X-Auth-Token": ACCESS_TOKEN,
        "Accept": "text/event-stream",
        "Content-Type": "application/json"
    }
    
    // Add Last-Event-Id if reconnecting
    if (reconnect && last_event_id != null) {
        headers["Last-Event-Id"] = last_event_id
    }
    
    body = {
        "observedDirectories": [dir_id],
        "observedAttributes": ["name", "size", "mtime"]
    }
    
    return sse_client.connect(url, headers, body)
}
```

**What happens**:
- **Caught up**: Connect directly to main monitor, receive live events
- **Behind**: System creates temporary catching monitor, replays missed events, 
  seamlessly transfers to live stream

### Handling seamless takeover

Takeover from catching monitor to main monitor is transparent - you just receive 
events continuously with no gaps or duplicates. No special handling needed!

```
// This works automatically - no special code needed
for event in sse_stream {
    process_event(event)
    // Events seamlessly transition from replay to live
}
```

**Event Id guarantees**:
- ✅ No duplicates: Each sequence number appears exactly once
- ✅ Ordered: Events arrive in strictly increasing sequence order

## Essential client patterns

### 1. Always verify file existence

**Problem**: Events may arrive out of order - you might receive a `changedOrCreated` 
event after a `deleted` event for the same file.

**Solution**: Check if file was deleted before processing changes.

```
class FileCache {
    files = {}              // file_id → attributes
    deleted_files = set()   // Set of deleted file IDs
    
    function handle_event(event) {
        file_id = event.data.fileId
        
        if (event.type == 'deleted') {
            deleted_files.add(file_id)
            files.remove(file_id)
            ui.remove_file(file_id)
        }
        else if (event.type == 'changedOrCreated') {
            // CRITICAL: Check if file was already deleted
            if (file_id in deleted_files) {
                return  // Ignore - file no longer exists
            }
            
            // Safe to process
            files[file_id] = event.data.attributes
            ui.update_file(file_id, event.data.attributes)
        }
    }
}
```

### 2. Handle duplicate deletions idempotently

**Problem**: You may receive multiple `deleted` events for the same file.

**Solution**: Make deletion idempotent - safe to call multiple times.

```
function handle_deletion(file_id) {
    // Idempotent deletion
    if (file_id in files) {
        files.remove(file_id)
        ui.remove_file(file_id)
    }
    // If already deleted, do nothing
}
```

### 3. Compare received data with cache

**Problem**: You may receive events where your requested attributes didn't actually change.

**Solution**: Compare new attributes with cached values before updating UI.

```
function handle_change(file_id, new_attrs) {
    old_attrs = files.get(file_id)
    
    if (old_attrs == null) {
        // New file
        files[file_id] = new_attrs
        ui.add_file(file_id, new_attrs)
        return
    }
    
    // Check if actually changed
    if (new_attrs != old_attrs) {
        files[file_id] = new_attrs
        ui.update_file(file_id, new_attrs)
    }
    else {
        // False positive - log but don't update UI
        log.debug("Spurious change event for " + file_id)
    }
}
```

### 4. Maintain local state

**Why**: Essential for handling out-of-order events, duplicates, and false positives.

```
class FileMonitoringClient {
    // Track file state
    files = {}              // file_id → attributes
    deleted_files = set()   // Set of deleted file IDs
    
    // Track connection state
    last_event_id = null
    connected = false
    
    function on_event(event) {
        // Always update last_event_id first
        last_event_id = event.id
        
        file_id = event.data.fileId
        
        if (event.type == 'deleted') {
            handle_deletion(file_id)
        }
        else if (event.type == 'changedOrCreated') {
            handle_change(file_id, event.data.attributes)
        }
        else if (event.type == 'heartbeat') {
            // Just update last_event_id (already done above)
        }
    }
}
```

## Complete client example

Full implementation with reconnection, state management, and error handling:

```
class SpaceFilesMonitorClient {
    // Configuration
    oneprovider_url
    space_id
    access_token
    observed_dirs
    observed_attrs
    
    // State management
    files = {}              // file_id → attributes
    deleted_files = set()   // Set of deleted file IDs
    last_event_id = null
    connected = false
    
    // === Connection Management ===
    
    function connect(reconnect = false) {
        url = oneprovider_url + "/api/v3/oneprovider/spaces/" + space_id + "/events/files"
        
        headers = {
            "X-Auth-Token": access_token,
            "Accept": "text/event-stream",
            "Content-Type": "application/json"
        }
        
        // Add Last-Event-Id for reconnection
        if (reconnect && last_event_id != null) {
            headers["Last-Event-Id"] = last_event_id
            log.info("Reconnecting from event ID: " + last_event_id)
        }
        
        body = {
            "observedDirectories": observed_dirs,
            "observedAttributes": observed_attrs
        }
        
        try {
            sse_client = SSEClient.connect(url, headers, body)
            connected = true
            log.info("Connected to event stream")
            return sse_client
        }
        catch (error) {
            log.error("Connection failed: " + error)
            throw error
        }
    }
    
    // === Event Processing ===
    
    function handle_event(event) {
        // Always update last_event_id
        if (event.id != null) {
            last_event_id = event.id
            save_to_storage("last_event_id", last_event_id)
        }
        
        if (event.type == 'changedOrCreated') {
            handle_file_change(
                event.data.fileId,
                event.data.parentFileId,
                event.data.attributes
            )
        }
        else if (event.type == 'deleted') {
            handle_file_deletion(
                event.data.fileId,
                event.data.parentFileId
            )
        }
        else if (event.type == 'heartbeat') {
            log.debug("Heartbeat received: " + event.id)
        }
    }
    
    function handle_file_change(file_id, parent_id, attrs) {
        // Check if file was deleted (out-of-order event)
        if (file_id in deleted_files) {
            log.debug("Ignoring change for deleted file: " + file_id)
            return
        }
        
        old_attrs = files.get(file_id)
        
        // Check if actually changed (avoid false positives)
        if (old_attrs != null && old_attrs == attrs) {
            log.debug("Spurious change event for " + file_id)
            return
        }
        
        // Update cache
        files[file_id] = attrs
        
        // Notify application
        if (old_attrs != null) {
            log.info("File updated: " + file_id)
            on_file_updated(file_id, attrs, old_attrs)
        }
        else {
            log.info("File created: " + file_id)
            on_file_created(file_id, attrs)
        }
    }
    
    function handle_file_deletion(file_id, parent_id) {
        // Idempotent deletion (handle duplicates)
        if (file_id in deleted_files) {
            log.debug("Duplicate deletion event for " + file_id)
            return
        }
        
        // Mark as deleted
        deleted_files.add(file_id)
        attrs = files.remove(file_id)
        
        log.info("File deleted: " + file_id)
        on_file_deleted(file_id, attrs)
    }
    
    // === Application Callbacks ===
    // Override these in your implementation
    
    function on_file_created(file_id, attrs) {
        // Handle new file
    }
    
    function on_file_updated(file_id, new_attrs, old_attrs) {
        // Handle file update
    }
    
    function on_file_deleted(file_id, attrs) {
        // Handle file deletion
    }
    
    // === Main Loop with Reconnection ===
    
    function run(max_reconnects = null) {
        reconnect_count = 0
        reconnect = false
        
        while (true) {
            try {
                // Connect (or reconnect)
                sse_client = connect(reconnect)
                reconnect_count = 0  // Reset on successful connection
                reconnect = false
                
                // Process events
                for event in sse_client.events() {
                    handle_event(event)
                }
            }
            catch (KeyboardInterrupt) {
                log.info("Shutting down...")
                break
            }
            catch (error) {
                log.error("Error: " + error)
                connected = false
                
                // Check reconnect limit
                if (max_reconnects != null && reconnect_count >= max_reconnects) {
                    log.error("Max reconnects reached, giving up")
                    break
                }
                
                // Exponential backoff
                reconnect_count++
                reconnect = true
                delay = min(2^reconnect_count, 60)  // Max 60 seconds
                log.info("Reconnecting in " + delay + "s (attempt " + reconnect_count + ")")
                sleep(delay)
            }
        }
    }
}

// === Example Usage ===

class MyFileMonitor extends SpaceFilesMonitorClient {
    function on_file_created(file_id, attrs) {
        print("✨ New file: " + attrs.name)
    }
    
    function on_file_updated(file_id, new_attrs, old_attrs) {
        print("📝 Updated: " + new_attrs.name)
        
        if (old_attrs.size != new_attrs.size) {
            print("   Size: " + old_attrs.size + " → " + new_attrs.size)
        }
    }
    
    function on_file_deleted(file_id, attrs) {
        name = (attrs != null) ? attrs.name : file_id
        print("🗑️  Deleted: " + name)
    }
}

// Configure and run
client = new MyFileMonitor(
    oneprovider_url = "https://oneprovider.example.com",
    space_id = "your-space-id",
    access_token = "your-access-token",
    observed_dirs = ["directory-object-id"],
    observed_attrs = ["name", "size", "mtime"]
)

client.run()
```

## Troubleshooting common issues

### Connection drops

**Symptom**: Connection closes unexpectedly

**Causes**:
- Network timeout
- Provider restart
- Token expiration

**Solution**: Implement automatic reconnection with exponential backoff (see example above).

### Missing events

**Symptom**: Some file changes not appearing

**Possible causes**:
1. **Not monitoring parent directory**: Only direct children of observed directories are monitored
2. **No permission**: You don't have access to read the file or its attributes
3. **Wrong attributes**: You requested attributes from a different document type

**Debug**:
```
// Check what directories you're monitoring
log("Observing: " + observed_dirs)

// Check what attributes you're requesting
log("Attributes: " + observed_attrs)

// Verify you have access to the directory
// (test via Oneprovider UI or API)
```

### Authorization failures

**Symptom**: Connection rejected with 401 or 403

**Causes**:
- Expired token
- Token doesn't have required caveats
- User not a space member

**Solution**:
```
// Verify token is valid
response = http.get(
    oneprovider_url + "/api/v3/oneprovider/user",
    headers = {"X-Auth-Token": access_token}
)
log("Token status: " + response.status_code)

// Check required permissions:
// - Must be space member
// - Token must allow: cv_api for op_space/{space_id}/file_events
```

## API reference

### Request format

**Method**: `POST`  
**Path**: `/api/v3/oneprovider/spaces/{spaceId}/events/files`  
**Content-Type**: `application/json`

**Body**:
```json
{
  "observedDirectories": ["dir1_object_id", "dir2_object_id"],
  "observedAttributes": ["name", "size", "mtime", "mode"]
}
```

### Headers

**Required**:
- `X-Auth-Token`: Your access token
- `Accept`: `text/event-stream`
- `Content-Type`: `application/json`

**Optional**:
- `Last-Event-Id`: Sequence number for reconnection (integer as string)

### Observable attributes

Available attributes to request:

**From file_meta document**:
- `name`, `index`, `type`, `activePermissionsType`, `posixPermissions`, `acl`
- `parentFileId`, `ownerUserId`, `hardlinkCount`, `symlinkValue`
- `originProviderId`, `directShareIds`

**From times document**:
- `mtime`, `atime`, `ctime`, `creationTime`

**From file_location document**:
- `size`, `isFullyReplicatedLocally`, `localReplicationRate`

## Next steps

- **[System Overview](../../design/files_monitoring/_overview.md)** - Understand the architecture
- **[Implementation Notes](../../design/files_monitoring/implementation_notes.md)** - Important caveats
- **[Reconnection Details](../../design/files_monitoring/reconnection.md)** - Deep dive into takeover protocol
