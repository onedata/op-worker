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

```python
class SpaceFilesMonitorClient(ABC):
    def __init__(
        self,
        ...
    ):
        self.last_event_id: str | None = None
        ...
        
    async def _handle_event(self, event: MessageEvent):
        self.last_event_id = event.last_event_id
        ...
    
```

**Important**: Persist the `last_event_id` (e.g., to disk or database) so reconnection 
works even after application restart.

### Reconnecting with Last-Event-Id

Include the `Last-Event-Id` header when reconnecting:

```python
async def _consume_stream(self, reconnect: bool = False) -> NoReturn:
    headers: dict[str, str] = {
        "X-Auth-Token": self.access_token,
        "Accept": "text/event-stream",
        "Content-Type": "application/json",
    }

    if reconnect and self.last_event_id is not None:
        headers["Last-Event-Id"] = self.last_event_id
    ...
```

**What happens**:
- **Caught up**: Connect directly to main monitor, receive live events
- **Behind**: System creates temporary catching monitor, replays missed events, 
  seamlessly transfers to live stream

### Handling seamless takeover

Takeover from catching monitor to main monitor is transparent - you just receive 
events continuously with no gaps or duplicates. No special handling needed!

```python
# This works automatically - no special code needed
async with sse_client.EventSource(
    url,
    option={"method": "POST"},
    headers=headers,
    json=body,
    ssl=self.verify_ssl,
    on_open=self.reset_backoff,
) as event_source:
    async for event in event_source:
        await self._handle_event(event)

```

**Event Id guarantees**:
- ✅ No duplicates: Each sequence number appears exactly once
- ✅ Ordered: Events arrive in strictly increasing sequence order

## Essential client patterns

### 1. Always verify file existence

**Problem**: Events may arrive out of order - you might receive a `changedOrCreated` 
event after a `deleted` event for the same file.

**Solution**: Check if file was deleted before processing changes.

```python
async def _handle_changed_or_created(self, data: dict) -> None:
    file_id: str = data.get("fileId")
    # If file is deleted ignore
    if file_id in self.deleted_files:
        return
    ...
```

### 2. Handle duplicate deletions idempotently

**Problem**: You may receive multiple `deleted` events for the same file.

**Solution**: Make deletion idempotent - safe to call multiple times.

```python
async def _handle_deleted(self, data: dict) -> None:
    file_id: str = data.get("fileId")
    if file_id in self.deleted_files:
        return
    ...
```

### 3. Compare received data with cache

**Problem**: You may receive events where your requested attributes didn't actually change.

**Solution**: Compare new attributes with cached values before updating UI.

```python
async def _handle_changed_or_created(self, data: dict) -> None:
    file_id: str = data.get("fileId")
    parent_file_id: str = data.get("parentFileId")
    attrs: dict[str, str | int] = data.get("attributes", {})
    cached_attrs: dict[str, str | int] = self.files.get(file_id, {}).copy()

    # First event about a file
    if not cached_attrs:
        self.files[file_id] = attrs
        await self.on_file_created(file_id=file_id, parent_file_id=parent_file_id)
        return
    
    updated_attrs: dict[str, str | int] = get_updated_attrs(attrs, cached_attrs)
    # Check if anything changed
    if not updated_attrs:
        return

    # Update cache
    self.files[file_id].update(updated_attrs)

    await self.on_file_updated(
        file_id=file_id,
        parent_file_id=parent_file_id,
        attrs=attrs,
        cached_attrs=cached_attrs,
    )

def get_updated_attrs(
    attrs: dict[str, str | int], cached_attrs: dict[str, str | int]
) -> dict[str, str | int]:
    return {k: attrs[k] for k in attrs if attrs[k] != cached_attrs[k]}
```

### 4. Maintain local state

**Why**: Essential for handling out-of-order events, duplicates, and false positives.

```python
class SSEEvent(Enum):
    CHANGED_OR_CREATED = "changedOrCreated"
    HEARTBEAT = "heartbeat"
    DELETED = "deleted"

    
class SpaceFilesMonitorClient(ABC):
    async def _handle_event(self, event: MessageEvent) -> None:
      event_type: str = event.type
      data_raw: str = event.data
      self.last_event_id = event.last_event_id
    
      try:
          data: dict = json.loads(data_raw)
      except json.JSONDecodeError:
          print(f"Cannot decode data: {data_raw!r}")
          return
    
      # Only put heartbeat event to the queue as event id is saved above
      if event_type == SSEEvent.HEARTBEAT.value:
          await self.heartbeat_events.put(
              (
                  event.last_event_id,
                  time.time(),
              )
          )
          return
      if event_type == SSEEvent.CHANGED_OR_CREATED.value:
          await self._handle_changed_or_created(data)
      elif event_type == SSEEvent.DELETED.value:
          await self._handle_deleted(data)
      else:
          print(f"Unknown event type={event_type}, data={data}")
```

## Complete client example

Full implementation with reconnection, state management, and error handling:

```python
import asyncio
import json
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import NoReturn, Final

from aiohttp_sse_client import client as sse_client
from aiohttp_sse_client.client import MessageEvent

INITIAL_BACKOFF_TIMEOUT: Final[int] = 1
MAX_BACKOFF_TIMEOUT: Final[int] = 60
BACKOFF_INCREASE_FACTOR: Final[int] = 2


class SSEEvent(Enum):
    CHANGED_OR_CREATED = "changedOrCreated"
    HEARTBEAT = "heartbeat"
    DELETED = "deleted"


class SpaceFilesMonitorClient(ABC):
    def __init__(
        self,
        oneprovider_authority: str,
        space_id: str,
        access_token: str,
        observed_dirs: list[str],
        observed_attrs: list[str],
        verify_ssl: bool = True,
    ):
        self.oneprovider_authority = oneprovider_authority.rstrip("/")
        self.space_id = space_id
        self.access_token = access_token
        self.observed_dirs = observed_dirs
        self.observed_attrs = observed_attrs
        self.verify_ssl = verify_ssl

        # fileId -> attrs
        self.files: dict[str, dict[str, str | int]] = {}
        self.deleted_files: set[str] = set()
        self.last_event_id: str | None = None
        self.first_event_id: str | None = None

        self.changed_or_created_events: asyncio.Queue[
            dict[str, dict[str, str | int]]
        ] = asyncio.Queue()
        self.heartbeat_events: asyncio.Queue[tuple[str, float]] = asyncio.Queue()

        self.backoff: int = INITIAL_BACKOFF_TIMEOUT

    # ======= PUBLIC ENTRYPOINT =======

    async def run(self) -> None:
        """
        Main loop
        """
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        while True:
            try:
                print("Connecting to SSE stream...")
                if self.last_event_id:
                    print(f"Last event id {self.last_event_id}")
                await self._consume_stream(reconnect=bool(self.last_event_id))
            except asyncio.CancelledError:
                break
            except Exception as e:  # pylint: disable=broad-exception-caught
                if loop.is_closed() or not loop.is_running():
                    print("Loop is closed, breaking run()")
                    break
                try:
                    print(f"Stream error: {e}\n Reconnect in {self.backoff} s")
                    await asyncio.sleep(self.backoff)
                except asyncio.CancelledError:
                    print("Cancelled during backoff sleep, shutting down")
                    break
                self.backoff = min(self.backoff * BACKOFF_INCREASE_FACTOR, MAX_BACKOFF_TIMEOUT)

        # clean up data structures to allow reusing this object after reconnection
        self.clean()

    async def _consume_stream(self, reconnect: bool = False) -> NoReturn:
        url: str = (
            f"https://{self.oneprovider_authority}/api/v3/oneprovider/spaces/"
            f"{self.space_id}/events/files"
        )

        headers: dict[str, str] = {
            "X-Auth-Token": self.access_token,
            "Accept": "text/event-stream",
            "Content-Type": "application/json",
        }

        if reconnect and self.last_event_id is not None:
            headers["Last-Event-Id"] = self.last_event_id

        body: dict[str, list[str]] = {
            "observedDirectories": self.observed_dirs,
            "observedAttributes": self.observed_attrs,
        }

        async with sse_client.EventSource(
            url,
            option={"method": "POST"},
            headers=headers,
            json=body,
            ssl=self.verify_ssl,
            on_open=self.reset_backoff,
        ) as event_source:
            async for event in event_source:
                await self._handle_event(event)

    async def _handle_event(self, event: MessageEvent) -> None:
        event_type: str = event.type
        data_raw: str = event.data
        self.last_event_id = event.last_event_id
        if self.first_event_id is None:
            self.first_event_id = event.last_event_id

        try:
            data: dict = json.loads(data_raw)
        except json.JSONDecodeError:
            print(f"Cannot decode data: {data_raw!r}")
            return

        # Only put heartbeat event to the queue as event id is saved above
        if event_type == SSEEvent.HEARTBEAT.value:
            await self.heartbeat_events.put(
                (
                    event.last_event_id,
                    time.time(),
                )
            )
            return
        if event_type == SSEEvent.CHANGED_OR_CREATED.value:
            await self._handle_changed_or_created(data)
        elif event_type == SSEEvent.DELETED.value:
            await self._handle_deleted(data)
        else:
            print(f"Unknown event type={event_type}, data={data}")

    async def _handle_changed_or_created(self, data: dict) -> None:
        file_id: str = data.get("fileId")
        parent_file_id: str = data.get("parentFileId")
        attrs: dict[str, str | int] = data.get("attributes", {})

        # If file is deleted ignore
        if file_id in self.deleted_files:
            return

        cached_attrs: dict[str, str | int] = self.files.get(file_id, {}).copy()

        await self.changed_or_created_events.put({file_id: attrs})

        # First event about a file
        if not cached_attrs:
            self.files[file_id] = attrs
            await self.on_file_created(file_id=file_id, parent_file_id=parent_file_id)
            return

        # Because the observed attributes are stored in different documents,
        # they are delivered in separate events. As a result,
        # a single logical file creation may be represented
        # by multiple events (e.g. 3–4 events), each containing a different
        # subset of the observed attributes.
        #
        # The first event received for a given attribute subset is treated as the
        # “file creation” event for that subset. Subsequent events for the same
        # subset are considered updates.
        if set(attrs.keys()) - set(cached_attrs.keys()):
            self.files[file_id].update(attrs)
            return

        updated_attrs: dict[str, str | int] = get_updated_attrs(attrs, cached_attrs)
        # Check if anything changed
        if not updated_attrs:
            return

        # Update cache
        self.files[file_id].update(updated_attrs)

        await self.on_file_updated(
            file_id=file_id,
            parent_file_id=parent_file_id,
            attrs=attrs,
            cached_attrs=cached_attrs,
        )

    async def _handle_deleted(self, data: dict) -> None:
        file_id: str = data.get("fileId")
        parent_file_id: str = data.get("parentFileId")

        if file_id in self.deleted_files:
            return

        self.files.pop(file_id, None)
        self.deleted_files.add(file_id)

        await self.on_file_deleted(file_id=file_id, parent_file_id=parent_file_id)

    def reset_backoff(self) -> None:
        self.backoff = INITIAL_BACKOFF_TIMEOUT

    # ======= Interface =======

    @abstractmethod
    async def on_file_created(self, file_id: str, parent_file_id: str) -> None:
        pass

    @abstractmethod
    async def on_file_updated(
        self,
        file_id: str,
        parent_file_id: str,
        attrs: dict[str, str | int],
        cached_attrs: dict[str, str | int],
    ) -> None:
        pass

    @abstractmethod
    async def on_file_deleted(self, file_id: str, parent_file_id: str) -> None:
        pass

    @abstractmethod
    def clean(self) -> None:
        self.files = {}
        self.deleted_files = set()


# ======= EXAMPLE IMPLEMENTATION =======


class SpaceFilesMonitorClientImpl(SpaceFilesMonitorClient):

    def __init__(
        self,
        oneprovider_authority: str,
        space_id: str,
        access_token: str,
        observed_dirs: list[str],
        observed_attrs: list[str],
        verify_ssl: bool = True,
    ):
        super().__init__(
            oneprovider_authority,
            space_id,
            access_token,
            observed_dirs,
            observed_attrs,
            verify_ssl=verify_ssl,
        )
        self.created_file_ids: asyncio.Queue[str] = asyncio.Queue()
        self.updated_file_attrs: asyncio.Queue[dict[str, dict[str, str | int]]] = (
            asyncio.Queue()
        )
        self.deleted_file_ids: asyncio.Queue[str] = asyncio.Queue()

    async def on_file_created(self, file_id: str, parent_file_id: str) -> None:
        await self.created_file_ids.put(file_id)

    async def on_file_updated(
        self,
        file_id: str,
        parent_file_id: str,
        attrs: dict[str, str | int],
        cached_attrs: dict[str, str | int],
    ) -> None:
        await self.updated_file_attrs.put(
            {file_id: get_updated_attrs(attrs, cached_attrs)}
        )

    async def on_file_deleted(self, file_id: str, parent_file_id: str) -> None:
        await self.deleted_file_ids.put(file_id)

    def clean(self) -> None:
        super().clean()
        self.created_file_ids = asyncio.Queue()
        self.updated_file_attrs = asyncio.Queue()
        self.deleted_file_ids = asyncio.Queue()


def get_updated_attrs(
    attrs: dict[str, str | int], cached_attrs: dict[str, str | int]
) -> dict[str, str | int]:
    return {k: attrs[k] for k in attrs if attrs[k] != cached_attrs[k]}
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
