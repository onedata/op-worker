Release notes for project op-worker
===================================

CHANGELOG
---------

### 20.02.2

-   **VFS-6853** Matching session cookie is now required to verify a GUI
    access tokens (they are used behind the scenes by the Onedata web
    applications), which increases security.
-   **VFS-6851** Fixed a security issue in Oneprovider share GUI.
-   **VFS-6746** Added available QoS parameters suggestion box in QoS
    expression editor.
-   **VFS-6732** New JSON and RDF metadata editor based on Ace Editor.
-   **VFS-6685** Added new REST API for removing custom file metadata
    (xattrs, json and rdf).
-   **VFS-6570** Showing loading indicator in file browser until file is
    available for download.
-   **VFS-6456** Show more details about lack of privileges when trying
    to perform various actions in GUI.
-   **VFS-6338** Enhanced API of the mechanism for importing existing
    data into Onedata spaces without need for copying the data. The
    mechanism is now called "storage import". Introduced modes of
    storage import: "manual" which allows for manual registration of
    files and "auto" which enables automatic detection and import of
    files from the storage. Introduced possibility to forcefully
    start/stop scans of auto storage import. Redesigned GUI related to
    storage import, adjusted to the new features.


### 20.02.1

-   **VFS-6668** Fix bug resulting in timeouts of workers after 30s.
-   **VFS-6645** Optimize changes querrying.
-   **VFS-6628** Extended harvesting configuration - it is now possible
    to select harvesting options, like metadata types to harvest or
    additional file details (fileName, spaceId), upon index creation.
    New metadata types are now harvestable - xattrs and rdf. Default
    values of HarvestingBackendType and HarvestingBackendEndpoint can
    now be set by Onezone admin - if set, these values can be omitted
    when creating a new harvester. New option (retry\_on\_rejection)
    allowing for all payloads rejected by the harvesting backend to be
    automatically analysed for offending data (e.g. fields that do not
    match the schema), pruned and submitted again.
-   **VFS-6580** Fixed bug that could block dbsync on-demand streams on
    multi-node deployments.
-   **VFS-6577** Improve data transfer performance to object storages
    (e.g. S3) by aligning transferred block size to the object size on
    target storage, thus minimizing the overhead necessary when updating
    a file object with partial content.
-   **VFS-6568** Introduced concept of readonly storage. If enabled,
    Oneprovider will block any operation that writes, modifies or
    deletes data on the storage. Such storage can only be used to import
    data into the space. Mandatory to ensure proper behaviour if the
    backend storage is actually configured as readonly.
-   **VFS-6547** Fixed switching between spaces in file browser GUI
    during upload.
-   **VFS-6535** Updated S3 SDK library to 1.8.7.
-   **VFS-6504** Added HTTP storage helper allowing registration of HTTP
    and HTTPS servers as storage sources for Onedata Spaces.
-   **VFS-6494** Introduced REST API for registering files.
-   **VFS-6474** Added initial support for XRootD storage, including
    direct access to XRootD storages and importing of legacy data sets
    stored on XRootD or EOS servers.
-   **VFS-6457** Added new publicly visible field to shares -
    description (supports the markdown format).
-   **VFS-6456** Do not allow the user to perform actions in the GUI
    related to transfers without the appropriate permissions.
-   **VFS-6455** Support for jumping to selected files in GUI, even if
    they are not visible on infinite-scroll list.
-   **VFS-6453** New Open Data and share description views with visual
    Dublin Core editor and Markdown editor.
-   **VFS-6450** Added file name and space id to harvested file
    metadata.
-   **VFS-6431** Added performance logs for object storages, which can
    generate CSV file containing all storage requests including their
    duration.
-   **VFS-6421** New generic GUI plugin for harvesters.
-   **VFS-6378** Onepanel GUI and REST API now explicitly block
    supporting a space with more than one imported storage (globally) -
    such operation was possible in the past but was never supported by
    the internal storage import logic and led to incoherent view on
    space data.
-   **VFS-6370** Create secure fold mechanism on model documents.
-   **VFS-6361** Added new REST api for creating transfers and viewing
    file distribution, accessible respectively under \`/transfers\` and
    \`/data/{fileId}/distribution\` paths. Old \`/replicas\`,
    \`/replicas-id\` and \`/replicas-view\` endpoints were deprecated
    and will be removed in next major release.
-   **VFS-6358** Optimization of files upload through GUI.
-   **VFS-6346** GUI improvements: added Oneprovider GUI notifications,
    better file selection, additional error handling, better file
    manager refresh UX, fixed overflow of context menu in file browser,
    fixes in responsive layout.
-   **VFS-6344** GUI: showing information if QoS requirement is
    impossible to be fulfilled.
-   **VFS-6343** Added delete account feature in GUI.
-   **VFS-6320** Old \`/spaces/{sid}/indexes\`,
    \`/spaces/{sid}/indexes/{index\_name}\`,
    \`/spaces/{sid}/indexes/{index\_name}/reduce\` and
    \`/spaces/{sid}/indexes/{index\_name}/query\` endpoints were
    deprecated and will be removed in next major release.
-   **VFS-6316** Added \`statfs\` support enabling preview of available
    storage in each space through oneclient, for instance using \`df\`
    or \`stat\` utilities.
-   **VFS-6288** Basic HA functionality (experimental) - protect
    Oneprovider from single node failure.
-   **VFS-6287** Integrate traverse pools with HA sub-system.
-   **VFS-6263** New experimental Quality of Service functionality. It
    is used to manage file replica distribution and redundancy between
    supporting Oneproviders. Users can define any number of QoS
    requirements for a file or directory. Each requirement consists of
    target replicas number and an expression that is used to select
    storages where the replicas should be placed ‐ it is matched against
    parameters that were assigned to storages by Oneprovider admins.
-   **VFS-6261** Integrate high-level services with HA sub-system.
-   **VFS-6225** Added new \`triggers\` field to changes stream
    specification allowing to send events only on specified docs types
    changes.
-   **VFS-6184** Added the space owner concept. Space owner works like
    \"root\" within the space - such user is allowed to perform all
    file/API operations, regardless of the assigned privileges and file
    permissions / ACLs. Ownership can be assigned to any number of
    users, and it is forbidden to leave a space without an owner -
    ownership must be transferred first.
-   **VFS-6167** Allow nodes adding and deleting in-fly basing on HA
    sub-system.
-   **VFS-6160** Reorganized Local User Mapping (LUMA) management.
    Introduced feeds for populating LUMA DB.
-   **VFS-6095** Mask private file attributes, such as uid or gid, when
    showing file attrs in share mode.
-   **VFS-5648** Extended QoS expression to allow comparators (\<, \>,
    \<=, \>=) and numeric values. Changed \"-\" operator to \"\\\".
    Space characters (\" \"), dashes (\"-\") and underscores (\"\_\")
    are now allowed in QoS parameters. Added more details to invalid QoS
    expression errors.
-   **VFS-4760** Added implicit API caveats that limit access tokens
    used by Onedata GUIs behind the scenes for authentication and
    authorization. Different services in the system are presented with
    user\'s access token with power limited to bare minimum required for
    the service to handle user requests. For example, Oneproviders do
    not have access to APIs that could alter or delete user data and
    memberships.

### 20.02.0-beta4

### 20.02.0-beta3

-   VFS-5989 Regular files can now be shared. Also both files and
    directories can be shared multiple times. Due to those changes share
    REST API was reworked.

-   VFS-5901 Application config can now be customized with arbitrary
    number of config files added to /etc/op\_worker/config.d/ directory.

-   VFS-5989 Removed old share REST API operating on file paths/ids.

-   VFS-6149 Allowed for RPN expression in QoS entry create

-   VFS-6149 Implemented API for QoS gui

-   VFS-6076 Set oz\_domain env in initializer

-   VFS-6250 Improve logging related to running on-connect-to-oz
    procedures

-   VFS-6250 Secured on\_zone\_connection when cluster not ready

-   VFS-5983 Fix consistent\_hashing usage after deps update

-   VFS-5983 Support datastore HA

-   VFS-6140 Do not include cdmi attrs into inherited xattrs

-   VFS-6231 Added upgrade essential workers in node manager

-   VFS-6140 Refactor metadata management

-   ensure that sync does not invalidate remotely created file when
    finding empty file created by opening the file, before it is
    replicated

-   VFS-6237 fix sync not detecting deletions in nested directories

-   VFS-5998 Removed storage sync cancel when resupporting space

-   VFS-5830 optimize replica\_deletion and autocleaning mechanisms

-   VFS-6211 Fixed rtranfer\_worker not starting when no connection to
    onezone

-   VFS-6142 Fix get\_children\_attrs name collision detection

-   VFS-6142 Rename compute\_file\_attr fun to resolve\_file\_attr in
    attr\_req

-   VFS-5998 Implemented cease support cleanup

-   VFS-5830 fix bug in countdown\_server, remove commented out code

-   VFS-6142 Reduce get\_file\_attrs functions

-   VFS-6142 Improve get\_children, get\_children\_attrs/details docs

-   VFS-6181 Limit number of messages send to changes\_stream\_handler

-   VFS-6140 Add upper limits when listing directory children

-   VFS-6140 Return file itself when listing children for regular file

-   VFS-6142 Return name with collision suffix when fetching file
    details

-   VFS-6142 Add get file/children details fun to allowed ops in share
    mode

-   VFS-6142 Rename file\_info to file\_details

-   VFS-6142 Remove redundancies in attr\_req module

-   VFS-6142 Remove redundancies in dir\_req module

-   VFS-6142 Rename read\_dir\_plus\_plus to get\_children\_info

-   VFS-6142 Clean up lfm\_proxy code

-   VFS-6185 Minor fix to dbsync\_state

-   VFS-6185 Add seq timestamp to dbsync state

-   VFS-6142 Return file details instead of jsut attrs for gs get
    op\_file.instance

-   VFS-6142 Fix gui gs translations of fields returned by read dir plus
    plus

-   VFS-6142 Add read dir plus plus operation

-   VFS-6140 Add cases for requesting files on providers not supporting
    user

-   VFS-5983 Update deps to support datastore HA

-   VFS-6140 Fix using share object ids in REST endpoints

-   VFS-6140 Add data rest routes

-   VFS-6076 Do not provide a default Onezone domain

-   VFS-5830 add autocleaning\_view\_traverse, refactor autocleaning to
    use autocleaning\_view\_traverse

-   VFS-5830 scheduling task to replica\_deletion\_master blocks when
    its queue is full which results in automatic throttling

-   VFS-6192 Process GS prush messages asynchronously to avoid deadlocks

-   VFS-6043 Periodically report dbsync state to Onezone, synchronize
    info about support parameters and state from Onezone

-   change order of clear blocks - truncate operations

-   VFS-5642 Fixed paths bounded cache initialization

-   VFS-6093 Update ctool and onedata-documentation ref

-   VFS-6093 Do not send consumerToken to oz if it is undefined

-   VFS-6093 Add consumer token to verify\_access\_token request payload

-   VFS-6093 Fix races on auth\_cache reacting to token status changes

-   VFS-6081 reorder function in simple\_scan module

-   VFS-6100 Implemented cleanup after QoS entry was deleted. Improved
    QoS cache management.

-   VFS-5646 Added traverse cancel after QoS entry was deleted

-   VFS-6081 sync handles recreating file with different type

-   VFS-5642 Fixed wrong QoS cache management

-   VFS-6093 Rename auth\_manager proto\_credentials() to
    client\_tokens()

-   VFS-6093 Rename auth\_manager:auth() to auth\_manager:credentials()

-   VFS-5642 Fixed status check for file without QoS

-   VFS-6093 Set session\_validity\_check\_interval\_seconds to 15 by
    default

-   VFS-6093 Add debug logs to auth\_cache

-   VFS-6081 bugfix and fixed dialysis

-   VFS-6081 fix sync ignoring newly created file when rename of file
    with the same name and suffix is in progress

-   VFS-6081 fix races in detecting deletions by sync, fix sync
    reimporting file in case of race between sync, delete of opened file
    and release

-   VFS-6093 Extract auth cache logic from auth\_manager to auth\_cache

-   VFS-5642 Fixed race when dir deleted during traverse

-   VFS-6093 Properly handle all kinds of client() in gs\_client\_worker

-   VFS-6093 Fix auth manager mocks

-   VFS-6093 Improve auth\_manager docs

-   VFS-6093 Monitor oz token/temp token status gs pushes/notifications

-   VFS-6093 Monitor token status after successful verification

-   VFS-6106 Update ctool and cluster-worker refs

-   VFS-6093 Clean auth\_manager code

-   VFS-6081 fix importing conflicting files

-   VFS-5642 Added qos\_status doc fixed dialyzer

-   VFS-5633 Use uuid instead of filename in qos status link name

-   VFS-6093 Improve token\_auth verification code

-   VFS-6093 Invalidate auth cache entries on oz connection termination

-   VFS-6093 Make sure that auth verification works even without cache

-   VFS-6081 refactor fslogic\_delete:process\_file\_links function

-   VFS-6093 Add temporary\_token\_secret record

-   VFS-5642 Added specs in qos\_status

-   VFS-6096 Implemented QoS related documents clean up procedure

-   VFS-5642 Implemented QoS status check during traverse

-   VFS-6093 Add od\_token record

-   VFS-6093 add root\_auth and guest\_auth types to auth\_manager

-   VFS-6081 further refactor of fslogic\_delete module, add emitting
    events to storage\_sync

-   VFS-6093 Accept session:auth() in
    auth\_manager:get\_caveats/to\_auth\_override

-   VFS-6081 add checks for fixed races in storage\_sync, code refactor
    according to PR comments

-   Allow getting file\_location of deleted file if it is opened

-   VFS-6081 add extra check to prevent races which resulted in sync
    reimporting files that were being deleted

-   VFS-6081 fix sync reimporting files with file\_meta marked as
    deleted, minor refactor

-   VFS-6081 do not remove deletion\_link when removing file from
    storage fails, add deletion\_links for directories

-   Hotfix - rename opened deleted files - prevent race

-   Hotfix - rename opened deleted files, enotdir when needed and fix
    async\_request\_manager

### 19.02.4

-   **VFS-6635** Improve synchronization retries politics to prevent
    synchronizer blocking by dead providers.
-   **VFS-6631** Rtransfer takes into account storage block size
    choosing blocks to synchronize.
-   **VFS-6607** Fix node restart with HA disabled.
-   **VFS-6587** Replica synchronizer takes into account storage blocks
    size during choice of blocks to be replicated.
-   **VFS-6578** Fix events manager initialization to prevent races
    between events.
-   **VFS-6540** Files upload GUI optimization using optimal (per space)
    upload file chunk size.
-   **VFS-6438** Decrease overhead of transfers of already replicated
    files. Optimization of on demand synchronization streams usage.
-   **VFS-6401** All authentication errors are now wrapped in
    UNAUTHORIZED error and map to 401 HTTP code to avoid ambiguity when
    reporting token related errors - tokens can be used for
    authentication as well as input data for some operations (e.g.
    invite tokens).
-   **VFS-6390** Because of asynchronous processing, it was possible
    that GraphSync session cleanup intertwined with deleted record
    cleanup (that removes corresponding subscriptions from sessions,
    possibly including the session being cleaned up) and caused an error
    that interrupted change propagation. Now, if the session is no
    longer existent, subscription removal errors are ignored and the
    propagation completes.
-   **VFS-6369** Fix datastore internal call, batch management during
    links listing and infinite loop during storage directories creation.

### 19.02.3

-   Fix handling of remote helper params

### 19.02.2

VFS-6081 sync handles recreating file with different type VFS-6081 fix
races in detecting deletions by sync, fix sync reimporting file in case
of race between sync, delete of opened file and release VFS-6081 fix
importing conflicting files VFS-6081 add extra check to prevent races
which resulted in sync reimporting files that were being deleted
VFS-6035 Add VM option that forbids terminating the node with Ctrl + C
VFS-6008 fix listing transfers when not all docs are synchronized
VFS-5933 Remove the concept of default space

### 19.02.1

-   Bump version to 19.02.1
-   VFS-5826 Ensure db start at cluster init
-   VFS-5900 GUI update \* Added showing special subjects in ACL editor
-   VFS-5826 Add missing event during dir creation
-   VFS-5891 Clean authorization nonce after its TTL
-   VFS-5826 Add events during file creation
-   VFS-5826 Change max\_read\_dir\_plus\_procs value
-   VFS-5826 Emmit attr\_changed events on chmod and acl change
-   VFS-5826 Change events processing - allow subscriptions per dir

### 19.02.0-rc2

-   VFS-5775 fix file distribution data backend crashing on empty file
-   VFS-5777 increase gui session ttl
-   VFS-5795 Add rebar3 get-deps command to Makefile and use it in
    package build instead of upgrade
-   VFS-5758 Update file creation
-   VFS-5699 Implement cluster upgrade procedure

### 19.02.0-rc1

-   VFS-5706 do not allow to create file if it exists on other provider
-   ensure time\_slot\_histogram shift size is non-negative
-   VFS-5678 create gui session instead of rest one for gs connection
-   VFS-5683 GUI update Renamed space indices to views
-   VFS-5678 replace app.config index vars with view ones
-   VFS-5678 replace index with view when possible
-   VFS-5631 use specific type for \#subscription records
-   VFS-5678 rename indices routes to views routes
-   VFS-5523 return ENOTSUP for clproto msg impossible to reroute
-   VFS-5400 Use compatibility reference json
-   VFS-5622 refactor listing children (ls) API
-   VFS-5622 return ok tuple for moveFile and copyFile gs rpc
-   VFS-5622 set parent to null for space root dir in gs translator
-   VFS-5622 add revisions to op\_logic (due to gs version 3)
-   VFS-5622 add data validation and authorization for gs rpc operations
-   VFS-5622 add move and copy rpc operations to gs
-   VFS-5622 add deleting file to gs
-   VFS-5657 Adjusted deb dependency versions for bionic
-   VFS-5657 Enabled Ubuntu distribution package tag
-   VFS-5486 Use async GS client API in gs\_client\_worker and handle
    timeouts in more elegant way
-   VFS-5544 Add revision awareness to od\_\* records cache to fix a
    possible race condition caused by asynchronous updates
-   VFS-5551 Use nobody auth override when applicable
-   VFS-5508 Allow to view provider list of a space for users without
    the view privilege,
-   VFS-5540 replace old cdmi container handler with new one
-   VFS-5597 Added s3 sync helper params
-   VFS-5540 remove obsolete cdmi\_objectid\_handler
-   VFS-5540 remove obsolete pre\_handler
-   VFS-5540 add cdmi\_capabilities which shows available cdmi
    operations
-   VFS-5540 add generic cdmi\_handler
-   VFS-4698 Implemented onepanel rest proxy.
-   VFS-4547 add new swagger generated indices routes
-   VFS-4547 rename occurrences of `indexes` to `indices`
-   VFS-4547 rm transfer scheduling validation from lfm
-   VFS-4547 add new privilege checks to transfer and db\_index backends
-   VFS-5107 Fix inability to change webdav credentials type to none
-   VFS-5107 Remove webdav credentials when type is none
-   VFS-4547 check user space privileges in REST operations
-   VFS-5454 implement op\_logic framework
-   VFS-5454 omit validation for user root dir in op\_file operations
-   VFS-5454 always use provider id in ERROR\_SPACE\_NOT\_SUPPORTED\_BY
-   VFS-5454 rename logical\_file\_manager to lfm
-   VFS-5454 add missing http codes macros
-   VFS-5454 use ?ROOT\_SESS\_ID when checking user space privs
-   VFS-5454 silence no\_connections error in dbsync\_communicator
-   VFS-5454 add location header to rest POST responses
-   VFS-5394 Restart rtransfer on all nodes after helper change
-   VFS-5394 Report errors when refreshing helper params
-   VFS-5394 Restart rtransfer after storage params change
-   VFS-5394 Create module handling helpers reload
-   VFS-5394 Refresh helper params after storage modification
-   VFS-5394 Add ability to list handles for a session
-   VFS-5394 Call event emition upon helper modification
-   VFS-5545 Fix storage verification
-   VFS-5454 accept REST req with no content-type and no body
-   VFS-5107 Extract helper functions for storage args preparation
-   VFS-5454 differentiate rest\_errors from api\_errors
-   VFS-5454 update rest routes
-   VFS-5454 add generic rest\_handler
-   VFS-5454 add routes generated from swagger definitions
-   VFS-5454 replace lfm\_files with logical\_file\_manager calls
-   VFS-5107 Ensure empty credentials for credentialsType:=none
-   VFS-5107 Disallow insecure mode for glusterfs and nulldevice
-   VFS-5107 Luma: convert only integers to binaries
-   VFS-5107 Convert LUMA response to binaries before validating
-   VFS-5107 Add missing webdav arguments to list of allowed fields
-   VFS-5107 Fix helper creation badmatch
-   VFS-5107 Validate admin ctx and args on helper update
-   VFS-5107 Never expect integers in helper user/group ctx
-   VFS-5107 Make webctx adminId special case of ctx transform
-   VFS-5107 Reuse validation between helper args and ctx
-   VFS-5107 Fix helper ctx validation
-   VFS-5392 Add file listing via id
-   VFS-5392 Add tree traverse and effective values
-   VFS-5107 Implement storage modification

### 18.02.3

-   VFS-5706 do not allow to create file if it exists on other provider

### 18.02.2

-   VFS-5391 Reduced size of monitoring and throttling log files
-   do not call getxattr when synchronisation of nfs4acl is disabled
-   VFS-5289 Added default callback for providing resources. Added unit
    tests for selecting callback that provides resources.
-   VFS-5371 return einval error when client msg decoding fails
-   VFS-5329 Fix rmdir on s3 and ceph
-   VFS-5348 fix connection deadlock
-   VFS-5076 increase connection:send\_msg timeout
-   VFS-5076 rm race between setting up session and connection
-   VFS-5316 Improve event manager performance
-   VFS-5251 reorganize code in storage\_detector
-   VFS-5320 improve auto-cleaning *additional precondition was added
    that allows to check whether replica still needs to be deleted*
    improved tests
-   VFS-5316 Improve sequencer manager performance
-   VFS-5251 improve verification of newly added storage
-   update file-popularity after scheduled replication
-   VFS-5076 rm unused fields from session model
-   VFS-5076 fill proxy info before sending msg in communicator
-   VFS-5076 add retries to functions in communicator
-   VFS-5076 change directory tree in session
-   VFS-5232 Update acl usage by rules
-   VFS-5076 add retries to communicator\'s communicate
-   VFS-5232 Improve file deletion verification by request handlers
-   VFS-5232 Improve rules verification
-   VFS-5076 refactor communicator
-   VFS-5076 add keepalive timer to connection\_manager
-   VFS-5076 remove obsolete modules
-   VFS-5232 Update location management
-   VFS-5076 add sending heartbeats to connection\_manager
-   VFS-5232 Update ownership management
-   VFS-5250 Added documents printing
-   VFS-5076 add connection\_manager to session document
-   VFS-5076 refactor router
-   VFS-5232 Fix permissions management during read/write via proxy
-   VFS-5232 Update storage\_id generation
-   VFS-5076 implement msg send via connection\_manager
-   VFS-5232 Refactor delete
-   VFS-5076 rm connections management from session\_watcher
-   VFS-5076 add API call to close connection
-   VFS-5232 Refactor file opening
-   VFS-5232 Fix file handles management
-   VFS-5232 Fix sync/delete race
-   VFS-5232 Fix rename into opened file
-   VFS-5232 Fix some races in simple\_scan
-   VFS-5232 Refactor create
-   VFS-5076 rm heartbeats from new connection
-   VFS-5232 Fix critical section in replica\_deletion\_req
-   VFS-5076 replace gpb with enif\_protobuf, add socket mode option
-   VFS-5076 new connection implementation

### 18.02.1

-   VFS-5139 Handle remote events timeout
-   VFS-5180 improvements of storage\_sync mechanism: - sync now
    understands and properly handles suffixes for conflicted files -
    sync does not synchronize files for which deletion has been delayed
    due to existing handles - race between creation of file\_meta and
    file\_location docs has been removed, now file\_location is always
    created before file\_meta - this commit also adds tests of
    implemented improvements
-   VFS-5196 Added bearer authentication
-   VFS-5195 return error when file referenced by explicit name and
    tree\_id is not\_found
-   VFS-5165 return error when index referenced by explicit name and
    tree\_id is not\_found
-   VFS-5165 handle conflicting indexes\' names
-   VFS-5190 Add REST enpoint for creating shares, make sure only
    directories can be shared and only one share per dir is allowed
-   fix missing function clauses for encoding and decoding webdav
    credentials in luma\_cache module
-   VFS-5141 Fixed conflicting files names on storage
-   VFS-5139 Add notify when flush fails
-   VFS-5154 Clean no\_dot\_erlang scripts
-   VFS-4751 Ensure forward compatiblity of group/space privileges
    changing in future Onezone versions
-   VFS-5139 Improve events reliability
-   VFS-5161 Use new configuration endpoint for op compatibility
-   VFS-5161 Check both current and deprecated endpoints for
    compatibility check
-   VFS-5161 Add endpoint /api/v3/oneprovider/configuration
-   VFS-5142 Clean documents from memory when session is broken
-   VFS-5121 support modification of file-popularity-config
-   VFS-5121 introduce popularity function in the file-popularity view
-   VFS-5077 Update communicator
-   VFS-5023 refactor, move autocleaning util modules to utils directory
-   VFS-5110 check arguments order when calling
    aggregate\_file\_read\_events
-   VFS-5023 fix test, add 2 fields to auto\_cleaning\_run report to
    prepare for infinite scroll over reports
-   VFS-5023 code improvement, revert sorting order of
    file\_popularity\_view, fix tests
-   VFS-5077 Refactor provider communicator
-   VFS-5077 Refactor communicator
-   VFS-5077 Refactor auth managers
-   VFS-5077 Refactor session manager worker
-   VFS-5122 Update GlusterFS to 3.12.15 on CentOS
-   VFS-5077 Refactor session manager
-   VFS-5077 Refactor session open files management
-   VFS-5023 refactor of auto-cleaning
-   VFS-5077 Refactor session module
-   VFS-5077 Refactor handshake error handling
-   VFS-4650 Added writing provider macaroon to a file
-   VFS-4977 document new metadata changes API
-   VFS-5077 Further refactor of event\_router
-   VFS-5077 Create async request manager
-   VFS-4977 refactor space changes streaming
-   VFS-5021 Fixed changes stream not closing on disconnection
-   VFS-5077 Refactor streams routing
-   VFS-5077 Events refactoring
-   VFS-5046 escape specified map function when updating index
-   VFS-4970 Documents expire
-   VFS-4925 Added check of origin header in ws connection handshake
-   Updating GUI, including: VFS-5038-allow-empty-handle-metadata *allow
    empty handle metadata* fixes in share page \* fixed handle detection
    after save
-   VFS-4825 call replica\_deletion\_master:cancel on eviction
    cancellation
-   VFS-4825 replace eviction\_worker with new one using
    gen\_transfer\_worker
-   VFS-4825 add initial implementation of transfer\_worker\_behaviour

### 18.02.0-rc13

-   8.Updating GUI, including: VFS-4980-index-transfers \* VFS-4980
    Added support for index transfers
-   VFS-4966 Using common property for file path and index name in
    transfer
-   VFS-4966 Added missing db\_index id in GUI API
-   VFS-4998 fix provider emitting file\_written\_event to itself when
    mtime has changed
-   VFS-4978 stop calling dbsync\_changes on ignored remote docs
-   VFS-4975 enable creating indexes on additional models
-   VFS-4968 Fix minor bug in synchronizer
-   VFS-4966 Fixed undefined QueryViewParams
-   VFS-4966 fix index\_db\_data\_backend get\_record to return proper
    proplist
-   VFS-4966 add db-index gui model, modify transfer model
-   VFS-4976 Changed gui to use create\_and\_open
-   Updating GUI, including: VFS-4927-transfers-loading \* VFS-4927
    Fixed delay before loading transfers list; error handling
-   VFS-4998 ensure that update scan does not start to fast
-   VFS-4968 Fix link problems in cluster\_worker
-   VFS-4991 handle ENOENT in remove\_utils:rm\_children
-   VFS-4978 rm db view from provider when he no longer supports index
-   VFS-4991 fix race on cleaning up directory from storage
-   VFS-4902 Added WebDAV helper
-   VFS-4902 Added proxygen library dependency
-   VFS-4902 Added WebDAV helper integration test
-   handle undefined FileCtx in import\_children. This situation occurs
    when files was deleted, and its link has been already deleted too,
    but it is still visible on storage
-   VFS-4968 Fix remote store
-   VFS-4614 Rename email\_list to emails in user record
-   VFS-4952 Implement provider modification using graph sync
-   Updating GUI, including: VFS-3774-safari-opensans \* VFS-3774 Fixed
    broken request for OpenSans font in Safari if redirecting to Onezone
-   VFS-4936 Implement supporting space via graph sync
-   VFS-4951 fix cleaning after removing file\_meta doc, filter out
    deleted custom\_metadata docs from indexes
-   VFS-4707 remove soft\_quota\_limit
-   VFS-4936 Use common errors API for space support change
-   VFS-4743 Refactor link trees
-   VFS-4936 Add function to change space support size

### 18.02.0-rc12

-   Releasing new version 18.02.0-rc12

### 18.02.0-rc11

-   VFS-4830 add add\_reduce function
-   Updating GUI, including: VFS-4454-login-loader-close \* VFS-4454 Fix
    hanging authorization loader spinner
-   Upgrade rtransfer\_link.
-   VFS-4922 add parsing provider list in query string to validator
-   VFS-4614 Rename custom\_static\_root to static\_root\_override
-   VFS-4924 Fix move into itself check
-   VFS-4830 add migration by index tests
-   VFS-4830 enable replica eviction by index, add tests for that
-   Updating GUI, including: VFS-4856-cdmi-object-id \* VFS-4856 Added
    CDMI object ID and file path info to file row
-   VFS-4830 transfer scheduling by index
-   ensure that sync does not invalidate replicated files
-   VFS-4813 Update error on quota
-   VFS-4813 Throw sync errors
-   VFS-4816 Use local time instead of Zone time when Onezone is
    unreachable
-   VFS-4816 Do not ivalidate GraphSync cache immediately after a
    connection to Onezone fails
-   VFS-4778 - refactor replica\_synchronizer:get\_holes
-   VFS-4778 - simplify find\_overlapping logic, add unit tests for it
-   VFS-4813 Do not use local blocks in replica finder
-   VFS-4741 Prevent synchronizer from crush when provider is not
    connected to onezone
-   VFS-4741 Invalidation uses only public blocks
-   VFS-4741 Add async synchronization requests
-   VFS-4778 Add priority to transfer blocks in progress
-   VFS-4769 Do not get user document when it is not needed
-   Updating GUI, including: VFS-4806-fix-distribution-dir \* VFS-4806
    Fixed regression bug: file distribution modal for directory has no
    transfers buttons
-   Updating GUI, including: VFS-4791-many-chunks-render \* VFS-4791
    Support for highly scattered files in file distribution charts
-   VFS-4788 Add calculation of total blocks size in get\_distribution
    API
-   VFS-4788 Implement file chunks interpolation to avoid transferring
    large file blocks lists to GUI frontend
-   Updating GUI, including: VFS-4242-add-cancel-button-for-transfers \*
    VFS-4242 Added transfers cancelling and rerunning actions
-   VFS-4741 Force publishing of at least one block
-   VFS-4741 Flush synchrinizers on terminate
-   VFS-4741 Allow async local blocks flush
-   VFS-3858 Fix a minor bug in space\_data\_backend
-   VFS-3858 Update cluster\_worker ref, adjust to new Graph Sync proto
    version
-   VFS-4741 Update cluster\_worker to allow int keys in links
-   VFS-4757 Fix file permissions conversion in GUI backend, tidy up
    .gitignore
-   VFS-4741 Create local blocks store
-   VFS-4708 Minor fslogic cache fix
-   VFS-4708 Optimize sync and compute checksum
-   Upgrade rtransfer\_link.
-   VFS-4055 Update bp\_tree
-   VFS-4726 Add backwards compatibility for changing privileges in
    18.07

### 18.02.0-rc10

-   fix duplicated transfer links after restart
-   VFS-4611 Refactor od optimize replica\_synchronizer and blocks
    management
-   VFS-4146 Renmae ip\_string to ip in clproto
-   VFS-4146 Prevent silencing of failure in resolving peer domain
-   VFS-4146 Introduce proto message with rtransfer nodes
-   VFS-4029 Implement responding to Let\'s Encrypt http challenge
-   VFS-4660 Allow configuration of sync priorities
-   VFS-4660 Set sync priorities
-   VFS-4656 Added cephrados to luma cache
-   VFS-4660 Extend synch messages with priority option
-   VFS-4656 Added cephrados helper
-   VFS-4562 check permissions when scheduling invalidation job
-   VFS-4590 Update cluster-worker ref to include pings in Graph Sync
    connection
-   VFS-4652 sync files with existing links but missing file-meta
-   VFS-4574 improve transfer doc conflict resolution
-   VFS-4574 add transfer\_onf\_stats\_aggregator
-   VFS-4574 aggregate separately file blocks and transfer stats
-   VFS-4478 do not include deleted files in file\_popularity\_view
-   VFS-4478 implementation of replica\_eviction

### 18.02.0-rc9

-   VFS-4611 Update garbage collection
-   VFS-4608 Fix leak in rtransfer quota manager
-   Updating GUI, including: VFS-4566-file-transfers-tab \* VFS-4566
    Added transfers list tab for specific file
-   VFS-4584 Introduce limited history of ended transfers per file
-   VFS-4611 Change algorithm of overlapping blocks finding
-   VFS-4608 Fix tp internal calls and errors during changes application
-   VFS-4532 Update node\_package vars for all platforms
-   VFS-4532 Add autogenerated.config file to start params
-   Updating GUI, including: VFS-4507 \* VFS-4507 Multiple improvements
    in transfers list view
-   VFS-4482 Set default values in http listener to more reasonable
-   VFS-4412 Update memory management
-   VFS-4569 Updated helpers and rtransfer\_link
-   Updating GUI, including: VFS-4538 \* VFS-4538 Better control of
    invalidation feature availability for files
-   VFS-4542 Ensure op-worker is backwards compatible when group and
    space privileges change

### 18.02.0-rc8

-   Releasing new version 18.02.0-rc8

### 18.02.0-rc7

-   VFS-4412 Add holes consolidation
-   Updating GUI, including: VFS-4471 \* VFS-4471 Improved file chunks
    bar rendering
-   handle {error, ebusy} from truncate
-   VFS-4412 Fix remote driver
-   VFS-4412 Fix blocks synchronization bugs
-   VFS-4523 Allow listing transfers per space rather than per session
-   Updating GUI, including: VFS-4391 \* VFS-4391 More efficient
    infinite scroll for transfers list and file transfer status

### 18.02.0-rc6

-   Upgrade rtransfer\_link.
-   VFS-4412 Fix algorithm for excluding old blocks from transfers
-   not update file location only when mtime equals previously synced
    mtime
-   VFS-4412 Improve performance of file\_location blocks management
-   VFS-4477 - add audit log for storage\_sync

### 18.02.0-rc5

-   Update rtransfer\_link.
-   VFS-4422 Hotfix for broken public share download
-   VFS-4482 Do not include link ids in transfer ids (GUI backend)
-   VFS-3953 Integrate new GUI static backend

### 18.02.0-rc4

-   VFS-4510 fix finish\_time not set for cancellation of replication
    and invalidation and for failed invalidation
-   Updating GUI, including: VFS-4487 \* VFS-4487 Fixed not updating
    completed transfer stats
-   VFS-4510 fix active transfer being restarted after restart of
    provider
-   Updating GUI, including: VFS-4387 \* VFS-4387 Fixed not updating
    current transfers stats if user not scrolled view
-   VFS-4509 improve error handling in storage\_sync full\_update
    procedure
-   VFS-4481 Setting space transfer list limit to 250
-   VFS-4314 Add cleaning of tranfers history for files upon deletion or
    long inactivity
-   VFS-4314 Implement backend for transfers pagination in GUI
    *Implement API for GUI frontend to list transfers by ranges and
    negative offsets* Improve behaviour of transfer links *Add enqueued
    transfer state between scheduled and active* Add transferred\_file
    record for tracking which files are currently being transferred

### 18.02.0-rc3

-   VFS-4430 remove unused fields from space\_strategies record, fix bug
    in storage\_sync\_monitoring:get\_record\_struct function
-   VFS-4366 fix badmatch when updating space\_transfer\_stats
-   Reduce default num of rtransfer connections to 16.
-   Update rtransfer to fix missing blocks.
-   Updating GUI, including: VFS-3945 \* VFS-3945 Added data
    invalidation functionality
-   VFS-4431 Stabilize zone\_connection test, update ctool ref - include
    better handling of end\_per\_suite crashes, update meck, clean up in
    rebar.config
-   VFS-4407 Updated helpers and rtransfer\_link refs
-   Updating GUI, including: VFS-4355 \* VFS-4355 Added summarized
    transfer charts per provider
-   VFS-4265 check if replica target provider supports space
-   VFS-4366 refactor space\_data\_backend
-   VFS-4366 refactor space\_transfer\_stats\_cache

### 18.02.0-rc2

-   Upgrade rtransfer\_link.
-   VFS-4446 Updated jiffy ref
-   VFS-4427 ensure that sync does not updates files being replicated
-   VFS-4443 Fixed generation of source archive with submodules
-   VFS-4396 ensure that sync\_file\_counter has been deregistered
-   VFS-4417 do not update mtime when performing move on non-posix
    storage
-   VFS-4393 Upon connection failure, try to send responses via other
    connections of related session
-   VFS-4396 fix race when updating transfer stats
-   Update cluster\_worker to enable links listing with neg offset
-   VFS-4361 increase timeout for verify\_helper
-   VFS-4295 Changed subtrees to submodules
-   VFS-4361 add transfer stats aggregation and flushing
-   VFS-4394 Updated helpers and rtransfer refs
-   VFS-4241 added storage\_sync\_monitoring model, major refactor of
    storage\_sync mechanism
-   VFS-4393 Close incoming connection upon unexpected errors
-   VFS-4313 Updated pkg config with new aws sdk s3 version
-   VFS-4313 Updated dockers.config

### 18.02.0-rc1

-   VFS-2021 Added dockers.config
-   VFS-4238 Updated helpers and rtransfer rebar refs
-   VFS-4280 Added simulated filesystem options to null device helper
-   Updating GUI, including: VFS-4239 \* VFS-4239 Infinite scroll for
    transfers lists
-   VFS-4369 - emit file\_renamed event
-   VFS-4299 Update config
-   VFS-4369 create delayed storage file with root session id
-   Updating GUI, including: VFS-4305 \* VFS-4305 Added charts for
    on-the-fly transfers
-   VFS-4299 Extend ls with tokens
-   VFS-4299 Use links during files listing
-   VFS-4239 Using real start time for transfers in GUI backend
-   VFS-4304 erase zeroed histograms for on the fly transfers
-   VFS-4304 fix old comments and dialyzer specs
-   VFS-4238 fix not working updating stats for on the fly transfers
-   VFS-4239 Do not present active transfers on scheduled list
-   VFS-4304 add aggregated transfer stats for on the fly transfers
-   VFS-4239 Setting development version of GUI
-   VFS-4304 handle case when space transfer stats document do not exist
-   VFS-4304 update models to store and compute on the fly transfer
    stats
-   VFS-4304 store on the fly transfer data
-   VFS-4239 add listing of scheduled transfers
-   VFS-4239 Disabling transfer fetch limit

### 18.02.0-beta6

-   VFS-3732 - improve storage\_sync tests
-   VFS-3732 - fix tests after removing sticky bit on space directory
-   VFS-3732 - remove sticky\_bit on space\_dir in file\_meta
-   Update compatible versions
-   Releasing new version 18.02.0-beta6
-   Update vsn in app.src file
-   VFS-3731 - code style improvement
-   VFS-3731 - fix sync removing remotely created files, fix transfer
    model upgrader
-   VFS-3731 - fix dialysis
-   VFS-3731 - fix removing non-empty directory after remote deletion
-   VFS-4299 Add flush cooldown
-   VFS-4113 Updated exometer counters
-   VFS-4234 Updated rtransfer and helpers ref

### 18.02.0-beta6

-   VFS-3731 - fix sync removing remotely created files, fix transfer
    model upgrader
-   VFS-3731 - fix removing non-empty directory after remote deletion
-   VFS-4299 Add flush cooldown
-   VFS-4113 Updated exometer counters
-   VFS-4234 Updated rtransfer and helpers

### 18.02.0-beta5

-   don\'t count files\' attrs hash when update is set to
    write\_once
-   Upgrade rtransfer\_link.
-   VFS-4310 Update replica management
-   Updating GUI, including: VFS-4260 \* VFS-4260 Added menu for
    managing space root dir data distribution
-   VFS-4310 Update getting blocks for sync
-   VFS-3703 Switched from mochiweb JSON parsing to jiffy
-   VFS-4310 Update bp\_tree
-   VFS-4316 - remove filename\_mapping strategy
-   Fix session deletion
-   VFS-4316 deleted dirs should be removed from storage
-   fix user\_logic:exists
-   improve error handling in fetch\_lock\_fetch\_helper
-   fix displaying transfer status in GUI
-   VFS-4285 Fix restart of deletion\_worker
-   VFS-4285 Add create/delete test with sync
-   VFS-4285 Update getting deleted files
-   VFS-4272 Check forward compatiblity during OP connections to OZ
-   VFS-4285 Update files deletion
-   bugfix in create\_parent\_dirs
-   VFS-4296 Fixed meck entry
-   VFS-4267 Adjust code to erl 20, update deps
-   Enable disabling rtransfer ssl in app.config
-   VFS-4281 pass UserCtx to create\_delayed\_storage\_file function
-   VFS-4273 - reverse\_luma cache refactored, tests fixed, added specs
-   VFS-4273 - handled direct luma cache
-   VFS-4273 - refactor of luma\_cache, cache is now implemented using
    links
-   VFS-4274 Fixed helpers ref in rebar.config
-   VFS-4262 Updated rtransfer link
-   VFS-4262 Updated helpers with new asio version
-   refactor sfm\_utils:create\_parent\_dirs function
-   VFS-4244 Scale helpers tests
-   VFS-4152 Fix missing event messages and performance test
-   VFS-4152 Extend rtransfer stress tests
-   Add rtransfer tests

### 18.02.0-beta4

-   VFS-4274 Fixed helpers ref in rebar.config
-   refactor sfm\_utils:create\_parent\_dirs function
-   VFS-4262 Updated rtransfer link
-   VFS-4262 Updated helpers with new asio version

### 18.02.0-beta3

-   VFS-4249 storage\_sync counters bugfixes and improvements
-   Enable graphite support for rtransfer\_link.
-   Enable SSL for rtransfer\_link.
-   Update storage select
-   Improve rtransfer prefetching counting.
-   Integrate rtransfer\_link.
-   Switch to rebar-dependency helpers.
-   VFS-4114 - bugfix in storage\_sync histogram
-   VFS-4155 add client keepalive msg handling
-   VFS-4155 increase cowboy and clproto timeouts.
-   VFS-4171 Added folly dependency
-   VFS-4171 Updated cberl ref to LibEvent based version
-   Updating GUI, including: VFS-4157 \* VFS-4157 Requesting completed
    transfers list with delay to be compatible with backend fixes
-   VFS-4222 Force gs connection start only on the dedicated node
-   VFS-4217 - fix race on increasing deleted\_files\_counter and
    checking whether sync has been finished
-   Updating GUI, including: VFS-4223 \* VFS-4223 Fixed long time of
    loading data distribution modal
-   VFS-4222 Separate graph sync status check from triggering reconnect
-   Updating GUI, including: VFS-4027 \* VFS-4027 Added support for
    peta-, exa-, zetta- and yottabytes
-   VFS-4158 Add socket timeout test
-   VFS-4114 - transfer management fixes and refactor: this commit
    includes: *fix for race condition on adding transfer to active
    transfers link* major refactor of transfer module and model *fix for
    handling error when replication of not yet synced file has been
    scheduled* added backoff algorithm for retrying transfers *added
    files\_to\_process and files\_processed counters in transfer model*
    refactor of multi\_provider\_rest\_test\_SUITE
-   VFS-4211 Restarting listeners is now done by node\_manager because
    some ets tables need to be created in the process
-   Updating GUI, including: VFS-4206 \* VFS-4206 Changed speed units on
    transfers view to bps
-   VFS-4209 Fix synchronization blocking with many spaces
-   VFS-4207 Do not match listener\'s stopping to ok to avoid crashes
    when it is not running
-   VFS-4207 Move listener restarting logic from onepanel to
    oneprovider, restart GS connection after ssl restart
-   VFS-4158 Fix root links scopes
-   VFS-4163 - use transfer timestamps as link keys
-   VFS-4158 Fix race in ensure connected
-   VFS-4158 Improve provider connection management
-   Updating GUI, including: VFS-4012, VFS-4154 *VFS-4012 Info about
    remote statistics on transfers view; fixed transfer row sort issues*
    VFS-4154 Dynamically adjust polling interval of transfers data;
    fixed transfer chart loading
-   VFS-4158 Fix minor bug in router
-   VFS-4158 Make connections asynchronous
-   VFS-4054 Make default IP undefined
-   VFS-4054 Delete check\_node\_ip\_address from node\_manager.
-   VFs-4054 Remove getting cluster ips from node\_manager\_plugin
-   VFS-4158 Fix batmatch in sequencer

### 18.02.0-beta2

-   Update app.config
-   fallback to admin\_ctx when luma is disabled
-   handle luma returning integer values
-   fix handling uid and gid from luma as integers
-   VFS-4036 Added flat storage path support
-   VFS-4040 Improve speed and reliability of datastore, improve
    permission cache and times update
-   disable http2
-   VFS-4128 Fix tp internal call on space\_storage
-   VFS-4035 Allow non-blocking provider messages handling
-   VFS-4130 Update ctool, adjust to new time\_utils API, fallback to
    REST when get\_zone\_time via GS fails
-   VFS-4035 Prevent blocking event stream by connection
-   VFS-4035 Handle processing status during provider communication
-   VFS-4035 Fix deadlock in storage file creation function
-   VFS-4124 Added posix rename handling for nulldevice and glusterfs
-   VFS-4117 Implement new read\_dir\_plus protocol
-   VFS-3704 update cowboy to version 2.2.2
-   VFS-4117 Control read\_dir\_plus threads number

### 18.02.0-beta1

-   VFS-4080 Verify other providers\' domains while connecting via IP
    addresses
-   VFS-3927 Remove support for IdP access token authorization and basic
    auth, only macaroon auth is now supported
-   VFS-3978 Do not distribute test CA with op-worker
-   VFS-3965 Provider now uses port 443 for protocol server
-   VFS-3965 Added an HTTP Upgrade Protocol procedure before handshake
-   VFS-3947 Use random nonce during every inter-provider connection to
    verify provider identity
-   VFS-3751 Ensure users can view basic info about their spaces and
    groups if they do not have view privileges
-   VFS-3751 Authorize providers using macaroons rather than
    certificates
-   VFS-3751 Rework provider communicator to work with macaroons and
    support verification
-   VFS-3751 Merge provider\_listener and protocol\_listener into one
-   VFS-3751 Add handshake message for providers
-   VFS-3635 Remove default OZ CA cert, download it every time before
    registration
-   VFS-3279 Implement new synchronization channel between OP and OZ
    (Graph Sync)
-   VFS-3730 Separate trusted CAs from certificate chain
-   VFS-3526 Implement subdomain delegation; Combine provider record
    fields \\\"redirection\_point\\\" and \\\"urls\\\" into
    \\\"domain\\\"
-   VFS-3526 Remove old dns plugin module
-   Refactor datastore models to integrate them with new datastore
-   Change links storing model to use dedicated links tree for each
    provider
-   VFS-4088 GUI: Fixed incorrect ordering and stacking of transfer
    chart series
-   VFS-4068 GUI: Fixed incorrect icons positioning in transfers table
-   VFS-4062 GUI: Remember opened space when switching between
    data-spaces-transfers views; fixes in data-space sidebar
-   VFS-4059 GUI: Fixed provider icon scaling in transfers view

### 17.06.2

-   Updating GUI, including: VFS-4088 \* VFS-4088 Fixed incorrect
    ordering and stacking of transfer chart series
-   VFS-4074 removes option start\_rtransfer\_on\_init, added
    gateway\_supervisor
-   VFS-4074 add rtransfer to supervision tree
-   Updating GUI, including: VFS-4068, VFS-4062, VFS-4059 *VFS-4068
    Fixed incorrect icons positioning in transfers table* VFS-4062
    Remember opened space when switching between data-spaces-transfers
    views; fixes in data-space sidebar \* VFS-4059 Fixed provider icon
    scaling in transfers view
-   VFS-3889 increase transfer\_workers\_num to 50, remove commented out
    code and todo
-   VFS-3889 restart transfers via rest, add test for many simultaneous
    transfers
-   Bump compatible versions to 17.06.0-rc9 and 17.06.0
-   VFS-3906 Transfer GUI displays charts from before several seconds
    rather than approximate them to present
-   VFS-3889 add cancellation of invalidation transfers

### 17.06.1

-   Releasing new version 17.06.1

### 17.06.0-rc9

-   VFS-3951 add zone connection test suite
-   VFS-4004 Update ctool to include safe ciphers in TLS
-   fix storage\_update not restarting after provider restart
-   move setting of rtransfer port to app.config, fix transfer
    destination being send as string \\\"undefined\\\" instead of null
-   lower rtransfer\_block\_size, increase number of transfer\_workers
-   do not allow provider to restart all transfers in supported space
-   Updating GUI, including: VFS-4002 \* VFS-4002 Showing transfer type
    (replication/migration/invalidation) in transfers table
-   change finalizing state of transfer in gui to invalidating, update
    finish time when invalidation is finished
-   Updating GUI, including: VFS-4000, VFS-3956, VFS-3595, VFS-3591,
    VFS-3210, VFS-3710
-   VFS-4000 Fixed fetching wrong transfer statistics for chosen
    timespan
-   VFS-3956 Fixed provider name tooltip rendering in migrate menu of
    data distribution modal
-   VFS-3595 Fixed locking ACL edit when switching between ACL and POSIX
    in permissions modal
-   VFS-3591 Fixed infinite loading of metadata panel when failed to
    fetch metadata for file
-   VFS-3210 Fixed displaying long text in basic file metadata keys and
    values
-   VFS-3710 Using binary prefix units for displaying sizes (MiB, GiB,
    etc.)
-   VFS-3951 add op ver compatibility check
-   VFS-3911 - mechanism for turning node\_manager plugins on/off in
    app.config, turn monitoring\_worker off by default
-   changes in throttling\_config
-   VFS-3951 add build\_version env var for op
-   VFS-3972 Fix attach-direct consoles in releases not being run with
    xterm terminal
-   VFS-3951 add rest endpoint for checking op version
-   add try-catch around rtransfer write, remove debug logs
-   fix bug in gateway\_connection:garbage\_collect function add
    retrying of fetching file chunk
-   VFS-3932 Reuse cluster worker graphite args
-   VFS-3911 - use exometer counter to control execution of
    storage\_import and storage\_update
-   VFS-3932 Added helper performance metrics
-   VFS-3892 Use weighted average (rather than arithmetic) to calculate
    transfer speeds between time windows, improve calculations on the
    edges of speedchart
-   VFS-3892 Move status field from transfer record to
    transfer-current-stat record
-   VFS-3857 make request\_chunk\_size and check\_status\_interval
    constants in replica\_synchronizer configurable in app.config
-   VFS-3857 canceling and automatic retries of transfers
-   fallback to admin\_ctx when luma is disabled
-   VFS-3811 Add exometer counters
-   handle luma returning integer values
-   Hotfix - Improve environment variables names
-   Hotfix - prevent rtransfer from crush
-   VFS-3864 Lower default request timeout to 30 seconds
-   do not restart transfers which has been deleted, create separate
    links trees for different spaces
-   add storageId and storageName to map\_group luma request
-   create files on storage with appropriate uids and gids

### 17.06.0-rc8

-   fallback to admin\_ctx when luma is disabled
-   handle luma returning integer values
-   Hotfix - Improve environment variables names
-   Hotfix - update deps
-   Hotfix - prevent rtransfer from crush
-   improvements according to PR
-   improve docs according to PR, please dialyzer
-   VFS-3864 Lower default request timeout to 30 seconds
-   do not restart transfers which has been deleted, create separate
    links trees for different spaces
-   add storageId and storageName to map\_group luma request
-   please dialyzer
-   create files on storage with appropriate uids and gids
-   VFS-3846 Do not mock oneprovider in initializer
-   Releasing new version 17.06.0-rc8
-   Update vsn in app.src file
-   do not restart failed transfers
-   Hotfix - update cluster\_worker
-   VFS-3851 - fix dialyzer
-   VFS-3851 - fix rtransfer not binding to many interfaces
-   VFS-3851 fix not casting replication of first file in the tree
    (replication was handled by transfer\_controller itself)
-   VFS-3851 - remove timeout from function awaiting rtransfer
    completion, delete old TODO
-   handling onedata groups in luma, added tests for luma improvements,
    cleaning docs after make\_file or create\_file failed
-   VFS-3813 Update tests
-   VFS-3813 Improve files creation performance
-   VFS-3813 Add comment to vm.args
-   refactor luma map\_group request
-   fix handling uid and gid from luma as integers
-   fix storage\_sync tests that use luma
-   handling onedata groups in luma, added tests for luma improvements
-   VFS-3813 Update get\_provider\_id
-   add spaceId to resolve\_group request, split resolving acl user id
    and group id to different functions
-   VFS-3808 Update deps
-   add case for onedata idp in reverse\_luma\_proxy:get\_user\_id
-   VFS-3808 Update deps
-   VFS-3808 Update deps
-   VFS-3808 Update deps
-   VFS-3808 Update deps
-   Update deps
-   Update deps

### 17.06.0-rc8

-   do not restart failed transfers
-   VFS-3851 - fix rtransfer not binding to many interfaces
-   VFS-3851 fix not casting replication of first file in the tree
    (replication was handled by transfer\_controller itself)
-   VFS-3851 remove timeout from function awaiting rtransfer completion
-   VFS-3813 Improve files creation performance
-   fix reverse luma not resolving onedata groups, add storage name to
    reverse luma request parameters
-   VFS-3686 create autocleaning links tree for each space

### 17.06.0-rc7

-   Fix failures connected with exometer timeouts
-   VFS-3815 Added erlang-observer as RPM build dependency
-   VFS-3686 allow to start space cleaning manually
-   VFS-3781 Added radosstriper library
-   VFS-3686 autocleaning API and model
-   Updating GUI, including: VFS-3710 - VFS-3710 Using binary prefixes
    for size units (IEC format: MiB, GiB, TiB, etc.)
-   Updating GUI, including: VFS-3668 - VFS-3668 Show file conflict
    names in files tree and change conflict name format to same as in
    Oneclient
-   VFS-3756 Repair session (prevent hang up)
-   VFS-3756 Update cluster\_worker to prevent provider from crush when
    database is down
-   VFS-3763 Fixed helpers namespace in NIF
-   VFS-3763 Updated to folly 2017.10.02
-   VFS-3753 - fix storage sync failing when luma is enabled

### 17.06.0-rc6

-   VFS-3693 Update exometer reporters management
-   VFS-3693 Reconfigure throttling

### 17.06.0-rc5

-   fix error that occurs when we try to count attrs hash of deleted
    file
-   fix fetching luma\_config

### 17.06.0-rc4

-   VFS-3682 Upgraded GlusterFS libraries
-   VFS-3663 Fix delete events and improve changes broadcasting
-   VFS-3616 parallelize replication of file
-   VFS-3705 recount current file size on storage when saving sequence
    of blocks
-   VFS-3615 resuming transfer after restart, fix of synchronization of
    links in transfer model
-   VFS-3705 fix quota leak
-   VFS-3701 Update logging and cluster start procedure
-   VFS-3709 add mechanism to ensure that exometer\_reporter is alive
-   VFS-3701 Better provider listener healthcheck
-   VFS-3666 Event emiter does not crush when file\_meta is not
    synchronized

### 17.06.0-rc3

-   VFS-3649 Emit attrs remote attrs change even if location does not
    exist
-   VFS-3500 Extend logging for wrong provider ids in tree\_broadcast
    messages
-   VFS-3449 set sync\_acl flag default to false
-   VFS-3549 Add endpoint for enabling space cleanup.
-   VFS-3500 Limit calls to storage when new file is created. Limit
    calls to storage\_strategies.
-   VFS-3549 Add list operation and histograms to transfers.
-   VFS-3549 Add transfer model.
-   VFS-3500 Do not create locations during get\_attrs
-   VFS-3567 Store missing documents in datastore cache
-   VFS-3449 adapting luma to new protocol, refactor of luma\_cache
    module, added tests of reverse\_luma and importing acls associated
    with groups, WIP
-   VFS-3449 adapting reverse luma for querying by acl username, groups
    handling, WIP
-   VFS-3541 Move file\_popularity increment from open to release.
-   VFS-3541 Do not migrate data during replica invalidation when
    migration\_provider\_id is set to undefined.
-   VFS-3449 storage\_sync supports NFS4 ACL, preparation of luma
    modules to support requests considering groups mapping, extended
    handling of acl principals in acl\_logic
-   VFS-3560 Updating GUI ref
-   VFS-3495 Improve rest error handling.
-   VFS-3444 Adjuster default helper buffer values in app.conf
-   VFS-3495 Update ctool and use its new util function for getting
    system time. Introduce hard open time limit to space cleanup.
-   VFS-3500 Configure throttling
-   VFS-3500 Use cache of parent during permissions checking
-   VFS-3495 Add parameters to file\_popularity\_view.
-   VFS-3495 Add histograms to file\_popularity model.
-   VFS-3498 Read\_dir+
-   VFS-3500 Reconfigure throttling
-   VFS-3495 Do not ivalidate partially unique file as root (we cannot
    guarantee its synchronization), add space cleanup test.
-   VFS-3500 Update couchbase pool size control
-   VFS-3500 Reconfigure cluster for better performance
-   VFS-3494 Add popularity views and use them in space\_cleanup.
-   VFS-3494 Move cleanup\_enabled flag to space\_storage doc.
-   VFS-3494 Add cleanup\_enabled flag to storage doc
-   VFS-3494 Add file\_popularity model tracking file open.
-   VFS-3494 Add invalidate\_file\_replica function to
    logical\_file\_manager and rest api.
-   VFS-3464 Added extended attributes support to storage helpers

### 17.06.0-rc2

-   fix overlapping imports
-   VFS-3470 Improve dbsync changes filtering and queue size control
-   VFS-3454 Use silent\_read in rrd\_utils.
-   Generate file\_meta uuid using default method.
-   VFS-3480 Remove file\_location links.
-   storage\_sync improvements: *use storage\_import\_start\_time* set
    queue\_type lifo in worker\_poll \* reset storage\_file\_ctx before
    adding job to pool
-   VFS-3430 Adjust stress tests to refactored file\_meta.
-   VFS-3430 Move periodic cleanup of permission cache to
    fslogic\_worker, refactor file\_meta.
-   VFS-3430 Adjust changes stream test to delayed creation of
    file\_location.
-   VFS-3430 remove file\_consistency.

### 17.06.0-rc1

-   VFS-3384 save last\_update\_start\_time and
    last\_update\_finish\_time in storage\_strategies
-   VFS-3384 save luma\_api\_key in luma\_config, fix storage\_sync
    chmod\_file\_update2 test
-   VFS-3448 Use single \'onedata\' bucket
-   VFS-3384 implementation of reverse\_luma and luma\_cache\_behaviour,
    update of luma tests
-   VFS-3378 Enabled native GlusterFS support on OSX
-   VFS-3363 Use no\_seq for saves to default bucket
-   Reconfigure couchbase pools

### 17.06.0-beta6

-   VFS-3366 Repair lost changes scheduling
-   VFS-3376 Use pipe character instead of dot to join and split
    associative ids in gui ids
-   VFS-3416 Change subscriptions updates
-   VFS-3415 Make shares in public mode be fetched using provider
    authorization
-   fix errors in space\_sync\_worker check\_strategies
-   VFS-3415 Fix a routing bug causing public share links malfuntion
-   VFS-3363 Use in-memory changes counter in streams
-   Add mising proxy\_via field in recursive invocation.
-   Fix provider\_communicator:send\_async/2
-   VFS-3361 Emit event on times update.
-   VFS-3409 Handle share requests in user context rather that provider
    context so all operation can be performed despite lack of support
-   VFS-3356 Add space\_storage/storage accessors
-   VFS-3361 Do not create empty replicated files.
-   VFS-3363 Improve dbsync performance
-   VFS-3289 backend for metrics of storage\_sync
-   VFS-3363 Fix concurent delete
-   VFS-3361 Return updated file\_ctx from
    storage\_file\_manager:new\_handle.
-   VFS-3361 Add \'storage\_file\_created\' field to file\_location.
    Split sfm\_utils\_create\_storage file into two functions creating
    file and location.
-   VFS-3361 Remove empty block from file\_location response.

### 17.06.0-beta4

-   VFS-3362 Update web-client
-   Enable storage helper buffering

### 17.06.0-beta3

-   Releasing new version 17.06.0-beta3

### 17.06.0-beta2

-   Added GlusterFS support
-   VFS-3344 Improve dbsync changes aggregation
-   VFS-3309 Remove message\_id model.
-   VFS-3350 Make sure that new permissions can be safely added to the
    system without breaking gui compliance
-   VFS-3350 Remove deprecated privilege names
-   VFS-3326 Fix dbsync recovery stream
-   VFS-3183 - refactor of storage\_sync
-   Decode cacert from pem into der format, when opening websocket
    connection.

### 3.0.0-rc16

-   Generate empty monitoring events in order to fill null data.
-   Send size of event in read/write events.
-   VFS-3183 Add fsync operation to fslogic
-   VFS-3233 Add support for sig v2 to AWS S3 helper
-   VFS-3248 Move xattrs from provider to fuse messages. Add create and
    replace flags to setxattr.
-   VFS-3017 Fix wrong index encoding.
-   VFS-3017 Emit file\_removed event when file removal is requested.
-   VFS-3187 Execute requests synchronously in connection process.
-   VFS-3187 Add trap\_exit flag to connection.
-   VFS-3017 Copy/remove files during move when non posix storage is
    used
-   VFS-3017 Enable file garbage collection, adjust tests to the new
    rename implemenetation
-   VFS-3025 Implement rename operation.
-   VFS-3025 Rewrite current remove implementation and delete rename
    operation.

### 3.0.0-rc15

-   Add token\_auth translator.
-   Disable storage helpers buffering
-   VFS-3233 Add support for sig v2 to AWS S3 helper
-   VFS-3244 Switch level of dbsync periodic status logs to debug
-   VFS-3244 Do not fail on deletion\_worker\'s init when we cannot
    list file handles for cleanup.
-   VFS-3244 Add file\_objectid to custom\_metadata document.
-   VFS-3251 Updating GUI to 3.0.0-rc15
-   VFS-3181 Add onezone URL to sessionDetails
-   Add service version info to sessionDetails in GUI
-   VFS-3213 Update cberl reference
-   VFS-3213 Add libcouchbase package dependency
-   VFS-3146 Update models specyfications
-   VFS-3146 Update hooks after datastore update
-   VFS-3146 Update datastore models to use new datastore API
-   VFS-3116 Handle chmod, truncate and updating timestamps in
    storage\_sync
-   VFS-3088 Update dbsync state and events
-   VFS-3116 Refactor storage\_import and space\_sync\_worker
-   VFS-3088 Integrate with refactored datastore

### 3.0.0-rc14

-   Dbsync uses datastore\_pool to dump documents to db
-   Update cluster\_worker reference
-   Do not fail dbsync posthook when we cannot chown file on storage.
-   Refactor event\_manager:get\_provider function.
-   Fix event proxying.

### 3.0.0-rc13

-   VFS-3118 Change default env value for custom gui root
-   VFS-3025 Add create\_and\_open operation to sfm and use it during
    file copying.
-   VFS-3097 Do not deserialize macaroons when it is not necessary
-   VFS-3025 Do not open file in logical\_file\_manager, use provided
    handle.
-   VFS-2961 Refactor functions duplicating code in od\_user module.

### 3.0.0-rc12

-   Update datastore caching mechanism - use dedicated processed instead
    of transactions
-   VFS-2991 Add consistent\_hashing library.
-   VFS-2719 Introduce limits to the frequency of reconnect attempts in
    subscriptions websocket client
-   VFS-2496 Fix a bug causing user updates not to include new spaces /
    groups
-   VFS-2496 Make sure new spaces and groups appear after creation
    despite not being yet synchronized from onezone
-   VFS-2496 Change relations in space-user\|group-permissions models
-   VFS-2910 Reduce number of helper system threads
-   VFS-2910 Update storage detection logic
-   VFS-2496 Allow updating default space in user data backend
-   VFS-2496 Migrate to fully relational model in gui backend
-   VFS-2909 Adjust code to updated ceph lib
-   VFS-2871 Update file\_consistency and dbsync
-   VFS-2835 Update change propagation controller
-   VFS-2793 Implement several simple space strategies
-   VFS-2808 Integrate new helpers.
-   VFS-2522 Do not fail when trash file index is found.
-   VFS-2829 Exclude root and guest sessions from file handles
-   VFS-2829 Use hidden file prefix for rename
-   VFS-2696 Add better error logging to backend for file acl update
-   VFS-2696 Rework file ACL model in GUI backend
-   VFS-2696 Fix a bug in GUI file rename that was breaking file paths
-   VFS-2723 Fix events routing for file subscriptions
-   VFS-2755 Send SyncResponse message with checksum and file\_location
    instead of sending solely checksum.
-   VFS-2860 Updating frontend to 3.0.0-rc12
-   VFS-2934 Enable storage helper buffering configuration
-   VFS-2856 Improve caching of rules result, inject modified file
    context into function arguments.
-   VFS-2856 Permission refactoring.
-   VFS-2856 Configure new log layout in lager.
-   VFS-2496 Push update of user record on every relation update
-   VFS-2496 Change relations in group-user\|group-permissions models
-   VFS-2496 Return unauthorized when trying to update a user other than
    the one with current session
-   VFS-2931 Reduce number of kept rotated log files
-   VFS-2910 Refactor LUMA modules
-   VFS-2856 Synchronize file before moving it between spaces.
-   VFS-2696 Refactor fslogic
-   VFS-2808 Integrate new helpers.
-   VFS-2696 Rework file permissions in GUI into one record containing
    POSIX and ACL perms
-   VFS-2696 Rework file ACL model in GUI backend
-   VFS-1959 Add and handle OpenFile, CreateFile and MakeFile msgs
-   VFS-2696 Implement file rename in GUI backend
-   VFS-2807 Repair mnesia overload by session\_watcher
-   VFS-2522 Add support for spatial queries.
-   VFS-2773 Subscribe for monitoring events on root session only.
-   VFS-2755 Do not send location update to the client who provoked the
    sync.
-   VFS-2742 Change API to work with GUID-based protocol.
-   VFS-2755 Send SyncResponse message with checksum and file\_location
    instead of sending solely checksum.

### 3.0.0-rc11

-   VFS-2773 Listen to more changes in /changes api and add a few new
    tests.
-   VFS-2764 Fix directories having 0B size in GUI
-   VFS-2764 Fix size of files being zero right after upload
-   VFS-2696 Change text/javascript to applicaiton/javascript
-   VFS-2696 Reroute events through proxy for open files.
-   VFS-2696 Fix wrong aggregarion of file\_attr event.
-   VFS-2733 Add REST routes to GUI listener
-   VFS-2733 Standarize app listeners

### 3.0.0-rc10

-   VFS-2742 Fix aggregation for update\_attr events.
-   VFS-2494 Updating GUI frontend reference
-   VFS-2703 Update mocking
-   VFS-2662 Account uploaded files in LS cache
-   VFS-2662 Fix a badly stacktrace in fslogic worker
-   VFS-2662 Append new files to the beginning of the files list
-   VFS-2662 Implement file creation compatible with pagination model
-   VFS-2662 Add ETS for LS results caching
-   VFS-2665 Add proper deserialization of handle timestamp in
    subscriptions.
-   VFS-2665 Update ctool and change handle timestamp type definition.
-   VFS-2524 Fix problems with acl protocol encoding, add tests for acl
    conversion.
-   VFS-2524 Add old acl conversion functions.
-   VFS-2400 Update to new ceph and aws libraries
-   VFS-2524 Improve translation of acl and xattr records.
-   VFS-2524 Add basic attributes to /attributes endpoint.
-   VFS-2667 Improve json encoder for DB operations
-   VFS-2524 Change format of attributes in rest.
-   VFS-2524 Fix wrong file owner in cdmi.
-   VFS-2524 Add copy operation to cdmi interface.
-   VFS-2665 Improve consistency checking in dbsync\_events module
-   VFS-2659 Add some new fields to subscriptions
-   VFS-2665 Add times as component of file\_consistency.
-   VFS-2665 Move times from file\_meta to separate model. Fix dbsync
    problems.
-   VFS-2573 Repair custom metadata propagation
-   VFS-2663 Update deps, update critical section and transaction usage
-   VFS-2659 Add some fields to records synchronized from OZ
-   VFS-2659 Refactor some filed names in records
-   VFS-2659 Rework user and group models
-   VFS-2659 OP no longer differentiates between groups and effective
    groups
-   VFS-2659 Rename spaces field in od\_user to space\_aliases
-   VFS-2659 Rename some of the key records in db
-   VFS-2593 Adapt stress tests to new mechanism allowing for running
    many test suites
-   VFS-2573 Invalidate permission cache propagation

### 3.0.0-rc9

-   VFS-2609 Fix error 500 when specifying wrong url for transfer
-   VFS-2609 Fix query-index invalid parameters
-   VFS-2609 Fix error 500 when requesting nonexistent transfer
-   VFS-2609 Handle metadata filter errors
-   VFS-2609 Handle invalid json as error 400

### 3.0.0-rc8

-   VFS-2625 Add tests for deletion and conflit resolving for handles
    and handle services
-   VFS-2625 Add support for public handles
-   VFS-2625 Do not use handle get or fetch
-   VFS-2625 Fix public share view not retrieving fiels correctly
-   VFS-2625 Fix handles not being properly retrieved via REST
-   VFS-2609 Add test of setting json primitives as metadata.
-   VFS-2524 Apply recommended changes.
-   VFS-2625 Add backends for handles and handle services
-   VFS-2524 Add move operation to cdmi, split move and copy tests.
-   VFS-2625 Add handle field to share record in data backend
-   VFS-2594 Make filters work with json metadata in arrays.
-   VFS-2625 Accound handles and handle\_serives in subscriptions tests
-   VFS-2625 Set default value of service properties in handle services
    to empty list
-   VFS-2625 Add handles and handle\_services to subscriptions
-   VFS-2626 Add handle field to share\_info

### 3.0.0-rc7

-   VFS-2567 Use ShareId and FileId in getPublicFileDownloadUrl public
    rpc call
-   VFS-2567 Use new approach to shared files displaying in public view
-   VFS-2567 Push container dir change upon share rename
-   VFS-2567 Share.file is now file rather than file-shared record
-   VFS-2567 Add file-property-shared record dedicated for shares view
-   VFS-2567 Add file-shared record dedicated for shares view
-   VFS-2567 Add container dir to share record
-   VFS-2567 Change name of fileProperty field in file public record
-   VFS-2567 Add reference to publi file from public metadata record
-   VFS-2567 Add public metadata record in file public record
-   VFS-2567 Make sure group type is an atom in onedata\_group fetch
-   VFS-2567 Allow getting only public data about a group
-   VFS-2567 Show shares only to users with space\_view\_data
-   VFS-2567 Fix json and rdf metadata not being properly deleted in
    update callback in data backend
-   VFS-2594 Add read and execute permission for others on space dir.
    Block guest users from reading non shared files
-   VFS-2567 Fix some bugs in code responsible for checking view
    privileges
-   VFS-2594 Add check of \'other\' perms for share files.
-   VFS-2567 Check view permissions in groups and shares gui backend
-   VFS-2594 Refactor lfm\_proxy module.
-   VFS-2594 Move xattr name definitions to header, do not alow direct
    modification of xattrs with \'onedata\_\' prefix.
-   VFS-2594 Add remove\_metadata operation.
-   VFS-2567 Check view permissions in space gui backend
-   VFS-2594 Add has\_custom\_metadata method to logical\_file\_manager.
-   VFS-2180 Implement support for read only spaces
-   VFS-2180 Add provider\'s ID to file\_attr message
-   VFS 2557 Update tests init/teardown
-   VFS-2456 Add metadata to public view
-   VFS-2456 Implement first version of metadata backend
-   VFS-2405 Add some error handling to group privileges
-   VFS-2405 Add some error handling to space privileges
-   VFS-2405 Add error handling when user is not authorized to manage
    shares
-   VFS-2555 Remove shares on file removal, add doc for share\_guid,
    decode oz 403 error as eacces.
-   VFS-2555 Add shares field to file attr.
-   VFS-2405 Adjust to new shares API in OP, fix a badmatch
-   VFS-2405 Use lfm API to create and delete share
-   VFS-2555 Implement remove\_share operation and move some logic out
    of share\_logic.
-   VFS-2555 Add Name parameter to create\_share operation.
-   VFS-2405 Implement share\_logic:delete
-   VFS-2555 Adjust fslogic\_proxyio\_test to shares.
-   VFS-2555 Improve share permissions and guest user management.
-   VFS-2555 Add protocol for operations on shares
-   VFS-2555 Add guest session, prepare api and tests for shares.
-   VFS-2405 do not use root session id in shares view
-   VFS-2405 Add mockup of public share data backend
-   VFS-2405 Add mapping in gui backend for the new space permission
    (manage shares)
-   VFS-2405 Further code refactor
-   VFS-2405 Adjust to new OZ model where shares are no longer spaces
-   VFS-2405 Add share specific parameters to space record

### 3.0.0-rc6

-   VFS-2180 Improve links conflict resolution
-   VFS-2582 Using GUI fix for blank notifications
-   VFS-2180 Adapt code to cluster\_worker\'s API change
-   VFS-2180 Improve dbsync implementation
-   VFS-2180 Use gen\_server2 instead of erlang\'s gen\_server module
-   VFS-2390 Fix handlers specification in REST API
-   VFS-2390 Update rebar to version 3
-   Update memory management
-   VFS-2180 Allow for concurrent file creation

### 3.0.0-rc5

-   VFS-2534 Use erlang:system\_time/1 instead of os:timestamp/0
-   VFS-2534 Skip dbsync state update if not changed
-   VFS-2543 Integrate gen\_server2
-   VFS-2446 Use default group type rather than undefined in group logic
-   VFS-2472 Convert metadata to from proplists to maps.
-   VFS-2472 Do not fail when user provides empty callback for replicate
    operation.
-   VFS-2540 add on\_bamboo variable to coverage target
-   VFS-2540 implement collecting .coverdata files in coverage.escript
    from many ct directories
-   VFS-2534 Improve events processing
-   VFS-2426 Add check\_perms operation to logical\_file\_manager.
-   VFS-2472 Add 1.1 as possible cdmi version, improve documentation.
-   VFS-2472 Handle acl identifier without \'\#\' separator.
-   VFS-2472 Add correct handling of key and keys parameters to
    query\_index handler.
-   VFS-2490 Update op-gui-default ref
-   VFS-2472 Add filter option to metadata PUT.
-   VFS-2472 Unify file identifiers in REST interface.
-   VFS-2472 Add checking permissions to REST API operations.
-   VFS-2472 Add listing and getting inherited xattrs to REST API.
-   VFS-2472 Add inherited option to listing and getting xattrs
    internals.
-   VFS-2472 Add inherited option to getting json metadata.
-   VFS-2472 Add json merging function.
-   VFS-2472 Add \'inherited\' option to list\_xattr and
    get\_metadata interface.
-   VFS-2472 Add escaping of user defined js function.
-   VFS-2309 oz test mock updated to match actual implementation
-   VFS-2309 implemented provider registration besed on public keys &
    updated tests
-   VFS-2309 listener starting fixes
-   VFS-2309 fixed public key encoding
-   VFS-2309 public key based identity endpoind

### 3.0.0-rc4

-   VFS-2384 Prevent unrelated events from being lost on crash.
-   VFS-2320 Move RRD databases to file system

### 3.0.0-RC3

-   VFS-2156 Remove GUI files
-   VFS-2311 Add private RPC to retrieve file download URL
-   VFS-2389 Change event stream management
-   VFS-2263 Do not create handles for created file if not needed
-   VFS-2189 Close connection after file upload failure
-   VFS-2319 Remove spawns on event emits
-   VFS-2402 Update cluster\_worker
-   Releasing new version 3.0.0-RC2
-   VFS-2273 Handle handshake errors
-   VFS-2233 Changing separate fuse request types to nested types
-   VFS-2336 Update LUMA API to swagger version
-   VFS-2303 Fix eunit tests.
-   VFS-2303 Add metadata-id endpoint.
-   VFS-2303 Add filters for getting metadata.
-   VFS-2303 Add query-index rest endpoint.
-   VFS-2340 Minor comments update
-   VFS-2303 Adjust query\_view function to handle any view option.
-   VFS-2303 Fix /index/:id PUT rest internal error.
-   VFS-2303 Add /index and /index/:id endpoints to rest API.
-   VFS-2269 Enable Symmetric Multiprocessing
-   VFS-2303 Store all user indexes in one file.
-   VFS-2303 Adjust metadata changes stream to the new metadata
    organization.
-   VFS-2303 Add index model.
-   VFS-2303 Add validation of metadata type.
-   VFS-2303 Add filtering by spaceID to views.
-   VFS-2303 Add view tests.
-   VFS-2303 Add better error handling for custom metadata.
-   VFS-2319 Reimplement monitoring using events
-   VFS-2303 Add support for rdf metadata.
-   VFS-2303 Move xattrs to custom\_metadata document.
-   VFS-2303 Add basic metadata operations.
-   VFS-2361 Turn off HSTS by default, allow configuration via
    app.config
-   VFS-2340, Update deps
-   Releasing new version 3.0.0-RC1
-   VFS-2049 Improve file\_consistency waiting for parent mechanism.
-   VFS-2049 Add waiting for parent\_links in dbsync hook.
-   VFS-2049 Fix file\_consistency wrong list ordering.
-   VFS-2303 Add custom\_metadata model.
-   VFS-2229 Add reaction to rename of external file\_location
-   VFS-2215 Disable blocks prefetching.
-   VFS-2215 Exclude file removal originator from event recipients.
-   VFS-2049 Make file\_consistency work after system restart.
-   VFS-1847 Refactor LUMA and helpers modules
-   Squashed \'appmock/\' changes from 71733d3..1f49f58
-   VFS-2049 Improve file\_consistency model.
-   VFS-2233 Extract file entry to generic fuse request
-   VFS-2049 Basic consistency checking before executing hook.

### 3.0.0-RC2

-   VFS-2336 Update LUMA API to swagger version
-   VFS-2303 Add metadata-id endpoint.
-   VFS-2303 Add filters for getting metadata.
-   VFS-2303 Add query-index rest endpoint.
-   VFS-2303 Adjust query\_view function to handle any view option.
-   VFS-2303 Add /index and /index/:id endpoints to rest API.
-   Fix reactive file displaying in GUI during file upload
-   VFS-2269 Enable Symmetric Multiprocessing
-   VFS-2303 Store all user indexes in one file.
-   VFS-2303 Adjust metadata changes stream to the new metadata
    organization.
-   VFS-2303 Add custom\_metadatada model to sync via dbsync.
-   VFS-2303 Add index model.
-   VFS-2303 Add validation of metadata type.
-   VFS-2303 Add filtering by spaceID to views.
-   VFS-2303 Add view tests.
-   VFS-2303 Add better error handling for custom metadata.
-   VFS-2340 Repair bug in storage file manager
-   VFS-2303 Add support for rdf metadata.
-   VFS-2303 Move xattrs to custom\_metadata document.
-   VFS-2340 Update file consistency management
-   VFS-2340 Add file consistency test
-   VFS-2329 Include data requested for sync in prefetching range.
-   VFS-2361 Turn off HSTS by default, allow configuration via
    app.config

### 3.0.0-RC1

-   VFS-2316 Update etls.
-   VFS-2292, Update dbsync batches storing
-   VFS-2215 Disable blocks prefetching.
-   VFS-2215 Exclude file removal originator from event recipients.
-   VFS-2215 Wrap event\_manager\'s handle\_cast in try/catch.
-   VFS-2292 Session managmenet update
-   VFS-2292 Minor initializer update
-   VFS-2292 Add os-mon
-   VFS-2250 Use wrappers for macaroon serialization
-   VFS-2214, Release handles for created files
-   VFS-2214, Update session management and lfm proxy

### 3.0.0-beta8

-   VFS-2254 Additional GUI model relations
-   VFS-2254 Always allow to get acl after creation.
-   VFS-2254 Return full acl record on create operation in file-acl
    backend..
-   VFS-2254 Change EAGAIN to EIO error on sync fail.
-   VFS-2254 Adjust file-acl protocol.
-   VFS-2197 Fail sync when rtransfer fails.
-   VFS-2254 Add acls to file\_data\_backend.
-   VFS-2115 Fix changing file GUID in request after merge
-   VFS-2115 Add file redirection to rename, add phantom files
    expiration
-   VFS-2115 Add file redirection

### 3.0.0-beta7

-   VFS-2225 Update GUI docker image
-   VFS-1882 Postpone deletion of open files
-   VFS-2170 Improve dbsync\'s protocol reliability
-   VFS-2143, Improve dbsync\_worker stashed changes management
-   VFS-2187 Add automatic file removal when upload fails
-   VFS-2187 Adjust rest\_test to new OZ client API
-   VFS-2187 Use new OZ REST client API from ctool that uses arbitrary
    Auth term rather than predefined rest client.
-   VFS-2039 Extract non-client messages from fuse\_messages

### 3.0.0-beta6

-   Update erlang tls
-   VFS-2112 Integrate monitoring with REST API
-   VFS-2109 Adjust cdmi tests to new error messages.
-   VFS-2108 Add prefetching for unsynchronized files.
-   VFS-2109 Accept Macaroon header with token, as auth method for REST.
-   VFS-2031 Improve queue flushing in dbsync
-   VFS-2031 Remove default space
-   VFS-2109 Add support for dir replication through REST api.
-   VFS-2109 Move rest error handling logic from
    cdmi\_exception\_handler to more generic
    request\_exception\_handler.
-   VFS-2019 Add space name to luma proxy call
-   VFS-1506 Make security rules more generic.
-   VFS-2081 Make dbsync singleton
-   VFS-2018 Add response after rename
-   VFS-1506 Fix sending file attributes after replica reconciliation.
-   VFS-1506 Include file gaps in file\_location\'s blocks.
-   VFS-1999 Use message origin instead of message sender as dbsync\'s
    provider context
-   VFS-1506 Add permission checking to utime operation.
-   VFS-2071 Adjust code to the new S3 helper
-   VFS-1999 Quota implementation
-   VFS-2018 Adding file renamed subscription
-   VFS-2018 Adding file\_renamed\_event
-   VFS-1854 Enable inter-provider sequencer

### 3.0.0-beta5

-   VFS-2050, Get file size update
-   VFS-2050, Repair errors in connections usage and dbsync batch
    applying
-   VFS-1987 group privileges as atoms
-   VFS-1772 unify imports in gui backend, add returned value to group
    join group
-   Increase limit for cdmi\_id, as guid of default space in production
    environment has 199 bytes.
-   VFS-1772 add relation to child groups in group record
-   VFS-2050, Extend multiprovider tests
-   Cache provider info pulled from onezone
-   Allow for zombie-file delete
-   Hotfix: Ignore sequencer messages that are received from provider
-   Hotfix: Fix sending changes of unsupported spaces
-   Ignore proxied subscription messages in router.
-   Ignore dbsync changes from unsupported spaces. Do not catch
    exceptions inside mnesia transactions (mnesia does not like it).
-   VFS-1772 update group logic concerning privileges
-   VFS-1772 align group logic with new group API
-   VFS-1987 set & get for nested group privileges
-   VFS-2059 change default create modes for files and directories
-   VFS-2059 use recursive remove in gui backend
-   VFS-2003 Add read\_event subscription to rest api.
-   VFS-1987 nested groups via fetch
-   VFS-1987 nested groups in subscriptions
-   VFS-2003 Add replicate\_file rest handler.
-   VFS-2003 Add rtransfer management api to fslogic.
-   VFS-1772 add backend for groups
-   VFS-2003 Reorganize rest modules.
-   VFS-1772 introduce models for system-user system-group
    system-provider

### 3.0.0-beta4

-   VFS-1995 Syncing locations update
-   Fixing updating times in rename interprovider
-   VFS-1999 Fix Write/read subscription translate
-   VFS-1618 Fix old rmdir usage
-   VFS-1671 Update cluster\_worker ref.
-   VFS-1618 Move configurable values to config
-   VFS-1618 Sort synchronization keys to avoid potential deadlocks
-   VFS-1975 Add uuid to release message, update release routing
-   VFS-1618 Add synchronization for file\_meta:rename
-   VFS-1854 Improve dbsync\'s temp state clearing
-   VFS-1854 Disable rereplication in dbsync
-   VFS-1954 Make session:get\_connections const.
-   VFS-1854 Fix GUI upload
-   VFS-1854 Fix uuid\_to\_path/2
-   VFS-1618 Fix storage files mode changing
-   VFS-1854 Fix merge
-   VFS-1964 Adjust permission tests to changes in required permissions
    for dir removal.
-   VFS-1854 Fix several cdmi tests
-   VFS-1964 Remove unnecessary unlink.
-   VFS-1964 Adjust existing implementation of recursive remove to
    behave like linux.
-   VFS-1618 Delete target file after checking all permissions, add ls
    assertions in tests
-   VFS-1618 Change tests to check acl on proper provider
-   VFS-1618 Change moving into itself detection to
    interprovider-friendly
-   VFS-1854 Fix fslogic\'s events subscribtion
-   VFS-1618 Improve permissions handling
-   VFS-1618 Enable grpca in rename tests
-   VFS-1887 Add missing implementation of release.
-   VFS-1854 Introduce logical\_file\_manager:release/1
-   VFS-1841 Fix target parent path usage
-   VFS-1841 Fix target path usage
-   VFS-1841 Change usage of fslogic\_req modules to
    logical\_files\_manager
-   VFS-1841 Use get\_file\_attr to check if target exists
-   VFS-1841 Use space\_info:get\_or\_fetch instead of
    oz\_spaces:get\_providers
-   VFS-1954 Implement Ceph helper tests.
-   VFS-1841 Fix timestamps update
-   VFS-1841 Fix usage of gen\_path after merge
-   VFS-1841 Fix chmod usage in rename
-   VFS-1841 Fix sfm file copy fallback
-   VFS-1781 Fix rename permissions annotations
-   VFS-1781 Inter-space and inter-provider rename
-   VFS-1618 First sketch of interspace rename

### 3.0.0-beta3

-   VFS-1932 Create StorageHelperFactory with unset BufferAgent.
-   VFS-1770 dissallow spaces with empty name
-   VFS-1953 Extracting times updating to functions, handling root space
-   VFS-1770 improve gui injection script
-   VFS-1747 Change checksum algorithm to md4.
-   VFS-1770 add polling mechainsm before onedata user is synced
-   VFS-1747 Add missing status to fuse\_response.
-   VFS-1747 Add checksum computing during sync.
-   VFS-1521 File GUID to UUID translation
-   VFS-1862 Integrate move implementation with cdmi. Add
    copy\_move\_test to cdmi\_test\_SUITE.
-   VFS-1798, enable cover
-   VFS-1521: Get providers for space from cache instead of OZ
-   VFS-1521: Fetch all space\_info data in space\_info:fetch
-   Adjust luma for chown operation.
-   VFS-1749 Use proper types in LUMA config
-   VFS-1751 Allow specifying request method in IAM calls
-   VFS-1596 Ceph permissions adjustment
-   VFS-1596 Refactor luma nif, use hex\_utils
-   VFS-1596 More readable LUMA tests
-   VFS-1596 Move LUMA internals to module
-   VFS-1596 Move app initialization to function
-   VFS-1596 Use dedicated credentials caches instead of luma response
-   VFS-1747 Fsync files after transfer.
-   VFS-1703 Add remove file event
-   VFS-1507 Omitting handle saving for root session
-   VFS-1596 Multi storage LUMA tests
-   VFS-1596 LUMA nif entry in Makefile
-   VFS-1507 Sending file handle in get\_file\_location
-   VFS-1596 Accessing Amazon IAM API from provider
-   VFS-1596 Python LUMA API description
-   VFS-1507 Sending file handle in get\_new\_file\_location, using
    handles in read and write
-   VFS-1596 Python LUMA implementation
-   VFS-1596 Update getting user details
-   VFS-1596 Ceph credentials mapping in provider
-   VFS-1596 Move LUMA logic to separate modules.
-   VFS-1596 LUMA and in-provider credentials mapping with switch
-   VFS-1596 Getting credentials from LUMA
-   Fix GUI download handler.
-   VFS-1768: Permissions table sorting
-   VFS-1768: Resetting old tokens after token modal close
-   VFS-1768: Sorting provider names in blocks table
-   VFS-1770 fix wrong size calculation
-   VFS-1768: Fixing token copy with selectjs - to not copy newline on
    start;

### 3.0.0-beta1

-   VFS-1802 Improve proxyio performance.
-   VFS-1521: Get providers for space from cache instead of OZ
-   VFS-1521: Resolve issues with too long document.key in dbsync\'s
    state
-   VFS-1768: BS Tooltip component; style improvements in file chunks
    modal
-   VFS-1768: Prevent opening space page when clicking on space settings
    icon; blocking Groups page with generic info message
-   VFS-1553: Improvements in permissions table; add users/groups action
    stub
-   VFS-1770 first reactive GUI for files
-   VFS-1553: Create and join space buttons/modals
-   VFS-1757 Change application ports availability checking procedure.
-   VFS-1549: Uploaded file name in upload widget
-   VFS-1549: Modification time display
-   VFS-1549: Dragging file on file browser initial support
-   VFS-1728-increase timeouts, timeouts definitions in separate file
-   VFS-1549: Added ember-notify
-   VFS-1745 Improve handling pending files.
-   VFS-1549: Permissions modal
-   VFS-1745 Use fslogic\_storage:new\_user\_ctx to generate uid and gid
    in chown function.
-   VFS-1746, Adjust db\_sync to new cluster\_worker
-   VFS-1549: Modals for create dir and file
-   VFS-1549: First modal for file browser
-   VFS-1549: File browser toolbar, with previous functions
-   VFS-1734 fix a bug in unique filename resolver
-   VFS-1734 server side file upload
-   VFS-1521 Enable cross-provider subscriptions
-   VFS-1629 added delete messages handling
-   VFS-1629 user included in subscription when gets session
-   VFS-1629 propagating updates to the datastore
-   VFS-1629 connected provider to the OZ (over websocket)
-   VFS-1629 registering connection under name
-   VFS-1521 Enable file\_location update in lfm
-   VFS-1629 simple user subscriptions
-   VFS-1521 Proxy read and write events
-   VFS-1521 Implement remote ProxyIO
-   VFS-1521 Improve logging
-   VFS-1521 Fixup provider proxy communication

### 3.0.0-alpha3

-   VFS-1598 Fix oz\_plugin module.
-   Add DBSync\'s stream restarter
-   VFS-1558: Changes in Polish i18n
-   Include Erlang ERTS include directory when building c\_src/ .

### 3.0.0-alpha2

-   VFS-1665 Pull in ctool with new Macaroons.
-   VFS-1405 Update cluster\_worker
-   VFS-1522 Find blocks to transfer in all file locations.

### 3.0.0-alpha

-   Dependencies management update
-   Add map for helpers IO service. Test open and mknod flags.
-   VFS-1524 Change space storage name to space ID. Resolve space name
    clash problem.
-   VFS-1504 Checking if directory is not moved into its subdirectory
-   VFS-1421 Change fslogic\_spaces:get\_space to return space when
    asking as root.
-   VFS-1421 Add malformed query string error message.
-   VFS-1484 Enable storage lookup by name.
-   VFS-1484 Set number of threads for Amazon S3 storage helper IO
    service.
-   VFS-1421 Send PermissionChangedEvent as list of events.
-   VFS-1421 Add translations for aggregated acl types.
-   VFS-1421 Handle proxyio exceptions, adjust lfm\_files\_test to new
    api.
-   VFS-1472 Add librados and libs3 package dependencies.
-   VFS-1472 Add IO service for Amazon S3 storage helper to factory.
-   VFS-1414 Swapping Limit and Offset arguments in lfm\_dirs:ls
-   VFS-1474 Changing matching to assertions, adding comments
-   VFS-1421 Change space\_id to file\_uuid in proxyio\_request.
-   VFS-1421 Add proper handling of accept headers in rest requests, fix
    some minor bugs.
-   VFS-1421 Chmod on storage with root privileges during set\_acl
    operation.
-   VFS-1421 Enable permission checking on storage\_file\_manager open
    operation.
-   VFS-1428 Add list of application ports to config file.
-   VFS-1426 Add gateways to a process group.
-   VFS-1421 Add permission control to storage\_file\_manager.
-   VFS-1421 Do not allow direct modification of cdmi extended
    attributes.
-   VFS-1421 Add mimemetype, completion\_status and transfer\_encoding
    management to logical\_file\_manager api.
-   VFS-1421 Add set\_acl, get\_acl, remove\_acl as separate fslogic
    requests, with proper permission control.
-   VFS-1421 Check permissions on rename operation, repair incorrect
    mock in fslogic\_req\_test\_SUITE.
-   VFS-1421 Return 401 in case of unauthorized access to objects by
    objectid.
-   VFS-1421 Perform fsync after creation of file throught REST request.
-   VFS-1428 Add user context to fslogic:get\_spaces function.
-   VFS-1148 adjust listeners to new cluster\_worker API
-   VFS-1428 Enable multiple ceph user credentials.
-   VFS-1148 add sync button in top menu
-   VFS-1426 Migrate rtransfer from 2.0
-   VFS-1421 Add acl validation, annotate with access checks common
    fslogic functions.
-   VFS-1148 allow choosing where to create new files and dirs
-   VFS-1148 add ability to create new files and dirs in gui
-   VFS-1421 Integrate acls with cdmi.
-   VFS-1148 file browser allows to preview text files
-   VFS-1148 working prototype of basic file browser
-   VFS-1421 Add acls to logical\_file\_manager, add acl setting
    integration test.
-   VFS-1421 Add groups to test environment.
-   VFS-1421 Add onedata\_group model and implement basic operations on
    acl.
-   VFS-1400 Add compilation utility script.
-   VFS-1148 first attempts at file manger page
-   VFS-1402 CDMI redirections based on trailing slashes.
-   VFS-1398 Add xattrs to onedata\_file\_api and cdmi\_metadata
    implementation.
-   VFS-1403 CDMI object PUT operation + tests.
-   VFS-1407 Add mechanism that will remove inactive sessions after
    timeout.
-   VFS-1404 Cdmi object get.
-   VFS-1397 Replace identity with auth in container\_handler.
-   VFS-1338 Cdmi container put.
-   Use Erlang cookie defined in env.json file while creating provider
    spaces.
-   VFS-1363 Add user context to all storage\_file\_manager operations
-   VFS-1382 fixed task manager test changing wrong env
-   VFS-1382 dns listener starts with cluster\_worker supervisor
-   VFS-1378 adjust to new ctool API
-   Create storages on provider.
-   VFS-1382 op-worker related work removed from cluster-worker
-   VFS-1382 node\_manager config extracted
-   VFS-1338 Implement mkdir operation, add tests of container creation
    to cdmi test
-   VFS-1382 separated packages meant to form cluster repo
-   VFS-1382 node\_manager plugin - extracted behaviour & ported
    implementation
-   Storage creation improvement
-   VFS-1339 Move cdmi modules to different packages. Implement binary
    dir put callback.
-   VFS-1218 check permissions while opening a file based on \\\"open
    flags\\\"
-   VFS-1289 Add performance tests for events API.
-   Fix pattern matching on maps.
-   VFS-1289 Extend set of event and sequencer tests.
-   VFS-1338 Extract api for protocol\_plugins. Implement dir exists
    callback.
-   VFS-1218 add lfm\_utils:call\_fslogic
-   Refactor of malformed\_request/2 and get\_cdmi\_capability/2.
-   Map instead of dict.
-   Include guard for cdmi\_errors.hrl.
-   Skeletons of capabilities handlers.
-   VFS-1289 Extend event manager with client subscription mechanism.
-   VFS-1327 Separate rest and cdmi as abstract protocol plugins.
-   VFS-1291 Add routing to cdmi object/container modules and add some
    tests.
-   Done users and groups; done getting token
-   VFS-1291 Add rest pre\_handler that deals with exceptions. Update
    ctool.
-   VFS-1291 Rearrange http\_worker modules hierarchy.
-   VFS-1255 Bump Boost to 1.58 for compatibility with client.
-   VFS-1218 merge delete\_file with unlink
-   VFS-1258, transactions skeleton
-   VFS-1218 implement attributes and location notification
-   VFS-1244 add possibility for client to update auth
-   VFS-1218 fix lfm read/write test
-   VFS-1242, Cache controller uses tasks
-   VFS-1242, Task pool
-   VFS-1242, Task manager skeleton
-   VFS-1217 Use RoXeon/annotations.
-   VFS-1218 add file\_watcher model
-   VFS-1194 add user context to StorageHelperCTX
-   VFS-1193 better connection handling
-   VFS-1194 initial helpers support
-   VFS-1199, cache dump to disk management update
-   VFS-1193 restart mcd\_cluster after connection failure
-   VFS-1199, forcing cache clearing once a period
-   VFS-1199, Saving cache to disk status management
-   VFS-1193 add configurable persistence driver
-   VFS-1172, use botan on host machine rather than throw in so files
-   VFS-1145 Integrate SSL2 into oneprovider.
-   implement generic transactions in datastore ensure file\_meta name
    uniqueness witihin its parent scope
-   VFS-1178, Cache controller uses non-transactional saves
-   move worker\_host\'s state to ETS table
-   use couchbase 4.0
-   VFS-1147 Integration with new protocol.
-   VFS-1147 Implementation of first operations on directories.
-   add disable mnesia transactions option
-   VFS-1129 Add deb build dependencies
-   VFS-1118, local tests controller added
-   implement mnesia links
-   VFS-1118, global cache controller added
-   VFS-1118, cache clearing skeleton
-   VFS-1115 Allow building RPM package.
-   VFS-1025, merge lb with develop
-   VFS-1053 Selecting explicit node for mnesia to join, instead of
    finding it randomly
-   VFS-1049 add check\_permissions annotation
-   VFS-1049 add initial fslogic file structure
-   VFS-1051 change worker startup order
-   implement datastore: \'delete with predicates\' and list
-   VFS-997 Add event stream periodic emission ct test.
-   VFS-997 Add event stream crash ct test.
-   VFS-997 Event manager ct test.
-   VFS-997 Add event utils and unit test.
-   VFS-1041, add send data endpoint to remote control
-   checking endpoints during healthcheck of http\_worker and
    dns\_worker
-   VFS-997 Change sequencer manager connection logic.
-   move session definitions to separate header
-   change location of message\_id header
-   extract certificate\_info to separate header
-   client\_communicator lib
-   VFS-1000, add logical and storage file manager\'s API design
-   oneproxy CertificateInfo message
-   new handshake
-   VFS-1000, add sequence support for response mocking
-   VFS-997 Add sequencer worker.
-   translation improvements
-   serialization improvements
-   VFS-1010 Make test master node discoverable through DNS.
-   client\_auth + basic integration with protobuf
-   VFS-997 Add sequencer dispatcher ct test.
-   VFS-997 Sequencer logic.
-   VFS-997 Add sequencer.
-   move datastore init to node\_manager
-   change created beam location to target dir
-   refactor worker\_host header
-   add input\_dir/target\_dir configuration
-   enable init\_cluster triggering when all nodes have appeared
-   rest/ccdmi function headers
-   remove request\_dispatcher.hrl
-   remove node\_manager.hrl
-   node\_manager refactoring
-   oneprovider app reformat + doc adjustment
-   http\_worker reformat + doc adjustment
-   redirector reformat + doc adjustment
-   session\_logic and n2o\_handler reformat + doc adjustment
-   rest\_handler reformat + doc adjustment
-   cdmi\_handler reformat + doc adjustment
-   dns\_worker reformat + doc adjustment
-   logger\_plugin reformat + doc adjustment
-   worker\_plugin\_behavior reformat + doc adjustment
-   worker\_host reformat + doc adjustment
-   client\_handler and provider\_handler reformat + doc adjustment
-   request\_dispatcher reformat + doc adjustment
-   oneproxy reformat + doc adjustment
-   gsi\_nif reformat + doc adjustment
-   gsi\_handler reformat + doc adjustment
-   node\_manager\_listener\_starter reformat + doc adjustment
-   node\_manager reformat + doc adjustment
-   cluster manager reformat + doc adjustment

### v2.5.0

-   VFS-965, full functionality of spaces page
-   Perform operations asynchronously in ws\_handler.
-   VFS-965, several funcionalities of page spaces
-   VFS-965, visial aspects of spaces page
-   VFS-965, first code for spaces page
-   VFS-959 Not sending notifications for a fuse that modifies a file.
-   set fuseID to CLUSTER\_FUSE\_ID during creation of file\_location
-   VFS-954, adjust to new file blocks API
-   setting fslogic context
-   VFS-939 Implement rtransfer.
-   VFS-954, implementation of data distribution panel
-   VFS-952 support for AttrUnsubscribe message
-   VFS-593, GR push channel messages handling
-   getting size from available blocks map, instead of storage
-   creating file location for remote files
-   creating file location for empty remote files moved to
    get\_file\_location
-   VFS-940 Subscribing for container state events.
-   VFS-940 Add rt\_map specialization.
-   informing client about available blocks
-   VFS-940 Add provider id to rt\_block + clang-format.
-   VFS-940 Add rt\_container abstraction.
-   VFS-939 Basic draft of rtransfer worker.
-   add get\_file\_size api
-   VFS-937 Saving provider ID in CCM state.
-   VFS-937 Add Global Registry channel.
-   register for db\_sync changes
-   VFS-919 Module monitoring lifecycle.
-   VFS-889 first working dbsync prototype based on BigCouch long poll
    Rest API
-   remote location module - new data structure and basic api for sync
    purposes
-   VFS-896 Redesign communication layer of the Gateway module.

### v2.1.0

-   conflicts resolved
-   VFS-900 Fix developer mode in gen\_dev.
-   VFS-900 Fix onedata.org domain conversion.
-   VFS-900 Update onepanel ref.
-   VFS-900 Disable developer mode by default.
-   VFS-900 Fix gen\_dev.
-   VFS-900 Add html encoding and fix some minor bugs.
-   VFS-900 Layout change.
-   VFS-900 Fix popup messages.
-   VFS-900 Apply recommended changes.
-   Remove config/sys.config.
-   VFS-900 Fix comments.
-   VFS-900 Update onepanel ref.
-   VFS-900 Add missing quote.
-   VFS-900 Change client download instructions.
-   VFS-900 Fix start of nodes management test.
-   VFS-900 Fix start of high load test.
-   VFS-900 Change format of some configuration variables.
-   VFS-900 Remove yamler.
-   versioning improvement
-   change versioning to fit short version format
-   change versioning not to include commit hash
-   package deb in gzip format (it\'s easier to sign such package with
    dpkg-sig)
-   VFS-923 Remove unnecessary provider hostname variable from start
    oneclient instruction.
-   VFS-923 Change client installation instructions.
-   ca certs loading fix
-   VFS-923 Change client package name.
-   VFS-923 Update client installation instructions.
-   release notes update
-   VFS-613, fix debounce fun
-   VFS-613, fix debounce function not being called prooperly
-   remove unused definitions
-   test adjustment
-   group hash improvement
-   client ACL fix
-   VFS-613, add debounce fun
-   VFS-897 Fix description.
-   VFS-613, move bootbox.js to template
-   VFS-613, merge with develop
-   VFS-613, fix top menu on all pages
-   VFS-613, fix collapsing top menu
-   VFS-613, adjust css

### v2.0.0

-   VFS-897 Use effective user privileges on page\_space.
-   VFS-897 Using effective user privileges.
-   VFS-899 Add breadcrumbs.
-   VFS-894, support for groups in acls
-   disable directory read permission checking
-   handling acl errors + some bugfixes
-   additional group synchronization
-   group permission checking
-   VFS-895 Add RPM package install files progress indicator.
-   VFS-888 Map files to blocks.
-   Include krb and ltdl dylibs in release
-   delete write permission check during set\_acl cdmi request
-   delete read permission check during get\_acl request
-   VFS-886, add posix and acl tabs for perms
-   VFS-886, add radio buttons
-   VFS-881 Minor GUI web pages refactoring.
-   VFS-881 Add groups management.
-   VFS-881 Add space privileges management page.
-   VFS-880 special characters in cdmi, + some minor fixes
-   VFS-881 Using privileges to enable/disable user actions.
-   VFS-886, modify chmod panel to include ACLs
-   VFS-888 Add file\_block DAO record and move file\_location into
    separate documents.
-   doc update
-   checking perms in cdmi
-   checking acl perms in storge\_files\_manager
-   VFs-859 Spaces and tokens web pages refactoring.
-   VFS-676 Update GRPCA.
-   VFS-855, change buttons to link to make them work without websocket
-   VFS-855, add download\_oneclient page
-   send access token hash to user
-   VFS-828 Allow user authentication through HTTP headers.
-   Getting and setting user metadata for CDMI.
-   Add user matadata to file attrs
-   VFS-829: improve error recovery while moving files between spaces

### 1.6.0

-   Security mechanism against attack for atoms table added
-   Invalid use of WebGUI cache fixed

### 1.5.0

-   WebGUI and FUSE client handler can use different certificates.
-   Xss and csrf protection mechanisms added.
-   Attack with symbolic links is not possible due to security mechanism
    update.

### 1.0.0

-   support multiple nodes deployment, automatically discover cluster
    structure and reconfigure it if needed.
-   handle requests from FUSE clients to show location of needed data.
-   provide needed data if storage system where data is located is not
    connected to client.
-   provide Web GUI for users which offers data and account management
    functions. Management functions include certificates management.
-   provide Web GUI for administrators which offers monitoring and logs
    preview (also Fuse clients logs).
-   provide users\' authentication via OpenID and certificates.
-   provide rule management subsystem (version 1.0).
-   reconfigure *oneclient* using callbacks.

------------------------------------------------------------------------

Generated by sr-release.
