# Release notes for project op-worker


CHANGELOG
---------

### 18.02.0-beta5

* fix user_logic:exists
* improve error handling in fetch_lock_fetch_helper
* fix displaying transfer status in GUI
* VFS-4285 Fix restart of deletion_worker
* VFS-4285 Add create/delete test with sync
* VFS-4285 Update getting deleted files
* VFS-4272 Check forward compatiblity during OP connections to OZ
* VFS-4285 Update files deletion
* bugfix in create_parent_dirs
* VFS-4296 Fixed meck entry
* VFS-4267 Adjust code to erl 20, update deps
* Enable disabling rtransfer ssl in app.config
* VFS-4281 pass UserCtx to create_delayed_storage_file function
* VFS-4273 - reverse_luma cache refactored, tests fixed, added specs
* VFS-4273 - handled direct luma cache
* VFS-4273 - refactor of luma_cache, cache is now implemented using links
* VFS-4274 Fixed helpers ref in rebar.config
* VFS-4262 Updated rtransfer link
* VFS-4262 Updated helpers with new asio version
* refactor sfm_utils:create_parent_dirs function
* VFS-4244 Scale helpers tests
* VFS-4152 Fix missing event messages and performance test
* VFS-4152 Extend rtransfer stress tests
* Add rtransfer tests


### 18.02.0-beta4

* VFS-4274 Fixed helpers ref in rebar.config
* refactor sfm_utils:create_parent_dirs function
* VFS-4262 Updated rtransfer link
* VFS-4262 Updated helpers with new asio version


### 18.02.0-beta3

* VFS-4249 storage_sync counters bugfixes and improvements
* Enable graphite support for rtransfer_link.
* Enable SSL for rtransfer_link.
* Update storage select
* Improve rtransfer prefetching counting.
* Integrate rtransfer_link.
* Switch to rebar-dependency helpers.
* VFS-4114 - bugfix in storage_sync histogram
* VFS-4155 add client keepalive msg handling
* VFS-4155 increase cowboy and clproto timeouts.
* VFS-4171 Added folly dependency
* VFS-4171 Updated cberl ref to LibEvent based version
* Updating GUI, including: VFS-4157 * VFS-4157 Requesting completed transfers list with delay to be compatible with backend fixes
* VFS-4222 Force gs connection start only on the dedicated node
* VFS-4217 - fix race on increasing deleted_files_counter and checking whether sync has been finished
* Updating GUI, including: VFS-4223 * VFS-4223 Fixed long time of loading data distribution modal
* VFS-4222 Separate graph sync status check from triggering reconnect
* Updating GUI, including: VFS-4027 * VFS-4027 Added support for peta-, exa-, zetta- and yottabytes
* VFS-4158 Add socket timeout test
* VFS-4114 - transfer management fixes and refactor: this commit includes: * fix for race condition on adding transfer to active transfers link * major refactor of transfer module and model * fix for handling error when replication of not yet synced file has been scheduled * added backoff algorithm for retrying transfers * added files_to_process and files_processed counters in transfer model * refactor of multi_provider_rest_test_SUITE
* VFS-4211 Restarting listeners is now done by node_manager because some ets tables need to be created in the process
* Updating GUI, including: VFS-4206 * VFS-4206 Changed speed units on transfers view to bps
* VFS-4209 Fix synchronization blocking with many spaces
* VFS-4207 Do not match listener's stopping to ok to avoid crashes when it is not running
* VFS-4207 Move listener restarting logic from onepanel to oneprovider, restart GS connection after ssl restart
* VFS-4158 Fix root links scopes
* VFS-4163 - use transfer timestamps as link keys
* VFS-4158 Fix race in ensure connected
* VFS-4158 Improve provider connection management
* Updating GUI, including: VFS-4012, VFS-4154 * VFS-4012 Info about remote statistics on transfers view; fixed transfer row sort issues * VFS-4154 Dynamically adjust polling interval of transfers data; fixed transfer chart loading
* VFS-4158 Fix minor bug in router
* VFS-4158 Make connections asynchronous
* VFS-4054 Make default IP undefined
* VFS-4054 Delete check_node_ip_address from node_manager.
* VFs-4054 Remove getting cluster ips from node_manager_plugin
* VFS-4158 Fix batmatch in sequencer


### 18.02.0-beta2

* Update app.config
* fallback to admin_ctx when luma is disabled
* handle luma returning integer values
* fix handling uid and gid from luma as integers
* VFS-4036 Added flat storage path support
* VFS-4040 Improve speed and reliability of datastore, improve permission cache and times update
* disable http2
* VFS-4128 Fix tp internal call on space_storage
* VFS-4035 Allow non-blocking provider messages handling
* VFS-4130 Update ctool, adjust to new time_utils API, fallback to REST when get_zone_time via GS fails
* VFS-4035 Prevent blocking event stream by connection
* VFS-4035 Handle processing status during provider communication
* VFS-4035 Fix deadlock in storage file creation function
* VFS-4124 Added posix rename handling for nulldevice and glusterfs
* VFS-4117 Implement new read_dir_plus protocol
* VFS-3704 update cowboy to version 2.2.2
* VFS-4117 Control read_dir_plus threads number


### 18.02.0-beta1

* VFS-4080 Verify other providers' domains while connecting via IP addresses
* VFS-3927 Remove support for IdP access token authorization and basic auth, only macaroon auth is now supported
* VFS-3978 Do not distribute test CA with op-worker
* VFS-3965 Provider now uses port 443 for protocol server
* VFS-3965 Added an HTTP Upgrade Protocol procedure before handshake
* VFS-3947 Use random nonce during every inter-provider connection to verify provider identity
* VFS-3751 Ensure users can view basic info about their spaces and groups if they do not have view privileges
* VFS-3751 Authorize providers using macaroons rather than certificates
* VFS-3751 Rework provider communicator to work with macaroons and support verification 
* VFS-3751 Merge provider_listener and protocol_listener into one
* VFS-3751 Add handshake message for providers
* VFS-3635 Remove default OZ CA cert, download it every time before registration
* VFS-3279 Implement new synchronization channel between OP and OZ (Graph Sync)
* VFS-3730 Separate trusted CAs from certificate chain
* VFS-3526 Implement subdomain delegation; Combine provider record fields "redirection_point" and "urls"  into "domain"
* VFS-3526 Remove old dns plugin module
* Refactor datastore models to integrate them with new datastore
* Change links storing model to use dedicated links tree for each provider
* VFS-4088 GUI: Fixed incorrect ordering and stacking of transfer chart series
* VFS-4068 GUI: Fixed incorrect icons positioning in transfers table
* VFS-4062 GUI: Remember opened space when switching between data-spaces-transfers views; fixes in data-space sidebar
* VFS-4059 GUI: Fixed provider icon scaling in transfers view


### 17.06.2

* Updating GUI, including: VFS-4088 * VFS-4088 Fixed incorrect ordering and stacking of transfer chart series
* VFS-4074 removes option start_rtransfer_on_init, added gateway_supervisor
* VFS-4074 add rtransfer to supervision tree
* Updating GUI, including: VFS-4068, VFS-4062, VFS-4059 * VFS-4068 Fixed incorrect icons positioning in transfers table * VFS-4062 Remember opened space when switching between data-spaces-transfers views; fixes in data-space sidebar * VFS-4059 Fixed provider icon scaling in transfers view
* VFS-3889 increase transfer_workers_num to 50, remove commented out code and todo
* VFS-3889 restart transfers via rest, add test for many simultaneous transfers
* Bump compatible versions to 17.06.0-rc9 and 17.06.0
* VFS-3906 Transfer GUI displays charts from before several seconds rather than approximate them to present
* VFS-3889 add cancellation of invalidation transfers


### 17.06.1

* Releasing new version 17.06.1


### 17.06.0-rc9

* VFS-3951 add zone connection test suite
* VFS-4004 Update ctool to include safe ciphers in TLS
* fix storage_update not restarting after provider restart
* move setting of rtransfer port to app.config, fix transfer destination being send as string "undefined" instead of null
* lower rtransfer_block_size, increase number of transfer_workers
* do not allow provider to restart all transfers in supported space
* Updating GUI, including: VFS-4002 * VFS-4002 Showing transfer type (replication/migration/invalidation) in transfers table
* change finalizing state of transfer in gui to invalidating, update finish time when invalidation is finished
* Updating GUI, including: VFS-4000, VFS-3956, VFS-3595, VFS-3591, VFS-3210, VFS-3710 
* VFS-4000 Fixed fetching wrong transfer statistics for chosen timespan 
* VFS-3956 Fixed provider name tooltip rendering in migrate menu of data distribution modal 
* VFS-3595 Fixed locking ACL edit when switching between ACL and POSIX in permissions modal
* VFS-3591 Fixed infinite loading of metadata panel when failed to fetch metadata for file 
* VFS-3210 Fixed displaying long text in basic file metadata keys and values 
* VFS-3710 Using binary prefix units for displaying sizes (MiB, GiB, etc.)
* VFS-3951 add op ver compatibility check
* VFS-3911 - mechanism for turning node_manager plugins on/off in app.config, turn monitoring_worker off by default
* changes in throttling_config
* VFS-3951 add build_version env var for op
* VFS-3972 Fix attach-direct consoles in releases not being run with xterm terminal
* VFS-3951 add rest endpoint for checking op version
* add try-catch around rtransfer write, remove debug logs
* fix bug in gateway_connection:garbage_collect function add retrying of fetching file chunk
* VFS-3932 Reuse cluster worker graphite args
* VFS-3911 - use exometer counter to control execution of storage_import and storage_update
* VFS-3932 Added helper performance metrics
* VFS-3892 Use weighted average (rather than arithmetic) to calculate transfer speeds between time windows, improve calculations on the edges of speedchart
* VFS-3892 Move status field from transfer record to transfer-current-stat record
* VFS-3857 make request_chunk_size and check_status_interval constants in replica_synchronizer configurable in app.config
* VFS-3857 canceling and automatic retries of transfers
* fallback to admin_ctx when luma is disabled
* VFS-3811 Add exometer counters
* handle luma returning integer values
* Hotfix - Improve environment variables names
* Hotfix - prevent rtransfer from crush
* VFS-3864 Lower default request timeout to 30 seconds
* do not restart transfers which has been deleted, create separate links trees for different spaces
* add storageId and storageName to map_group luma request
* create files on storage with appropriate uids and gids


### 17.06.0-rc8

* fallback to admin_ctx when luma is disabled
* handle luma returning integer values
* Hotfix - Improve environment variables names
* Hotfix - update deps
* Hotfix - prevent rtransfer from crush
* improvements according to PR
* improve docs according to PR, please dialyzer
* VFS-3864 Lower default request timeout to 30 seconds
* do not restart transfers which has been deleted, create separate links trees for different spaces
* add storageId and storageName to map_group luma request
* please dialyzer
* create files on storage with appropriate uids and gids
* VFS-3846 Do not mock oneprovider in initializer
* Releasing new version 17.06.0-rc8
* Update vsn in app.src file
* do not restart failed transfers
* Hotfix - update cluster_worker
* VFS-3851 - fix dialyzer
* VFS-3851 - fix rtransfer not binding to many interfaces
* VFS-3851 fix not casting replication of first file in the tree (replication was handled by transfer_controller itself)
* VFS-3851 - remove timeout from function awaiting rtransfer completion, delete old TODO
* handling onedata groups in luma, added tests for luma improvements, cleaning docs after make_file or create_file failed
* VFS-3813 Update tests
* VFS-3813 Improve files creation performance
* VFS-3813 Add comment to vm.args
* refactor luma map_group request
* fix handling uid and gid from luma as integers
* fix storage_sync tests that use luma
* handling onedata groups in luma, added tests for luma improvements
* VFS-3813 Update get_provider_id
* add spaceId to resolve_group request, split resolving acl user id and group id to different functions
* VFS-3808 Update deps
* add case for onedata idp in reverse_luma_proxy:get_user_id
* VFS-3808 Update deps
* VFS-3808 Update deps
* VFS-3808 Update deps
* VFS-3808 Update deps
* Update deps
* Update deps


### 17.06.0-rc8

* do not restart failed transfers
* VFS-3851 - fix rtransfer not binding to many interfaces
* VFS-3851 fix not casting replication of first file in the tree (replication was handled by transfer_controller itself)
* VFS-3851 remove timeout from function awaiting rtransfer completion
* VFS-3813 Improve files creation performance
* fix reverse luma not resolving onedata groups, add storage name to reverse luma request parameters
* VFS-3686 create autocleaning links tree for each space


### 17.06.0-rc7

* Fix failures connected with exometer timeouts
* VFS-3815 Added erlang-observer as RPM build dependency
* VFS-3686 allow to start space cleaning manually
* VFS-3781 Added radosstriper library
* VFS-3686 autocleaning API and model
* Updating GUI, including: VFS-3710 - VFS-3710 Using binary prefixes for size units (IEC format: MiB, GiB, TiB, etc.)
* Updating GUI, including: VFS-3668 - VFS-3668 Show file conflict names in files tree and change conflict name format to same as in Oneclient
* VFS-3756 Repair session (prevent hang up)
* VFS-3756 Update cluster_worker to prevent provider from crush when database is down
* VFS-3763 Fixed helpers namespace in NIF
* VFS-3763 Updated to folly 2017.10.02
* VFS-3753 - fix storage sync failing when luma is enabled


### 17.06.0-rc6

* VFS-3693 Update exometer reporters management
* VFS-3693 Reconfigure throttling


### 17.06.0-rc5

* fix error that occurs when we try to count attrs hash of deleted file
* fix fetching luma_config


### 17.06.0-rc4

* VFS-3682 Upgraded GlusterFS libraries
* VFS-3663 Fix delete events and improve changes broadcasting
* VFS-3616 parallelize replication of file
* VFS-3705 recount current file size on storage when saving sequence of blocks
* VFS-3615 resuming transfer after restart, fix of synchronization of links in transfer model
* VFS-3705 fix quota leak
* VFS-3701 Update logging and cluster start procedure
* VFS-3709 add mechanism to ensure that exometer_reporter is alive
* VFS-3701 Better provider listener healthcheck
* VFS-3666 Event emiter does not crush when file_meta is not synchronized


### 17.06.0-rc3

* VFS-3649 Emit attrs remote attrs change even if location does not exist
* VFS-3500 Extend logging for wrong provider ids in tree_broadcast messages
* VFS-3449 set sync_acl flag default to false
* VFS-3549 Add endpoint for enabling space cleanup.
* VFS-3500 Limit calls to storage when new file is created. Limit calls to storage_strategies.
* VFS-3549 Add list operation and histograms to transfers.
* VFS-3549 Add transfer model.
* VFS-3500 Do not create locations during get_attrs
* VFS-3567 Store missing documents in datastore cache
* VFS-3449 adapting luma to new protocol, refactor of luma_cache module, added tests of reverse_luma and importing acls associated with groups, WIP
* VFS-3449 adapting reverse luma for querying by acl username, groups handling, WIP
* VFS-3541 Move file_popularity increment from open to release.
* VFS-3541 Do not migrate data during replica invalidation when migration_provider_id is set to undefined.
* VFS-3449 storage_sync supports NFS4 ACL, preparation of luma modules to support requests considering groups mapping, extended handling of acl principals in acl_logic
* VFS-3560 Updating GUI ref
* VFS-3495 Improve rest error handling.
* VFS-3444 Adjuster default helper buffer values in app.conf
* VFS-3495 Update ctool and use its new util function for getting system time. Introduce hard open time limit to space cleanup.
* VFS-3500 Configure throttling
* VFS-3500 Use cache of parent during permissions checking
* VFS-3495 Add parameters to file_popularity_view.
* VFS-3495 Add histograms to file_popularity model.
* VFS-3498 Read_dir+
* VFS-3500 Reconfigure throttling
* VFS-3495 Do not ivalidate partially unique file as root (we cannot guarantee its synchronization), add space cleanup test.
* VFS-3500 Update couchbase pool size control
* VFS-3500 Reconfigure cluster for better performance
* VFS-3494 Add popularity views and use them in space_cleanup.
* VFS-3494 Move cleanup_enabled flag to space_storage doc.
* VFS-3494 Add cleanup_enabled flag to storage doc
* VFS-3494 Add file_popularity model tracking file open.
* VFS-3494 Add invalidate_file_replica function to logical_file_manager and rest api.
* VFS-3464 Added extended attributes support to storage helpers


### 17.06.0-rc2

* fix overlapping imports
* VFS-3470 Improve dbsync changes filtering and queue size control
* VFS-3454 Use silent_read in rrd_utils.
* Generate file_meta uuid using default method.
* VFS-3480 Remove file_location links.
* storage_sync improvements:  * use storage_import_start_time  * set queue_type lifo in worker_poll  * reset storage_file_ctx before adding job to pool
* VFS-3430 Adjust stress tests to refactored file_meta.
* VFS-3430 Move periodic cleanup of permission cache to fslogic_worker, refactor file_meta.
* VFS-3430 Adjust changes stream test to delayed creation of file_location.
* VFS-3430 remove file_consistency.


### 17.06.0-rc1

* VFS-3384 save last_update_start_time and last_update_finish_time in storage_strategies
* VFS-3384 save luma_api_key in luma_config, fix storage_sync chmod_file_update2 test
* VFS-3448 Use single 'onedata' bucket
* VFS-3384 implementation of reverse_luma and luma_cache_behaviour, update of luma tests
* VFS-3378 Enabled native GlusterFS support on OSX
* VFS-3363 Use no_seq for saves to default bucket
* Reconfigure couchbase pools


### 17.06.0-beta6

* VFS-3366 Repair lost changes scheduling
* VFS-3376 Use pipe character instead of dot to join and split associative ids in gui ids
* VFS-3416 Change subscriptions updates
* VFS-3415 Make shares in public mode be fetched using provider authorization
* fix errors in space_sync_worker check_strategies
* VFS-3415 Fix a routing bug causing public share links malfuntion
* VFS-3363 Use in-memory changes counter in streams
* Add mising proxy_via field in recursive invocation.
* Fix provider_communicator:send_async/2
* VFS-3361 Emit event on times update.
* VFS-3409 Handle share requests in user context rather that provider context so all operation can be performed despite lack of support
* VFS-3356 Add space_storage/storage accessors
* VFS-3361 Do not create empty replicated files.
* VFS-3363 Improve dbsync performance
* VFS-3289 backend for metrics of storage_sync
* VFS-3363 Fix concurent delete
* VFS-3361 Return updated file_ctx from storage_file_manager:new_handle.
* VFS-3361 Add 'storage_file_created' field to file_location. Split sfm_utils_create_storage file into two functions creating file and location.
* VFS-3361 Remove empty block from file_location response.


### 17.06.0-beta4

* VFS-3362 Update web-client
* Enable storage helper buffering


### 17.06.0-beta3

* Releasing new version 17.06.0-beta3


### 17.06.0-beta2

* Added GlusterFS support
* VFS-3344 Improve dbsync changes aggregation
* VFS-3309 Remove message_id model.
* VFS-3350 Make sure that new permissions can be safely added to the system without breaking gui compliance
* VFS-3350 Remove deprecated privilege names
* VFS-3326 Fix dbsync recovery stream
* VFS-3183 - refactor of storage_sync
* Decode cacert from pem into der format, when opening websocket connection.


### 3.0.0-rc16

* Generate empty monitoring events in order to fill null data.
* Send size of event in read/write events.
* VFS-3183 Add fsync operation to fslogic
* VFS-3233 Add support for sig v2 to AWS S3 helper
* VFS-3248 Move xattrs from provider to fuse messages. Add create and replace flags to setxattr.
* VFS-3017 Fix wrong index encoding.
* VFS-3017 Emit file_removed event when file removal is requested.
* VFS-3187 Execute requests synchronously in connection process.
* VFS-3187 Add trap_exit flag to connection.
* VFS-3017 Copy/remove files during move when non posix storage is used
* VFS-3017 Enable file garbage collection, adjust tests to the new rename implemenetation
* VFS-3025 Implement rename operation.
* VFS-3025 Rewrite current remove implementation and delete rename operation.


### 3.0.0-rc15

* Add token_auth translator.
* Disable storage helpers buffering
* VFS-3233 Add support for sig v2 to AWS S3 helper
* VFS-3244 Switch level of dbsync periodic status logs to debug
* VFS-3244 Do not fail on deletion_worker's init when we cannot list file handles for cleanup.
* VFS-3244 Add file_objectid to custom_metadata document.
* VFS-3251 Updating GUI to 3.0.0-rc15
* VFS-3181 Add onezone URL to sessionDetails
* Add service version info to sessionDetails in GUI
* VFS-3213 Update cberl reference
* VFS-3213 Add libcouchbase package dependency
* VFS-3146 Update models specyfications
* VFS-3146 Update hooks after datastore update
* VFS-3146 Update datastore models to use new datastore API
* VFS-3116 Handle chmod, truncate and updating timestamps in storage_sync
* VFS-3088 Update dbsync state and events
* VFS-3116 Refactor storage_import and space_sync_worker
* VFS-3088 Integrate with refactored datastore


### 3.0.0-rc14

* Dbsync uses datastore_pool to dump documents to db
* Update cluster_worker reference
* Do not fail dbsync posthook when we cannot chown file on storage.
* Refactor event_manager:get_provider function.
* Fix event proxying.


### 3.0.0-rc13

* VFS-3118 Change default env value for custom gui root
* VFS-3025 Add create_and_open operation to sfm and use it during file copying.
* VFS-3097 Do not deserialize macaroons when it is not necessary
* VFS-3025 Do not open file in logical_file_manager, use provided handle.
* VFS-2961 Refactor functions duplicating code in od_user module.


### 3.0.0-rc12

* Update datastore caching mechanism - use dedicated processed instead of transactions
* VFS-2991 Add consistent_hashing library.
* VFS-2719 Introduce limits to the frequency of reconnect attempts in subscriptions websocket client
* VFS-2496 Fix a bug causing user updates not to include new spaces / groups
* VFS-2496 Make sure new spaces and groups appear after creation despite not being yet synchronized from onezone
* VFS-2496 Change relations in space-user|group-permissions models
* VFS-2910 Reduce number of helper system threads
* VFS-2910 Update storage detection logic
* VFS-2496 Allow updating default space in user data backend
* VFS-2496 Migrate to fully relational model in gui backend
* VFS-2909 Adjust code to updated ceph lib
* VFS-2871 Update file_consistency and dbsync
* VFS-2835 Update change propagation controller
* VFS-2793 Implement several simple space strategies
* VFS-2808 Integrate new helpers.
* VFS-2522 Do not fail when trash file index is found.
* VFS-2829 Exclude root and guest sessions from file handles
* VFS-2829 Use hidden file prefix for rename
* VFS-2696 Add better error logging to backend for file acl update
* VFS-2696 Rework file ACL model in GUI backend
* VFS-2696 Fix a bug in GUI file rename that was breaking file paths
* VFS-2723 Fix events routing for file subscriptions
* VFS-2755 Send SyncResponse message with checksum and file_location instead of sending solely checksum.
* VFS-2860 Updating frontend to 3.0.0-rc12
* VFS-2934 Enable storage helper buffering configuration
* VFS-2856 Improve caching of rules result, inject modified file context into function arguments.
* VFS-2856 Permission refactoring.
* VFS-2856 Configure new log layout in lager.
* VFS-2496 Push update of user record on every relation update
* VFS-2496 Change relations in group-user|group-permissions models
* VFS-2496 Return unauthorized when trying to update a user other than the one with current session
* VFS-2931 Reduce number of kept rotated log files
* VFS-2910 Refactor LUMA modules
* VFS-2856 Synchronize file before moving it between spaces.
* VFS-2696 Refactor fslogic
* VFS-2808 Integrate new helpers.
* VFS-2696 Rework file permissions in GUI into one record containing POSIX and ACL perms
* VFS-2696 Rework file ACL model in GUI backend
* VFS-1959 Add and handle OpenFile, CreateFile and MakeFile msgs
* VFS-2696 Implement file rename in GUI backend
* VFS-2807 Repair mnesia overload by session_watcher
* VFS-2522 Add support for spatial queries.
* VFS-2773 Subscribe for monitoring events on root session only.
* VFS-2755 Do not send location update to the client who provoked the sync.
* VFS-2742 Change API to work with GUID-based protocol.
* VFS-2755 Send SyncResponse message with checksum and file_location instead of sending solely checksum.


### 3.0.0-rc11

* VFS-2773 Listen to more changes in /changes api and add a few new tests.
* VFS-2764 Fix directories having 0B size in GUI
* VFS-2764 Fix size of files being zero right after upload
* VFS-2696 Change text/javascript to applicaiton/javascript
* VFS-2696 Reroute events through proxy for open files.
* VFS-2696 Fix wrong aggregarion of file_attr event.
* VFS-2733 Add REST routes to GUI listener
* VFS-2733 Standarize app listeners


### 3.0.0-rc10

* VFS-2742 Fix aggregation for update_attr events.
* VFS-2494 Updating GUI frontend reference
* VFS-2703 Update mocking
* VFS-2662 Account uploaded files in LS cache
* VFS-2662 Fix a badly stacktrace in fslogic worker
* VFS-2662 Append new files to the beginning of the files list
* VFS-2662 Implement file creation compatible with pagination model
* VFS-2662 Add ETS for LS results caching
* VFS-2665 Add proper deserialization of handle timestamp in subscriptions.
* VFS-2665 Update ctool and change handle timestamp type definition.
* VFS-2524 Fix problems with acl protocol encoding, add tests for acl conversion.
* VFS-2524 Add old acl conversion functions.
* VFS-2400 Update to new ceph and aws libraries
* VFS-2524 Improve translation of acl and xattr records.
* VFS-2524 Add basic attributes to /attributes endpoint.
* VFS-2667 Improve json encoder for DB operations
* VFS-2524 Change format of attributes in rest.
* VFS-2524 Fix wrong file owner in cdmi.
* VFS-2524 Add copy operation to cdmi interface.
* VFS-2665 Improve consistency checking in dbsync_events module
* VFS-2659 Add some new fields to subscriptions
* VFS-2665 Add times as component of file_consistency.
* VFS-2665 Move times from file_meta to separate model. Fix dbsync problems.
* VFS-2573 Repair custom metadata propagation
* VFS-2663 Update deps, update critical section and transaction usage
* VFS-2659 Add some fields to records synchronized from OZ
* VFS-2659 Refactor some filed names in records
* VFS-2659 Rework user and group models
* VFS-2659 OP no longer differentiates between groups and effective groups
* VFS-2659 Rename spaces field in od_user to space_aliases
* VFS-2659 Rename some of the key records in db
* VFS-2593 Adapt stress tests to new mechanism allowing for running many test suites
* VFS-2573 Invalidate permission cache propagation


### 3.0.0-rc9

* VFS-2609 Fix error 500 when specifying wrong url for transfer
* VFS-2609 Fix query-index invalid parameters
* VFS-2609 Fix error 500 when requesting nonexistent transfer
* VFS-2609 Handle metadata filter errors
* VFS-2609 Handle invalid json as error 400


### 3.0.0-rc8

* VFS-2625 Add tests for deletion and conflit resolving for handles and handle services
* VFS-2625 Add support for public handles
* VFS-2625 Do not use handle get or fetch
* VFS-2625 Fix public share view not retrieving fiels correctly
* VFS-2625 Fix handles not being properly retrieved via REST
* VFS-2609 Add test of setting json primitives as metadata.
* VFS-2524 Apply recommended changes.
* VFS-2625 Add backends for handles and handle services
* VFS-2524 Add move operation to cdmi, split move and copy tests.
* VFS-2625 Add handle field to share record in data backend
* VFS-2594 Make filters work with json metadata in arrays.
* VFS-2625 Accound handles and handle_serives in subscriptions tests
* VFS-2625 Set default value of service properties in handle services to empty list
* VFS-2625 Add handles and handle_services to subscriptions
* VFS-2626 Add handle field to share_info


### 3.0.0-rc7

* VFS-2567 Use ShareId and FileId in getPublicFileDownloadUrl public rpc call
* VFS-2567 Use new approach to shared files displaying in public view
* VFS-2567 Push container dir change upon share rename
* VFS-2567 Share.file is now file rather than file-shared record
* VFS-2567 Add file-property-shared record dedicated for shares view
* VFS-2567 Add file-shared record dedicated for shares view
* VFS-2567 Add container dir to share record
* VFS-2567 Change name of fileProperty field in file public record
* VFS-2567 Add reference to publi file from public metadata record
* VFS-2567 Add public metadata record in file public record
* VFS-2567 Make sure group type is an atom in onedata_group fetch
* VFS-2567 Allow getting only public data about a group
* VFS-2567 Show shares only to users with space_view_data
* VFS-2567 Fix json and rdf metadata not being properly deleted in update callback in data backend
* VFS-2594 Add read and execute permission for others on space dir. Block guest users from reading non shared files
* VFS-2567 Fix some bugs in code responsible for checking view privileges
* VFS-2594 Add check of 'other' perms for share files.
* VFS-2567 Check view permissions in groups and shares gui backend
* VFS-2594 Refactor lfm_proxy module.
* VFS-2594 Move xattr name definitions to header, do not alow direct modification of xattrs with 'onedata_' prefix.
* VFS-2594 Add remove_metadata operation.
* VFS-2567 Check view permissions in space gui backend
* VFS-2594 Add has_custom_metadata method to logical_file_manager.
* VFS-2180 Implement support for read only spaces
* VFS-2180 Add provider's ID to file_attr message
* VFS 2557 Update tests init/teardown
* VFS-2456 Add metadata to public view
* VFS-2456 Implement first version of metadata backend
* VFS-2405 Add some error handling to group privileges
* VFS-2405 Add some error handling to space privileges
* VFS-2405 Add error handling when user is not authorized to manage shares
* VFS-2555 Remove shares on file removal, add doc for share_guid, decode oz 403 error as eacces.
* VFS-2555 Add shares field to file attr.
* VFS-2405 Adjust to new shares API in OP, fix a badmatch
* VFS-2405 Use lfm API to create and delete share
* VFS-2555 Implement remove_share operation and move some logic out of share_logic.
* VFS-2555 Add Name parameter to create_share operation.
* VFS-2405 Implement share_logic:delete
* VFS-2555 Adjust fslogic_proxyio_test to shares.
* VFS-2555 Improve share permissions and guest user management.
* VFS-2555 Add protocol for operations on shares
* VFS-2555 Add guest session, prepare api and tests for shares.
* VFS-2405 do not use root session id in shares view
* VFS-2405 Add mockup of public share data backend
* VFS-2405 Add mapping in gui backend for the new space permission (manage shares)
* VFS-2405 Further code refactor
* VFS-2405 Adjust to new OZ model where shares are no longer spaces
* VFS-2405 Add share specific parameters to space record


### 3.0.0-rc6

* VFS-2180 Improve links conflict resolution
* VFS-2582 Using GUI fix for blank notifications
* VFS-2180 Adapt code to cluster_worker's API change
* VFS-2180 Improve dbsync implementation
* VFS-2180 Use gen_server2 instead of erlang's gen_server module
* VFS-2390 Fix handlers specification in REST API
* VFS-2390 Update rebar to version 3
* Update memory management
* VFS-2180 Allow for concurrent file creation


### 3.0.0-rc5

* VFS-2534 Use erlang:system_time/1 instead of os:timestamp/0
* VFS-2534 Skip dbsync state update if not changed
* VFS-2543 Integrate gen_server2
* VFS-2446 Use default group type rather than undefined in group logic
* VFS-2472 Convert metadata to from proplists to maps.
* VFS-2472 Do not fail when user provides empty callback for replicate operation.
* VFS-2540 add on_bamboo variable to coverage target
* VFS-2540 implement collecting .coverdata files in coverage.escript from many ct directories
* VFS-2534 Improve events processing
* VFS-2426 Add check_perms operation to logical_file_manager.
* VFS-2472 Add 1.1 as possible cdmi version, improve documentation.
* VFS-2472 Handle acl identifier without '&#35;' separator.
* VFS-2472 Add correct handling of key and keys parameters to query_index handler.
* VFS-2490 Update op-gui-default ref
* VFS-2472 Add filter option to metadata PUT.
* VFS-2472 Unify file identifiers in REST interface.
* VFS-2472 Add checking permissions to REST API operations.
* VFS-2472 Add listing and getting inherited xattrs to REST API.
* VFS-2472 Add inherited option to listing and getting xattrs internals.
* VFS-2472 Add inherited option to getting json metadata.
* VFS-2472 Add json merging function.
* VFS-2472 Add 'inherited' option to list_xattr and get_metadata interface.
* VFS-2472 Add escaping of user defined js function.
* VFS-2309 oz test mock updated to match actual implementation
* VFS-2309 implemented provider registration besed on public keys & updated tests
* VFS-2309 listener starting fixes
* VFS-2309 fixed public key encoding
* VFS-2309 public key based identity endpoind


### 3.0.0-rc4

* VFS-2384 Prevent unrelated events from being lost on crash.
* VFS-2320 Move RRD databases to file system


### 3.0.0-RC3

* VFS-2156 Remove GUI files
* VFS-2311 Add private RPC to retrieve file download URL
* VFS-2389 Change event stream management
* VFS-2263 Do not create handles for created file if not needed
* VFS-2189 Close connection after file upload failure
* VFS-2319 Remove spawns on event emits
* VFS-2402 Update cluster_worker
* Releasing new version 3.0.0-RC2
* VFS-2273 Handle handshake errors
* VFS-2233 Changing separate fuse request types to nested types
* VFS-2336 Update LUMA API to swagger version
* VFS-2303 Fix eunit tests.
* VFS-2303 Add metadata-id endpoint.
* VFS-2303 Add filters for getting metadata.
* VFS-2303 Add query-index rest endpoint.
* VFS-2340 Minor comments update
* VFS-2303 Adjust query_view function to handle any view option.
* VFS-2303 Fix /index/:id PUT rest internal error.
* VFS-2303 Add /index and /index/:id endpoints to rest API.
* VFS-2269 Enable Symmetric Multiprocessing
* VFS-2303 Store all user indexes in one file.
* VFS-2303 Adjust metadata changes stream to the new metadata organization.
* VFS-2303 Add index model.
* VFS-2303 Add validation of metadata type.
* VFS-2303 Add filtering by spaceID to views.
* VFS-2303 Add view tests.
* VFS-2303 Add better error handling for custom metadata.
* VFS-2319 Reimplement monitoring using events
* VFS-2303 Add support for rdf metadata.
* VFS-2303 Move xattrs to custom_metadata document.
* VFS-2303 Add basic metadata operations.
* VFS-2361 Turn off HSTS by default, allow configuration via app.config
* VFS-2340, Update deps
* Releasing new version 3.0.0-RC1
* VFS-2049 Improve file_consistency waiting for parent mechanism.
* VFS-2049 Add waiting for parent_links in dbsync hook.
* VFS-2049 Fix file_consistency wrong list ordering.
* VFS-2303 Add custom_metadata model.
* VFS-2229 Add reaction to rename of external file_location
* VFS-2215 Disable blocks prefetching.
* VFS-2215 Exclude file removal originator from event recipients.
* VFS-2049 Make file_consistency work after system restart.
* VFS-1847 Refactor LUMA and helpers modules
* Squashed 'appmock/' changes from 71733d3..1f49f58
* VFS-2049 Improve file_consistency model.
* VFS-2233 Extract file entry to generic fuse request
* VFS-2049 Basic consistency checking before executing hook.


### 3.0.0-RC2

* VFS-2336 Update LUMA API to swagger version
* VFS-2303 Add metadata-id endpoint.
* VFS-2303 Add filters for getting metadata.
* VFS-2303 Add query-index rest endpoint.
* VFS-2303 Adjust query_view function to handle any view option.
* VFS-2303 Add /index and /index/:id endpoints to rest API.
* Fix reactive file displaying in GUI during file upload
* VFS-2269 Enable Symmetric Multiprocessing
* VFS-2303 Store all user indexes in one file.
* VFS-2303 Adjust metadata changes stream to the new metadata organization.
* VFS-2303 Add custom_metadatada model to sync via dbsync.
* VFS-2303 Add index model.
* VFS-2303 Add validation of metadata type.
* VFS-2303 Add filtering by spaceID to views.
* VFS-2303 Add view tests.
* VFS-2303 Add better error handling for custom metadata.
* VFS-2340 Repair bug in storage file manager
* VFS-2303 Add support for rdf metadata.
* VFS-2303 Move xattrs to custom_metadata document.
* VFS-2340 Update file consistency management
* VFS-2340 Add file consistency test
* VFS-2329 Include data requested for sync in prefetching range.
* VFS-2361 Turn off HSTS by default, allow configuration via app.config


### 3.0.0-RC1

* VFS-2316 Update etls.
* VFS-2292, Update dbsync batches storing
* VFS-2215 Disable blocks prefetching.
* VFS-2215 Exclude file removal originator from event recipients.
* VFS-2215 Wrap event_manager's handle_cast in try/catch.
* VFS-2292 Session managmenet update
* VFS-2292 Minor initializer update
* VFS-2292 Add os-mon
* VFS-2250 Use wrappers for macaroon serialization
* VFS-2214, Release handles for created files
* VFS-2214, Update session management and lfm proxy


### 3.0.0-beta8


* VFS-2254 Additional GUI model relations
* VFS-2254 Always allow to get acl after creation.
* VFS-2254 Return full acl record on create operation in file-acl backend..
* VFS-2254 Change EAGAIN to EIO error on sync fail.
* VFS-2254 Adjust file-acl protocol.
* VFS-2197 Fail sync when rtransfer fails.
* VFS-2254 Add acls to file_data_backend.
* VFS-2115 Fix changing file GUID in request after merge
* VFS-2115 Add file redirection to rename, add phantom files expiration
* VFS-2115 Add file redirection


### 3.0.0-beta7

* VFS-2225 Update GUI docker image
* VFS-1882 Postpone deletion of open files
* VFS-2170 Improve dbsync's protocol reliability
* VFS-2143, Improve dbsync_worker stashed changes management
* VFS-2187 Add automatic file removal when upload fails
* VFS-2187 Adjust rest_test to new OZ client API
* VFS-2187 Use new OZ REST client API from ctool that uses arbitrary Auth term rather than predefined rest client.
* VFS-2039 Extract non-client messages from fuse_messages


### 3.0.0-beta6

* Update erlang tls
* VFS-2112 Integrate monitoring with REST API
* VFS-2109 Adjust cdmi tests to new error messages.
* VFS-2108 Add prefetching for unsynchronized files.
* VFS-2109 Accept Macaroon header with token, as auth method for REST.
* VFS-2031 Improve queue flushing in dbsync
* VFS-2031 Remove default space
* VFS-2109 Add support for dir replication through REST api.
* VFS-2109 Move rest error handling logic from cdmi_exception_handler to more generic request_exception_handler.
* VFS-2019 Add space name to luma proxy call
* VFS-1506 Make security rules more generic.
* VFS-2081 Make dbsync singleton
* VFS-2018 Add response after rename
* VFS-1506 Fix sending file attributes after replica reconciliation.
* VFS-1506 Include file gaps in file_location's blocks.
* VFS-1999 Use message origin instead of message sender as dbsync's provider context
* VFS-1506 Add permission checking to utime operation.
* VFS-2071 Adjust code to the new S3 helper
* VFS-1999 Quota implementation
* VFS-2018 Adding file renamed subscription
* VFS-2018 Adding file_renamed_event
* VFS-1854 Enable inter-provider sequencer


### 3.0.0-beta5

* VFS-2050, Get file size update
* VFS-2050, Repair errors in connections usage and dbsync batch applying
* VFS-1987 group privileges as atoms
* VFS-1772 unify imports in gui backend, add returned value to group join group
* Increase limit for cdmi_id, as guid of default space in production environment has 199 bytes.
* VFS-1772 add relation to child groups in group record
* VFS-2050, Extend multiprovider tests
* Cache provider info pulled from onezone
* Allow for zombie-file delete
* Hotfix: Ignore sequencer messages that are received from provider
* Hotfix: Fix sending changes of unsupported spaces
* Ignore proxied subscription messages in router.
* Ignore dbsync changes from unsupported spaces. Do not catch exceptions inside mnesia transactions (mnesia does not like it).
* VFS-1772 update group logic concerning privileges
* VFS-1772 align group logic with new group API
* VFS-1987 set & get for nested group privileges
* VFS-2059 change default create modes for files and directories
* VFS-2059 use recursive remove in gui backend
* VFS-2003 Add read_event subscription to rest api.
* VFS-1987 nested groups via fetch
* VFS-1987 nested groups in subscriptions
* VFS-2003 Add replicate_file rest handler.
* VFS-2003 Add rtransfer management api to fslogic.
* VFS-1772 add backend for groups
* VFS-2003 Reorganize rest modules.
* VFS-1772 introduce models for system-user system-group system-provider


### 3.0.0-beta4

* VFS-1995 Syncing locations update
* Fixing updating times in rename interprovider
* VFS-1999 Fix Write/read subscription translate
* VFS-1618 Fix old rmdir usage
* VFS-1671 Update cluster_worker ref.
* VFS-1618 Move configurable values to config
* VFS-1618 Sort synchronization keys to avoid potential deadlocks
* VFS-1975 Add uuid to release message, update release routing
* VFS-1618 Add synchronization for file_meta:rename
* VFS-1854 Improve dbsync's temp state clearing
* VFS-1854 Disable rereplication in dbsync
* VFS-1954 Make session:get_connections const.
* VFS-1854 Fix GUI upload
* VFS-1854 Fix uuid_to_path/2
* VFS-1618 Fix storage files mode changing
* VFS-1854 Fix merge
* VFS-1964 Adjust permission tests to changes in required permissions for dir removal.
* VFS-1854 Fix several cdmi tests
* VFS-1964 Remove unnecessary unlink.
* VFS-1964 Adjust existing implementation of recursive remove to behave like linux.
* VFS-1618 Delete target file after checking all permissions, add ls assertions in tests
* VFS-1618 Change tests to check acl on proper provider
* VFS-1618 Change moving into itself detection to interprovider-friendly
* VFS-1854 Fix fslogic's events subscribtion
* VFS-1618 Improve permissions handling
* VFS-1618 Enable grpca in rename tests
* VFS-1887 Add missing implementation of release.
* VFS-1854 Introduce logical_file_manager:release/1
* VFS-1841 Fix target parent path usage
* VFS-1841 Fix target path usage
* VFS-1841 Change usage of fslogic_req modules to logical_files_manager
* VFS-1841 Use get_file_attr to check if target exists
* VFS-1841 Use space_info:get_or_fetch instead of oz_spaces:get_providers
* VFS-1954 Implement Ceph helper tests.
* VFS-1841 Fix timestamps update
* VFS-1841 Fix usage of gen_path after merge
* VFS-1841 Fix chmod usage in rename
* VFS-1841 Fix sfm file copy fallback
* VFS-1781 Fix rename permissions annotations
* VFS-1781 Inter-space and inter-provider rename
* VFS-1618 First sketch of interspace rename


### 3.0.0-beta3

* VFS-1932 Create StorageHelperFactory with unset BufferAgent.
* VFS-1770 dissallow spaces with empty name
* VFS-1953 Extracting times updating to functions, handling root space
* VFS-1770 improve gui injection script
* VFS-1747 Change checksum algorithm to md4.
* VFS-1770 add polling mechainsm before onedata user is synced
* VFS-1747 Add missing status to fuse_response.
* VFS-1747 Add checksum computing during sync.
* VFS-1521 File GUID to UUID translation
* VFS-1862 Integrate move implementation with cdmi. Add copy_move_test to cdmi_test_SUITE.
* VFS-1798, enable cover
* VFS-1521: Get providers for space from cache instead of OZ
* VFS-1521: Fetch all space_info data in space_info:fetch
* Adjust luma for chown operation.
* VFS-1749 Use proper types in LUMA config
* VFS-1751 Allow specifying request method in IAM calls
* VFS-1596 Ceph permissions adjustment
* VFS-1596 Refactor luma nif, use hex_utils
* VFS-1596 More readable LUMA tests
* VFS-1596 Move LUMA internals to module
* VFS-1596 Move app initialization to function
* VFS-1596 Use dedicated credentials caches instead of luma response
* VFS-1747 Fsync files after transfer.
* VFS-1703 Add remove file event
* VFS-1507 Omitting handle saving for root session
* VFS-1596 Multi storage LUMA tests
* VFS-1596 LUMA nif entry in Makefile
* VFS-1507 Sending file handle in get_file_location
* VFS-1596 Accessing Amazon IAM API from provider
* VFS-1596 Python LUMA API description
* VFS-1507 Sending file handle in get_new_file_location, using handles in read and write
* VFS-1596 Python LUMA implementation
* VFS-1596 Update getting user details
* VFS-1596 Ceph credentials mapping in provider
* VFS-1596 Move LUMA logic to separate modules.
* VFS-1596 LUMA and in-provider credentials mapping with switch
* VFS-1596 Getting credentials from LUMA
* Fix GUI download handler.
* VFS-1768: Permissions table sorting
* VFS-1768: Resetting old tokens after token modal close
* VFS-1768: Sorting provider names in blocks table
* VFS-1770 fix wrong size calculation
* VFS-1768: Fixing token copy with selectjs - to not copy newline on start; 


### 3.0.0-beta1

* VFS-1802 Improve proxyio performance.
* VFS-1521: Get providers for space from cache instead of OZ
* VFS-1521: Resolve issues with too long document.key in dbsync's state
* VFS-1768: BS Tooltip component; style improvements in file chunks modal
* VFS-1768: Prevent opening space page when clicking on space settings icon; blocking Groups page with generic info message
* VFS-1553: Improvements in permissions table; add users/groups action stub
* VFS-1770 first reactive GUI for files
* VFS-1553: Create and join space buttons/modals
* VFS-1757 Change application ports availability checking procedure.
* VFS-1549: Uploaded file name in upload widget
* VFS-1549: Modification time display
* VFS-1549: Dragging file on file browser initial support
* VFS-1728-increase timeouts, timeouts definitions in separate file
* VFS-1549: Added ember-notify
* VFS-1745 Improve handling pending files.
* VFS-1549: Permissions modal
* VFS-1745 Use fslogic_storage:new_user_ctx to generate uid and gid in chown function.
* VFS-1746, Adjust db_sync to new cluster_worker
* VFS-1549: Modals for create dir and file
* VFS-1549: First modal for file browser
* VFS-1549: File browser toolbar, with previous functions
* VFS-1734 fix a bug in unique filename resolver
* VFS-1734 server side file upload
* VFS-1521 Enable cross-provider subscriptions
* VFS-1629 added delete messages handling
* VFS-1629 user included in subscription when gets session
* VFS-1629 propagating updates to the datastore
* VFS-1629 connected provider to the OZ (over websocket)
* VFS-1629 registering connection under name
* VFS-1521 Enable file_location update in lfm
* VFS-1629 simple user subscriptions
* VFS-1521 Proxy read and write events
* VFS-1521 Implement remote ProxyIO
* VFS-1521 Improve logging
* VFS-1521 Fixup provider proxy communication


### 3.0.0-alpha3

* VFS-1598 Fix oz_plugin module.
* Add DBSync's stream restarter
* VFS-1558: Changes in Polish i18n
* Include Erlang ERTS include directory when building c_src/ .


### 3.0.0-alpha2

* VFS-1665 Pull in ctool with new Macaroons.
* VFS-1405 Update cluster_worker
* VFS-1522 Find blocks to transfer in all file locations.


### 3.0.0-alpha

* Dependencies management update
* Add map for helpers IO service. Test open and mknod flags.
* VFS-1524 Change space storage name to space ID. Resolve space name clash problem.
* VFS-1504 Checking if directory is not moved into its subdirectory
* VFS-1421 Change fslogic_spaces:get_space to return space when asking as root.
* VFS-1421 Add malformed query string error message.
* VFS-1484 Enable storage lookup by name.
* VFS-1484 Set number of threads for Amazon S3 storage helper IO service.
* VFS-1421 Send PermissionChangedEvent as list of events.
* VFS-1421 Add translations for aggregated acl types.
* VFS-1421 Handle proxyio exceptions, adjust lfm_files_test to new api.
* VFS-1472 Add librados and libs3 package dependencies.
* VFS-1472 Add IO service for Amazon S3 storage helper to factory.
* VFS-1414 Swapping Limit and Offset arguments in lfm_dirs:ls
* VFS-1474 Changing matching to assertions, adding comments
* VFS-1421 Change space_id to file_uuid in proxyio_request.
* VFS-1421 Add proper handling of accept headers in rest requests, fix some minor bugs.
* VFS-1421 Chmod on storage with root privileges during set_acl operation.
* VFS-1421 Enable permission checking on storage_file_manager open operation.
* VFS-1428 Add list of application ports to config file.
* VFS-1426 Add gateways to a process group.
* VFS-1421 Add permission control to storage_file_manager.
* VFS-1421 Do not allow direct modification of cdmi extended attributes.
* VFS-1421 Add mimemetype, completion_status and transfer_encoding management to logical_file_manager api.
* VFS-1421 Add set_acl, get_acl, remove_acl as separate fslogic requests, with proper permission control.
* VFS-1421 Check permissions on rename operation, repair incorrect mock in fslogic_req_test_SUITE.
* VFS-1421 Return 401 in case of unauthorized access to objects by objectid.
* VFS-1421 Perform fsync after creation of file throught REST request.
* VFS-1428 Add user context to fslogic:get_spaces function.
* VFS-1148 adjust listeners to new cluster_worker API
* VFS-1428 Enable multiple ceph user credentials.
* VFS-1148 add sync button in top menu
* VFS-1426 Migrate rtransfer from 2.0
* VFS-1421 Add acl validation, annotate with access checks common fslogic functions.
* VFS-1148 allow choosing where to create new files and dirs
* VFS-1148 add ability to create new files and dirs in gui
* VFS-1421 Integrate acls with cdmi.
* VFS-1148 file browser allows to preview text files
* VFS-1148 working prototype of basic file browser
* VFS-1421 Add acls to logical_file_manager, add acl setting integration test.
* VFS-1421 Add groups to test environment.
* VFS-1421 Add onedata_group model and implement basic operations on acl.
* VFS-1400 Add compilation utility script.
* VFS-1148 first attempts at file manger page
* VFS-1402 CDMI redirections based on trailing slashes.
* VFS-1398 Add xattrs to onedata_file_api and cdmi_metadata implementation.
* VFS-1403 CDMI object PUT operation + tests.
* VFS-1407 Add mechanism that will remove inactive sessions after timeout.
* VFS-1404 Cdmi object get.
* VFS-1397 Replace identity with auth in container_handler.
* VFS-1338 Cdmi container put.
* Use Erlang cookie defined in env.json file while creating provider spaces.
* VFS-1363 Add user context to all storage_file_manager operations
* VFS-1382 fixed task manager test changing wrong env
* VFS-1382 dns listener starts with cluster_worker supervisor
* VFS-1378 adjust to new ctool API
* Create storages on provider.
* VFS-1382 op-worker related work removed from cluster-worker
* VFS-1382 node_manager config extracted
* VFS-1338 Implement mkdir operation, add tests of container creation to cdmi test
* VFS-1382 separated packages meant to form cluster repo
* VFS-1382 node_manager plugin - extracted behaviour & ported implementation
* Storage creation improvement
* VFS-1339 Move cdmi modules to different packages. Implement binary dir put callback.
* VFS-1218 check permissions while opening a file based on "open flags"
* VFS-1289 Add performance tests for events API.
* Fix pattern matching on maps.
* VFS-1289 Extend set of event and sequencer tests.
* VFS-1338 Extract api for protocol_plugins. Implement dir exists callback.
* VFS-1218 add lfm_utils:call_fslogic
* Refactor of malformed_request/2 and get_cdmi_capability/2.
* Map instead of dict.
* Include guard for cdmi_errors.hrl.
* Skeletons of capabilities handlers.
* VFS-1289 Extend event manager with client subscription mechanism.
* VFS-1327 Separate rest and cdmi as abstract protocol plugins.
* VFS-1291 Add routing to cdmi object/container modules and add some tests.
* Done users and groups; done getting token
* VFS-1291 Add rest pre_handler that deals with exceptions. Update ctool.
* VFS-1291 Rearrange http_worker modules hierarchy.
* VFS-1255 Bump Boost to 1.58 for compatibility with client.
* VFS-1218 merge delete_file with unlink
* VFS-1258, transactions skeleton
* VFS-1218 implement attributes and location notification
* VFS-1244 add possibility for client to update auth
* VFS-1218 fix lfm read/write test
* VFS-1242, Cache controller uses tasks
* VFS-1242, Task pool
* VFS-1242, Task manager skeleton
* VFS-1217 Use RoXeon/annotations.
* VFS-1218 add file_watcher model
* VFS-1194 add user context to StorageHelperCTX
* VFS-1193 better connection handling
* VFS-1194 initial helpers support
* VFS-1199, cache dump to disk management update
* VFS-1193 restart mcd_cluster after connection failure
* VFS-1199, forcing cache clearing once a period
* VFS-1199, Saving cache to disk status management
* VFS-1193 add configurable persistence driver
* VFS-1172, use botan on host machine rather than throw in so files
* VFS-1145 Integrate SSL2 into oneprovider.
* implement generic transactions in datastore ensure file_meta name uniqueness witihin its parent scope
* VFS-1178, Cache controller uses non-transactional saves
* move worker_host's state to ETS table
* use couchbase 4.0
* VFS-1147 Integration with new protocol.
* VFS-1147 Implementation of first operations on directories.
* add disable mnesia transactions option
* VFS-1129 Add deb build dependencies
* VFS-1118, local tests controller added
* implement mnesia links
* VFS-1118, global cache controller added
* VFS-1118, cache clearing skeleton
* VFS-1115 Allow building RPM package.
* VFS-1025, merge lb with develop
* VFS-1053 Selecting explicit node for mnesia to join, instead of finding it randomly
* VFS-1049 add check_permissions annotation
* VFS-1049 add initial fslogic file structure
* VFS-1051 change worker startup order
* implement datastore: 'delete with predicates' and list
* VFS-997 Add event stream periodic emission ct test.
* VFS-997 Add event stream crash ct test.
* VFS-997 Event manager ct test.
* VFS-997 Add event utils and unit test.
* VFS-1041, add send data endpoint to remote control
* checking endpoints during healthcheck of http_worker and dns_worker
* VFS-997 Change sequencer manager connection logic.
* move session definitions to separate header
* change location of message_id header
* extract certificate_info to separate header
* client_communicator lib
* VFS-1000, add logical and storage file manager's API design
* oneproxy CertificateInfo message
* new handshake
* VFS-1000, add sequence support for response mocking
* VFS-997 Add sequencer worker.
* translation improvements
* serialization improvements
* VFS-1010 Make test master node discoverable through DNS.
* client_auth + basic integration with protobuf
* VFS-997 Add sequencer dispatcher ct test.
* VFS-997 Sequencer logic.
* VFS-997 Add sequencer.
* move datastore init to node_manager
* change created beam location to target dir
* refactor worker_host header
* add input_dir/target_dir configuration
* enable init_cluster triggering when all nodes have appeared
* rest/ccdmi function headers
* remove request_dispatcher.hrl
* remove node_manager.hrl
* node_manager refactoring
* oneprovider app reformat + doc adjustment
* http_worker reformat + doc adjustment
* redirector reformat + doc adjustment
* session_logic and n2o_handler reformat + doc adjustment
* rest_handler reformat + doc adjustment
* cdmi_handler reformat + doc adjustment
* dns_worker reformat + doc adjustment
* logger_plugin reformat + doc adjustment
* worker_plugin_behavior reformat + doc adjustment
* worker_host reformat + doc adjustment
* client_handler and provider_handler reformat + doc adjustment
* request_dispatcher reformat + doc adjustment
* oneproxy reformat + doc adjustment
* gsi_nif reformat + doc adjustment
* gsi_handler reformat + doc adjustment
* node_manager_listener_starter reformat + doc adjustment
* node_manager reformat + doc adjustment
* cluster manager reformat + doc adjustment



### v2.5.0

* VFS-965, full functionality of spaces page
* Perform operations asynchronously in ws_handler.
* VFS-965, several funcionalities of page spaces
* VFS-965, visial aspects of spaces page
* VFS-965, first code for spaces page
* VFS-959 Not sending notifications for a fuse that modifies a file.
* set fuseID to CLUSTER_FUSE_ID during creation of file_location
* VFS-954, adjust to new file blocks API
* setting fslogic context
* VFS-939 Implement rtransfer.
* VFS-954, implementation of data distribution panel
* VFS-952 support for AttrUnsubscribe message
* VFS-593, GR push channel messages handling
* getting size from available blocks map, instead of storage
* creating file location for remote files
* creating file location for empty remote files moved to get_file_location
* VFS-940 Subscribing for container state events.
* VFS-940 Add rt_map specialization.
* informing client about available blocks
* VFS-940 Add provider id to rt_block + clang-format.
* VFS-940 Add rt_container abstraction.
* VFS-939 Basic draft of rtransfer worker.
* add get_file_size api
* VFS-937 Saving provider ID in CCM state.
* VFS-937 Add Global Registry channel.
* register for db_sync changes
* VFS-919 Module monitoring lifecycle.
* VFS-889 first working dbsync prototype based on BigCouch long poll Rest API
* remote location module - new data structure and basic api for sync purposes
* VFS-896 Redesign communication layer of the Gateway module.



### v2.1.0

* conflicts resolved
* VFS-900 Fix developer mode in gen_dev.
* VFS-900 Fix onedata.org domain conversion.
* VFS-900 Update onepanel ref.
* VFS-900 Disable developer mode by default.
* VFS-900 Fix gen_dev.
* VFS-900 Add html encoding and fix some minor bugs.
* VFS-900 Layout change.
* VFS-900 Fix popup messages.
* VFS-900 Apply recommended changes.
* Remove config/sys.config.
* VFS-900 Fix comments.
* VFS-900 Update onepanel ref.
* VFS-900 Add missing quote.
* VFS-900 Change client download instructions.
* VFS-900 Fix start of nodes management test.
* VFS-900 Fix start of high load test.
* VFS-900 Change format of some configuration variables.
* VFS-900 Remove yamler.
* versioning improvement
* change versioning to fit short version format
* change versioning not to include commit hash
* package deb in gzip format (it's easier to sign such package with dpkg-sig)
* VFS-923 Remove unnecessary provider hostname variable from start oneclient instruction.
* VFS-923 Change client installation instructions.
* ca certs loading fix
* VFS-923 Change client package name.
* VFS-923 Update client installation instructions.
* release notes update
* VFS-613, fix debounce fun
* VFS-613, fix debounce function not being called prooperly
* remove unused definitions
* test adjustment
* group hash improvement
* client ACL fix
* VFS-613, add debounce fun
* VFS-897 Fix description.
* VFS-613, move bootbox.js to template
* VFS-613, merge with develop
* VFS-613, fix top menu on all pages
* VFS-613, fix collapsing top menu
* VFS-613, adjust css


### v2.0.0

* VFS-897 Use effective user privileges on page_space.
* VFS-897 Using effective user privileges.
* VFS-899 Add breadcrumbs.
* VFS-894, support for groups in acls
* disable directory read permission checking
* handling acl errors + some bugfixes
* additional group synchronization
* group permission checking
* VFS-895 Add RPM package install files progress indicator.
* VFS-888 Map files to blocks.
* Include krb and ltdl dylibs in release
* delete write permission check during set_acl cdmi request
* delete read permission check during get_acl request
* VFS-886, add posix and acl tabs for perms
* VFS-886, add radio buttons
* VFS-881 Minor GUI web pages refactoring.
* VFS-881 Add groups management.
* VFS-881 Add space privileges management page.
* VFS-880 special characters in cdmi, + some minor fixes
* VFS-881 Using privileges to enable/disable user actions.
* VFS-886, modify chmod panel to include ACLs
* VFS-888 Add file_block DAO record and move file_location into separate documents.
* doc update
* checking perms in cdmi
* checking acl perms in storge_files_manager
* VFs-859 Spaces and tokens web pages refactoring.
* VFS-676 Update GRPCA.
* VFS-855, change buttons to link to make them work without websocket
* VFS-855, add download_oneclient page
* send access token hash to user
* VFS-828 Allow user authentication through HTTP headers.
* Getting and setting user metadata for CDMI.
* Add user matadata to file attrs
* VFS-829: improve error recovery while moving files between spaces



### 1.6.0



* Security mechanism against attack for atoms table added
* Invalid use of WebGUI cache fixed



### 1.5.0


* WebGUI and FUSE client handler can use different certificates.
* Xss and csrf protection mechanisms added.
* Attack with symbolic links is not possible due to security mechanism update.



### 1.0.0


* support multiple nodes deployment, automatically discover cluster structure and reconfigure it if needed.
* handle requests from FUSE clients to show location of needed data. 
* provide needed data if storage system where data is located is not connected to client.
* provide Web GUI for users which offers data and account management functions. Management functions include certificates management.
* provide Web GUI for administrators which offers monitoring and logs preview (also Fuse clients logs).
* provide users' authentication via OpenID and certificates.
* provide rule management subsystem (version 1.0).
* reconfigure *oneclient* using callbacks.




________

Generated by sr-release. 
