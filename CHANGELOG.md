# Release notes for project op-worker


CHANGELOG
---------

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
