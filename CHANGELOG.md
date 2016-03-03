# Release notes for project op-worker


CHANGELOG
---------

### New Version Tag do not change this line

* VFS-1665 Pull in ctool with new Macaroons.
* Squashed 'appmock/' changes from 835e98c..fab87fc
* VFS-1522 adjust fuse_config_manager_test_SUITE to new storage config format.
* VFS-1405 Fix fuse_config_manager_test_SUITE's config
* VFS-1405 Update cluster_worker
* VFS-1581, Tests update
* VFS-1581, Deps update
* VFS-1522 Refactor rtransfer config.
* VFS-1522 Add rtransfer config test.
* VFS-1522 Fix badmatch during cache cleanup.
* VFS-1522 Merge changes fix.
* VFS-1405 Disable cache clear assert in test's initializer
* VFS-1522 Reconciliation of conflicting changes.
* VFS-1405 Fix DBSync's state save interval
* Update glog version to 0.3.4 in package build requirements.
* Squashed 'helpers/' changes from 1f79408..6280b36
* VFS-1522 Fix dialyzer errors.
* VFS-1405 Remove unneeded test
* VFS-1405 Fix DBSync's tests
* VFS-1405 Update cluster_worker
* VFS-1581, Deps update
* VFS-1405 Fix DBSync's persistent state
* VFS-1405 Make DBSync's state persistent
* VFS-1581, Deps update
* VFS-1405 Improve DBSync performance
* VFS-1522 Change db_driver to couchdb in fuse_config_manager_test_SUITE.
* VFS-1522 Remove log spam during file synchronization.
* VFS-1581, Deps update
* VFS-1522 Find blocks to transfer in all file locations.
* VFS-1522 Find blocks to transfer in all file locations.
* VFS-1581, Deps update
* VFS-1405 Fix handling sync_gateway status
* VFS-1581, Test and deps update
* VFS-1405 Improve intervals in dbsync
* VFS-1522 Extract get_block_for_sync to separate module.
* VFS-1581, Test update
* VFS-1581, Deps update
* VFS-1522 Fix rtransfer tests.
* VFS-1581, Tests update
* VFS-1581, Tests update
* VFS-1581, Tests update
* VFS-1581, Test update
* VFS-1581, repair tests with initializer
* VFS-1405 update deps
* VFS-1522 Do not load initializer module on init.
* VFS-1405 Fix dbsync_test_SUITE
* VFS-1405 Fix dbsync_test_SUITE
* VFS-1405 update deps
* VFS-1522 Update dependencies, remove usage of deprecated functions.
* VFS-1405 Update cluster_worker
* VFS-1522 Update gui.
* VFS-1522 Update clproto.
* VFS-1522 Update gui.
* VFS-1522 Update rebar dependencies.
* VFS-1405 Speedup dbsync_test_SUITE
* Releasing new version 3.0.0-alpha
* Update vsn in app.src file
* Update deps
* VFS-1405 Fix dbsync_test_SUITE's filename generator
* VFS-1405 Fix dbsync startup
* VFS-1405 Fix session_manager_test_SUITE
* VFS-1405 Change deafult db driver
* Squashed 'bamboos/' changes from 2f522cc..6c375f4
* VFS-1522 Bugfixes and improvements of replication.
* HOTFIX update deprecated npm dep
* Update submodules
* VFS-1405 Remove utild:mtime/0 reference
* VFS-1522 Add tests of client notifications, fix wrong notify functions, do not notify on every change in dbsync_events.
* VFS-1522 Better management of file size changes.
* VFS-1508 revert unwanted changes
* VFS-1508 revert unwanted changes
* VFS-1522 Minor bugfixes related with remote transfer.
* VFS-1522 Improve rtransfer callback functions.
* VFS-1508 please dialyzer
* VFS-1522 Adjust helpers_nif to new helpers.
* Squashed 'helpers/' changes from 1f79408..02abcd2
* VFS-1522 Remove gwproto nif.
* VFS-1522 Add replica invalidation based on changes.
* VFS-1508 update submodules refs
* VFS-1508 add gui clean target
* VFS-1508 update ctool ref
* VFS-1508 update gui ref
* VFS-1529 Add delayed storage test file verification.
* VFS-1522 Store recent changes in file_location doc.
* VFS-1508 update submodules refs
* Squashed 'appmock/' changes from 68aaf01..835e98c
* VFS-1508 update submodules refs
* VFS-1522 Remove dbsync spam, start rtransfer during fslogic_worker init, extend routing of messages with no response.
* Squashed 'bamboos/' changes from bb39552..2f522cc
* Squashed 'appmock/' changes from 2f5d162..68aaf01
* VFS-1508 update submodules refs
* VFS-1508 fix a bug in bamboos
* VFS-1508 add clean target
* Squashed 'appmock/' changes from 0999c5d..2f5d162
* VFS-1508 update submodules refs
* VFS-1508 update make.py to use keys from .ssh/docker by default
* Squashed 'bamboos/' changes from ca613bd..bb39552
* VFS-1508 update submodules refs
* VFS-1508 add gui targets to makefile
* VFS-1528 Make unique_integer always positive
* VFS-1603 Update ctool and cluster_manager references.
* VFS-1528 Remove new utils:time usage.
* Squashed 'appmock/' changes from 0999c5d..56b4db3
* VFS-1522 Add greedy invalidation of file blocks.
* VFS-1571 Update ctool, cluster-worker and rebar.
* Squashed 'appmock/' changes from edf439f..0999c5d
* Squashed 'bamboos/' changes from 69ff95b..ca613bd
* VFS-1522 Add test of file synchronization.
* VFS-1520 - bugfix, wrong module name
* VFS-1405 Fix dbsync tests
* VFS-1508 update cluster-worker ref
* VFS-1405 Improve dbsync tests
* VFS-1508 update cluster-worker ref
* VFS-1508 update ctool ref
* Update ctool
* VFS-1508 update cluster-worker ref
* VFS-1603 Preserve .git/ under cluster_worker/ when packaging.
* VFS-1508 add tags to rebar.config
* VFS-1405 Improve dbsync tests
* VFS-1508 fix docs building command
* VFS-1508 update ctool ref
* WIP
* VFS-1520-update ctool ref
* VFS-1508 minor refactoring
* VFS-1603 Preserve .git/ under vendor/ when packaging.
* VFS-1508 update gui ref
* VFS-1508 use node from repo rather than nvm
* VFS-1405 Speed up dbsync test
* VFS-1520 - update ctool ref
* VFS-1405 Improve code style
* VFS-1522 Add synchronize_block operation, refactor fslogic_block module.
* VFS-1603 Fix whitespaces in Makefile.
* update cluster-worker
* VFS-1522 Introduce version_vectors and add versioning of update operations.
* VFS-1505 Updating cluster_worker and ctool
* VFS-1405 Improve code style
* Releasing new version 3.0.0
* Update vsn in app.src file
* Delete ReleaseNotes
* VFS-1505 Updating cluster_worker and ctool
* Dependencies management update
* VFS-1528 Remove ctool utils time methods
* VFS-1508 update ctool and gui refs
* VFS-1522 Do not modify file_locations of other providers.
* VFS-1508 update ctool ref
* VFS-1616: Removing some comments
* VFS-1508 add wget to deb build depends
* VFS-1616: Vertical scroll for temporary Recent view
* VFS-1520 - delete empty lines between test and test_base
* VFS-1508 update gui ref
* VFS-1520 - update ctool ref
* VFS-1520 - delete annotations/performance.erl from erl_first_files
* VFS-1522 Add truncate_should_change_size_and_blocks test.
* VFS-1508 make rel now creates production release, and make test_rel dev release
* VFS-1522 Uncomment correct assertion.
* VFS-1522 Add write_should_add_blocks_to_file_location test.
* Squashed 'bamboos/' changes from d7ccb09..ce6e993
* VFS-1508 fix nvm sourcing during building
* VFS-1616: Forcing Ember version in bower.json
* VFS-1603 Add bc tool to package dependencies.
* VFS-1603 Add distribution check to package rule in Makefile.
* VFS-1616: Restored user name in top bar
* VFS-1505 Reverting ctool and cluster_worker update
* VFS-1505 Changing size -1 to undefined
* VFS-1616 Bower package resolutions (to prevent build prompt)
* VFS-1520 - move utils to src/test_utils to fix tests
* VFs-1508 update gui ref
* VFS-1520 - delete commented out line
* VFS-1520 - move test utils back to test_distributed directory
* VFS-1616 Extended documentation in JSDoc format
* VFS-1522 Extend dbsync_trigger_should_create_local_file_location test.
* VFS-1522 Create local file_location when external file_meta arrives.
* VFS-1520 - update ctool ref, delete test_distributed from src_dirs
* VFS-1520 - update usage of performance_macros
* VFS-1520 - move test utils from test_distributed to src/test_utils
* VFS-1603 Update package dependencies.
* VFS-1616 Integrating prototype Ember App from various into op-worker
* VFS-1508 update rpm build preparation to adjust to ember-cli
* VFS-1508 update description of gui.config file
* VFS-1508 fix a minor bug in provider.up
* VFS-1508 fix a minor bug in provider.up
* VFS-1471 update bind addr for provider_listener
* VFS-1508 direct output from ember live build to log file in dev environment
* VFS-1405 update ctool
* VFS-1522 Create replication test SUITE.
* VFS-1405 update cluster_worker
* VFS-1405 fix most ct tests
* VFS-1520 - update ctool-ref
* VFS-1520 - comment-out 'no-auto-compile ' opt from ct_run
* VFS-1520-test_distributed deleted from src_dirs
* VFS-1520-test_distributed deleted from src_dirs
* VFS-1520-delete comments from rebar.config
* VFS-1522 Update gui.
* VFS-1522 Update ctool and cluster_worker.
* VFS-1522 Update ctool and cluster_worker.
* VFS-1603 Update package dependencies.
* VFS-1522 Update clproto.
* VFS-1505 Fixing wrong test assertions
* VFS-1520 delete commented-out lines
* VFS-1520 set cluste-worker ref to develop
* VFS-1529 Use fslogic worker for remote client storage configuration.
* VFS-1520 delete annotations from more tests
* VFS-1522 Fix wrong types.
* VFS-1471 revert all deps
* VFS-1520-code improvements
* VFS-1529 Fix dialyzer.
* VFS-1505 Refactoring times updating and emitting
* VFS-1529 Add mechanism for automatic detection of directly accessible storages.
* VFS-1505 Fixing file attrs emitting in read_dir
* VFS-1520 - update ctool ref
* VFS-1505 Handling read atime update by read_events
* VFS-1520 - annotations deleted from all ct tests in op_worker
* VFS-1520 - annotations deleted in another 4  tests
* VFS-1520 - annotations deleted in more tests
* VFS-1505 Avoid sending events when atime does not change
* VFS-1505 Bugfix
* VFS-1520 - deleted annotations from few tests
* VFS-1505 Updating ctool and cluster_worker to versions allowing negative file size value
* VFS-1508 fix a repetition in gitignore
* VFS-1508 remove old gui files
* VFS-1508 add gui cleaning to clean target to rebar.config
* VFS-1508 update ctool ref
* VFS-1508 fix a bad spec
* VFS-1505 Fixing tests failing unintentionally due to timestamps updating
* VFS-1508 minor fix
* VFS-1508 improve some specs and comments
* VFS-1508 improve some specs and comments
* VFS-1471 fix provider_listener healthcheck
* VFS-1520 - stress_test passing
* VFS-1508 fix callback mechanism based on websocket adapter
* VFS-1522 Add dbcync/common include to dbsync_events.
* VFS-1522 Update ctool and cluster_worker.
* VFS-1522 Add replication_db_sync_handler module.
* VFS-1508 add missing storage options to env, improve env.up to fail if env configuration fails
* VFS-1405 wait for datastore startup
* VFS-1522 Update cl_proto.
* VFS-1471 fix dialyzer
* VFS-1471 several code style improvements
* VFS-1471 several code style improvements
* VFS-1405 revert helpers_test_SUITE
* VFS-1405 fix dialyzer
* VFS-1505 Change time events sending attrs excluding size
* VFS-1520 - annotations deleted from connection_test_SUITE and sequencer_performance_test_SUITE
* VFS-1508 add gui tests to makefile
* VFS-1471 fix dialyzer
* VFS-1471 fix dialyzer
* VFS-1405 several spec fixes
* VFS-1414 Correcting types
* VFS-1508 revert some unneeded changes
* VFS-1508 fix an empty dir being left out in git
* Squashed 'bamboos/' changes from b146078..d7ccb09
* VFS-1471 add provider_communicator
* VFS-1508 add a test file
* VFS-1508 change space select element to buttons
* VFS-1414 refactor logical files maganer
* VFS-1466 fix
* VFS-1466 update ctool ref
* Merge branches 'develop' and 'feature/VFS-1466-integrate-rtransfer-acceptance-tests' of ssh://git.plgrid.pl:7999/vfs/op-worker into feature/VFS-1466-integrate-rtransfer-acceptance-tests
* Revert "Merge pull request &#35;713 in VFS/op-worker from feature/VFS-1414-refactor-lfm-move-all-logic-to-lfm_xxx to develop"
* Revert "Merge pull request &#35;713 in VFS/op-worker from feature/VFS-1414-refactor-lfm-move-all-logic-to-lfm_xxx to develop"
* VFS-1405 fix clproto reference
* VFS-1476, ckuster_worker update
* VFS-1405 fix clproto reference
* VFS-1508 proceed with migrating gui prototype to new ember
* VFS-1405 update docs
* VFS-1504 Refactoring moving_into_itself
* VFS-1466 update ctool ref
* VFS-1505 Update times after file operations
* VFS-1466 added assert in request_bigger_than_file_test
* VFS-1414 Fix after merging
* Add map for helpers IO service. Test open and mknod flags.
* VFS-1508 add test file
* VFS-1508 migrate prototype gui to ember-cli based gui
* VFS-1476, tests update
* VFS-1508 rework gui structire in repo
* VFS-1508 add symbolic link to gui tests in project root
* VFS-1508 introduce a switch in env.json to toggle gui livereload
* VFS-1405 use protocol_endpoint for interprovider communication
* VFS-1405 revert appmock
* Squashed 'bamboos/' changes from 9dfea37..0665aad
* Merge remote-tracking branch 'origin/develop' into feature/VFS-1405-local-dbsync
* VFS-1522 Update cluster_worker.
* Merge remote-tracking branch 'origin/develop' into feature/VFS-1405-local-dbsync
* Merge remote-tracking branch 'origin/develop' into feature/VFS-1405-local-dbsync
* VFS-1405 Update cluster_worker and remove some logs
* VFS-1522 Update ctool.
* VFS-1508 slightly rework provider_up
* VFS-1508 change owner of GUI tmp dir periodically rather than run ember build as user
* VFS-1508 run livereload as user
* VFS-1522 Update clproto, remove space_info from dbsync.
* VFS-1421 Update cluster_worker.
* VFS-1421 Update clproto.
* VFS-1476, stress test update
* VFS-1466 added assert in request_bigger_than_file_test
* VFS-1476, deps update
* VFS-1421 Do not log error when owner permission check fails.
* VFS-1528 Remove deprecated use of erlang:now/0
* Fix space ID on storage.
* VFS-1508 update submodules refs
* VFS-1508 remove some unused code, improve some specs
* VFS-1466 - request_bigger_than_file_test passing
* VFS-1524 Fix node ip address check.
* VFS-1421 Move storage mocking into initializer.
* VFS-1508 change build process of GUI to use ember-cli, add livereload
* VFS-1466 - offset_greater_than_file_size_test passing
* VFS-1421 Add missing groups for test user.
* VFS-1421 Remove undefined type.
* VFS-1421 Merge changes.
* VFS-1421 Update clproto.
* VFS-1421 Update ctool and cluster_worker.
* VFS-1421 Fix dialyzer.
* VFS-1421 Fix ct tests.
* VFS-1405 Update cluster_worker
* VFS-1524 Add new space_info model instead of using space_details record form ctool. Disable failing cover in eunit.
* Add environment setup in helpers NIF unit tests.
* Squashed 'bamboos/' changes from b146078..69ff95b
* Squashed 'helpers/' changes from 5822b42..1f79408
* VFS-1524 Add space name-id separator alias.
* VFS-1405 Update proto
* VFS-1466 - add prepare_rtransfer auxiliary function
* VFS-1524 Adjust integration tests to new file meta structure.
* VFS-1421 Fix fslogic_proxyio eunit.
* VFS-1500, decrease memory usage
* VFS-1524 Change space storage name to space ID. Resolve space name clash problem.
* VFS-1421 Fix eunit typo.
* VFS-1421 Refactor cdmi_acl name usage.
* VFS-1466 - change expected error messages
* VFS-1466 - delete auxiliary test_fetch function
* VFS-1504 Checking if directory is not moved into its subdirectory
* VFS-1466 Fix string/binary mismatch in rtransfer NIFs.
* VFS-1466 - use auxiliary functions for calling rtransfer
* VFS-1466 - delete call to fetch_test
* VFS-1421 Change fslogic_spaces:get_space to return space when asking as root.
* Squashed 'bamboos/' changes from a9641e4..9dfea37
* VFS-1421 Fix dialyzer.
* Squashed 'bamboos/' changes from ea16980..a9641e4
* VFS-1476, Minor corrections after merge
* Squashed 'helpers/' changes from e12e0d0..5822b42
* VFS-1421 Add recommended changes.
* VFS-1466 - auxiliary fetch_test function
* VFS-1476, Minor corrections after merge
* VFS-1476, Tests update
* VFS-1466 - test fetching data which size is not the multiple of block size
* VFS-1421 Adjust proxyio to new changes in fslogic_spaces:get_space interface.
* VFS-1518 Fix events performance tests.
* VFS-1421 Fix merge errors.
* VFS-1518 Fix sequencer performance tests.
* VFS-1421 Update ctool, cluster-worker, clproto.
* VFS-1466 - delete setting cookie from test
* VFS-1421 Add malformed query string error message.
* VFS-1405 Sync DB in space context only
* VFS-1484 Enable storage lookup by name.
* VFS-1484 Set number of threads for Amazon S3 storage helper IO service.
* VFS-1405 Fix direct batch update
* VFS-1405 Fix protocol errors
* VFS-1477, Skip sequencer performance tests
* VFS-1421 Send PermissionChangedEvent as list of events.
* VFS-1428 Fix dialyzer.
* VFS-1484 Fix dialyzer.
* VFS-1421 Add translations for aggregated acl types.
* VFS-1405 Fix protocol errors
* VFS-1477, Skip events performance test
* VFS-1421 Handle proxyio exceptions, adjust lfm_files_test to new api.
* VFS-1421 Update clproto.
* Squashed 'helpers/' changes from d64a745..e12e0d0
* VFS-1484 Add client configuration mechanism.
* VFS-1421 Fix failing eunits.
* VFS-1234 fix protocol errors
* VFS-1234 fix protocol errors
* VFS-1234 implement inter-provider communication
* VFS-1421 Add permission_changed_event.
* VFS-1477, Deps update
* Squashed 'helpers/' changes from 3142e3c..aff4303
* VFS-1472 Add documentation.
* VFS-1477, Deps update
* VFS-1421 Fix minor issues discovered during hands on cdmi test.
* VFS-1414 Moving all logic to lfm_xxx modules
* VFS-1477, Deps update
* Squashed 'helpers/' changes from bc04786..3142e3c
* VFS-1421 Remove unnecessary log.
* VFS-1477, Tests update
* VFS-1414 Removing types.hrl
* VFS-1472 Add librados and libs3 package dependencies.
* VFS-1405 Support for dbsync proto
* VFS-1472 Add IO service for Amazon S3 storage helper to factory.
* Squashed 'helpers/' changes from d64a745..bc04786
* VFS-1472 Adjust fslogic to Amazon S3 storage helper.
* VFS-1405 Fix merge
* VFS-1421 Rename space_id to file_uuid in proxyio messages.
* VFS-1421 Remove spaceId from get_helper_params message.
* VFS-1414 Swapping Limit and Offset arguments in lfm_dirs:ls
* VFS-1474 Changing matching to assertions, adding comments
* VFS-1421 Change space_id to file_uuid in proxyio_request.
* VFS-1421 Add proper handling of accept headers in rest requests, fix some minor bugs.
* VFS-1474 Removing mkdir wrapper
* VFS-1459 bugfix
* VFS-1421 Chmod on storage with root privileges during set_acl operation.
* VFS-1474 Fixing types
* VFS-1421 Update ctool and cluster-worker.
* VFS-1474 Creating acl permissions tests
* VFS-1421 Enable permission checking on storage_file_manager open operation.
* VFS-1421 Change helper StorageID param to FileUUID.
* VFS-1466 - change write_fun in more_than_block_fetch_test
* VFS-1459 handle dir record in translator.erl to fix connection:timeout in oneclient
* VFS-1428 Update ctool reference.
* Squashed 'helpers/' changes from ebab919..d64a745
* Squashed 'bamboos/' changes from f3a9a31..b146078
* VFS-1428 Fix dir record translation.
* VFS-1428 Add list of application ports to config file.
* VFS-1426 Add gateways to a process group.
* VFS-1421 Add permission control to storage_file_manager.
* VFS-1428 Add cluster manager symlink to gitignore.
* Squashed 'bamboos/' changes from b605f93..f3a9a31
* Squashed 'helpers/' changes from 6c1d616..ebab919
* VFS-1474 Creating posix permissions tests
* VFS-1466 - delete TODO from rtransfer_test_SUITE, update ctool ref
* VFS-1148 update some deps
* VFS-1148 update cluster worker ref
* VFS-1148 remove unused env
* VFS-1148 add port checking on application init
* VFS-1148 remove some wrongly merged code
* VFS-1148 remove some wrongly merged code
* VFS-1148 remove some wrongly merged code
* VFS-1148 fix couple of badmatches resulting from api change
* VFS-1148 please dialyzer
* VFS-1148 please dialyzer
* VFS-1148 please dialyzer
* VFS-1459 - fix not visible files in default space
* VFS-1148 lots of cleaning up, adding specs and comments
* VFS-1148 add a lot of specs and comments
* VFS-1421 Remove done todo items.
* VFS-1421 Do not allow direct modification of cdmi extended attributes.
* VFS-1421 Add mimemetype, completion_status and transfer_encoding management to logical_file_manager api.
* Squashed 'bamboos/' changes from 4a4a179..ea16980
* Squashed 'bamboos/' changes from 258e68c..4a4a179
* VFS-1148 update cluster-worker ref
* VFS-1466-update env.json, update ctool
* VFS-1493,  Deps update
* VFS-1493,  Deps update
* VFS-1426 Fix type errors.
* VFS-1421 Fix some permission requirements, adjust cdmi_object_handler to new permission model, reenable acl tests.
* VFS-1148 update ws_adapter file
* VFS-1148 add some specs
* VFS-1431- update ctool
* VFS-1493, Fix stop deadlock with cluster_worker
* VFS-1431- fixes
* VFS-1421 Extract ?run macro to separate module, export file_meta:to_uuid function.
* VFS-1421 Add set_acl, get_acl, remove_acl as separate fslogic requests, with proper permission control.
* VFS-1421 Check permissions on rename operation, repair incorrect mock in fslogic_req_test_SUITE.
* VFS-1421 Fix wrong include path in eunit.
* VFS-1421 Return 401 in case of unauthorized access to objects by objectid.
* VFS-1421 Update .gitignore.
* VFS-1421 Set http_code to 400 in test of requests with wrong content-type.
* VFS-1421 Fix wrong file_doc parameter.
* VFS-1421 Fix wrong check_permissions path in erl_first_files, update .gitignore.
* VFS-1428 Fix dialyzer and ct tests.
* VFS-1148 improve sync on prototype webpage
* VFS-1431- few tests passing, only on modified rtransfer_server (problem with hiding testmaster node)
* VFS-1421 Perform fsync after creation of file throught REST request.
* VFS-1428 Fix default space mapping in fslogic.
* Squashed 'bamboos/' changes from b605f93..258e68c
* VFS-1148 add sync button on prototype webpage
* VFS-1148 update gui ref
* VFS-1428 Add user context to fslogic:get_spaces function.
* VFS-1148 adjust listeners to new cluster_worker API
* VFS-1431- starting environment per testcase to start rtransfer per testcase
* VFS-1428 Enable multiple ceph user credentials.
* VFS-1428 Fix passing proxy IO helper arguments.
* VFS-1428 Fix setting space ID in fslogic context.
* VFS-1428 Fix space storage uuid.
* VFS-1428 Fix exception handling in helpers nif.
* VFS-1428 Fix space to storage mapping.
* VFS-1428 Fix fslogic context setup.
* VFS-1428 Update riakc version.
* VFS-1428 Pass arguments from Ceph helper to Ceph helper context.
* VFS-1148 fix return value from gui listener starter
* VFS-1431-tests starting, not skipping, but failing
* VFS-1421 Update ctool and cluster_worker
* VFS-1421 Additional merge changes.
* VFS-1148 update ctool ref
* VFS-1148 update ctool ref
* Squashed 'helpers/' changes from aa46c42..6c1d616
* VFS-1457 update deps
* VFS-1431-tests are starting
* VFS-1421 Merge changes.
* VFS-1400 Add debug_info flag to erl_compile script.
* VFS-1400 Refactor code and fix failing tests.
* VFS-1428 Adjust client to new helpers design.
* Squashed 'bamboos/' changes from ff23669..b605f93
* Squashed 'appmock/' changes from 3a6cc48..edf439f
* VFS-1457 updating deps
* Squashed 'helpers/' changes from 56d8ba8..aa46c42
* VFS-1457 updating cw dependency
* Squashed 'appmock/' changes from 33f272e..3a6cc48
* Squashed 'bamboos/' changes from 38d8036..ff23669
* VFS-1400 Fix cdmi_test_SUITE.
* Merge branch 'develop' into VFS-1402-redirect-to-correct-path
* VFS-1421 Adjust new tests, refactor cdmi error handling.
* VFS-1457 updating op-worker to custer-manager name change
* VFS-1457 fix ccm app name
* VFS-1421 Add cdmi_acl test.
* VFS-1457 initial cluster-manager renaming
* VFS-1400 Add missing spec.
* VFS-1400 Move http_worker files from src/modules/http_worker to src/http.
* VFS-1148 revert some changes concerning cdmi
* VFS-1148 merge with cluster worker
* VFS-1148 add sync button in top menu
* VFS-1405 Implement initial protocol logic
* VFS-1426 Migrate rtransfer from 2.0
* VFS-1421 Add acl validation, annotate with access checks common fslogic functions.
* VFS-1148 allow choosing where to create new files and dirs
* VFS-1148 add ability to create new fiels and dirs in gui
* VFS-1148 add preview and edition of files
* VFS-1429 Fix dialyzer.
* VFS-1382 do not depend on branch
* VFS-1382 do not depend on branch
* VFS-1421 Refactor check_permissions annotation.
* VFS-1148 fix a typo
* VFS-1382 manual merge fixes
* VFS-1421 Integrate acls with cdmi.
* VFS-1382 manual merge fixes
* VFS-1148 file browser allows to preview text files
* VFS-1402 - code improvements
* VFS-1401 - reformat code
* VFS-1382 changing cookie
* VFS-1382 review issues addressed (nagios name + refactoring)
* VFS-1421 Refactoring of check_permissions annotation.
* VFS-1382 moving stres tests
* VFS-1376 Fix deadlock in sequencer manager.
* Squashed 'bamboos/' changes from 63b9ff6..38d8036
* VFS-1382 addressing review issues
* VFS-1382 addressing review issues
* VFS-1148 working prototype of basic file browser
* VFS-1421 Add acls to logical_file_manager, add acl setting integration test.
* VFS-1402 - fix dialyzer
* VFS-1402 -delete change in provider_worker.py
* VFS-1402 - delete_dir_test and moved_permanently_test passing
* VFS-1376 Apply recommended changes.
* VFS-1421 Add groups to test environment.
* VFS-1421 Add acl conversion unit test.
* VFS-1421 Add onedata_group model and implement basic operations on acl.
* VFS-1382 including appmock in op-worker only
* VFS-1382 addressing review comments
* VFS-1400 Fix cdmi_id eunit test, extend erl_compile script.
* VFS-1402 - bugfix in cdmi_existence_checker
* VFS-1402 - remove and add trailing slash in cdmi_existence_checker
* VFS-1400 Enable new tests, fix streaming errors.
* Squashed 'bamboos/' changes from 7bd9d8f..63b9ff6
* VFS-1402 - delete lfm_utils:ls_all function
* VFS-1402 - bug fixes and code improvement
* VFS-1382 restoring make.py
* VFS-1382 restoring make.py
* VFS-1400 Refactoring.
* VFS-1376 Fix ct tests.
* VFS-1400 Cdmi objectid implementation.
* VFS-1398 Remove unused function, match {not_found, xattr}.
* VFS-1382 added 'up' scripts, example json, fixed comments
* VFS-1382 added 'up' scripts, example json, fixed comments
* VFS-1382 initial script refactoring
* VFS-1382 initial script refactoring
* VFS-1401-code improvements
* VFS-1405 Implement logic that assigns space to given document
* VFS-1398 Update ctool.
* VFS-1382 fixing dialyzer
* VFS-1398 Improve documentation.
* VFS-1398 Change function call from global to local.
* VFS-1382 merge & fixes: bamboos + cluster-worker
* VFS-1382 merge & fixes: bamboos + cluster-worker
* VFS-1400 Compile only files from src in erl_compile.sh script.
* VFS-1400 Add compilation utility script.
* VFS-1148 first attempts at file manger page
* VFS-1401- mock opening file without permission in errors_test, delete try-catch from stream functions
* VFS-1382 hacking performance annotations
* VFS-1429, Add fsync to lfm
* VFS-1382 hacking performance annotations
* VFS-1339 rmdir fix.
* VFS-1339 Delete container test.
* VFS-1402 Change redirection paths.
* VFS-1401-improved handling exceptions
* VFS-1339 Fix and update directory deletion.
* VFS-1376 Pass session type to event stream init handler.
* VFS-1418 rework gui_session API
* VFS-1382 making op-worker build with releaseable cluster-worker
* VFS-1371 Add fslogic_proxyio eunit tests.
* VFS-1429 Enable event stream flush.
* VFS-1148 update clproto ref
* VFS-1382 moving ct coverage to cluster-worker
* VFS-1402 Refactor.
* VFS-1376 Fix dialyzer errors.
* VFS-1398 Fix wrong get_xattr return type.
* VFS-1401-merge with VFS-1398
* VFS-1398 Reorganize xattr records definitions.
* VFS-1376 Fix compilation error.
* VFS-1401 - error_test passes
* VFS-1382 moving op_ccm appmock
* VFS-1382 making cluster-worker release possible
* VFS-1398 Fix dialyzer.
* VFS-1148 fix some macros being not up-to-date with websocket adapter
* Squashed 'helpers/' changes from a38f2b2..56d8ba8
* VFS-1402 CDMI redirections based on trailing slashes.
* VFS-1382 rest listener env fix
* VFS-1382 plugin names as env variables & plugin defaults
* VFS-1382 reworking healthcheck & http_worker
* VFS-1382 reworking healthcheck & http_worker
* VFS-1401 - error_test starts and fails
* VFS-1400 Merge changes.
* VFS-1400 Add first version of objectid support.
* VFS-1148 update gui ref
* VFS-1398 Fix wrong xattr document key.
* VFS-1398 Update ctool.
* VFS-1148 add a todo note
* VFS-1401 - error_test moved from demo and updated
* VFS-1398 Update ctool.
* VFS-1148, rename static data backend behaviour
* VFS-1340 - fixes
* VFS-1148 rework static data backend -> callback backend
* VFS-1340 - code improvements
* VFS-1340 - fixes
* VFS-1340 - handling error cases 2
* VFS-1340 - handling error cases
* VFS-1398 Add objectid tests
* VFS-1398 Fix xattr link removal.
* VFS-1398 Add cdmi_metadata test, improve handling certificates, add temporary workaround for not removable links.
* VFS-1340 -all tests passing
* VFS-1340 -2 bug fixes after merge
* VFS-1398 Add xattrs to onedata_file_api and cdmi_metadata implementation.
* VFS-1376 Fix spec descriptions.
* VFS-1340 - bug fixes after merge
* VFS-1340 - fix spelling mistake
* VFS-1340 - after merge with VFS-1403
* VFS-1398 Update ctool.
* VFS-1398 Add todo to fix removal of datastore links and disable failing tests.
* VFS-1403 Typo fix.
* VFS-1398 Fix minor errors, add crud_xattr_test.
* VFS-1398 Fix minor errors, add set_and_get_xattr_test.
* VFS-1403 Fix dialyzer.
* VFS-1382 changing supervision tree
* VFS-1340 delete tracer calls
* VFS-1340 fixing dialyzer
* VFS-1382 global definitions trimmed
* VFS-1376 Add failures logging threshold to sequencer out stream.
* VFS-1382 datastore cache level removed from plugins
* VFS-1382 node_manager modules refactoring
* VFS-1398 Updated specs, added set_and_get_xattr_test.
* VFS-1398 Add xattr model and xattr operations to fslogic.
* VFS-1403 Remove mock validation in 'choose_handler_test', do not fail if session is reused during init.
* VFS-1382 fixed listener starting
* VFS-1382 fixing authorship
* VFS-1382 fixing authorship
* VFS-1382 listener_starter renamed
* VFS-1340 list_dir_test passing
* VFS-1382 reworked datastore inludes
* VFS-1403 Add recommended changes.
* VFS-1406, cache controller minor update
* VFS-1405 Implement logic that assigns space to given document
* VFS-1405 couchdb drive up and working
* Merge remote-tracking branch 'remotes/origin/develop' into feature/VFS-1382-cluster-framework-retry - fixes
* VFS-1363 Default open mode to rdwr in translator
* VFS-1382 fixed todos
* VFS-1340 bugfix
* VFS-1340 create_file_test passing
* VFS-1403 Remove usage of mimetype taken from attrs.
* VFS-1403 Update ctool.
* VFS-1403 CDMI object PUT operation + tests.
* VFS-1404 Export types from onedata_file_api.
* Merge remote-tracking branch 'remotes/origin/develop' into feature/VFS-1382-cluster-framework-retry
* VFS-1407 Fix dialyzer.
* VFS-1404 Dialyzer fixes.
* VFS-1382 reverted 13330e9a73cae11c0361e1d84fcd62cd555f7ddd
* VFS-1382 fixed coverage script
* VFS-1404 Typo fix.
* VFS-1404 Remove call to undefined function.
* VFS-1148 fix wrongly renamed file
* VFS-1404 Fix dialyzer errors.
* VFS-1363 fix undefined storage in file_blocks
* VFS-1148 make cluster_elements_test more generic
* VFS-1363 disable umask in helpers_nif
* VFS-1363 fix auto parent mkdir
* `VFS-1363 fix uid & gid attribute
* VFS-1363 Fix type errors in tests
* VFS-1382 fixed listener starter spec
* VFS-1407 Sending subscriptions with handshake response.
* VFS-1148 remove some old code, rework some paths
* VFS-1404 Fix dialyzer errors.
* VFS-1148 fix an include
* VFS-1404 Update ctool.
* VFS-1407 Add mechanism that will remove inactive sessions after timeout.
* VFS-1148 update gui ref
* VFS-1148 fix a typo, update ctool ref
* VFS-1378 revert unwanted changes
* VFS-1378 merge with develop
* VFS-1378 merge with develop
* VFS-1378 merge with develop
* VFS-1338 Adjust tests to new functions.
* VFS-1378 please dialyzer
* VFS-1378 please dialyzer
* VFS-1378 please dialyzer
* Squashed 'op_ccm/' changes from 1bf1ad8..fa69024
* Squashed 'appmock/' changes from 0e1718a..33f272e
* VFS-1378 update ctool ref
* VFS-1378 update ctool ref
* Squashed 'op_ccm/' changes from 5f961ba..1bf1ad8
* Squashed 'appmock/' changes from 8d64333..0e1718a
* VFS-1378 update ctool ref
* Squashed 'op_ccm/' changes from a6681d8..5f961ba
* Squashed 'appmock/' changes from 5490d65..8d64333
* VFS-1378 update ctool ref
* VFS-1378 update ctool ref
* VFS-1148 please dialyzer, reformat some code
* VFS-1378 move http_common.hrl to new location
* VFS-1338 Fix compilation error.
* VFS-1148 adjust to new ctool API
* VFS-1338 Use TEMP_DIR macro in tests.
* VFS-1148 remove unused code
* VFS-1403 Apply code review suggestions.
* VFS-1378 revent lfm_files_SUITE changes
* VFS-1378 merge bamboos
* VFS-1378 merge bamboos
* VFS-1148, merge with 1388
* VFS-1382 fixed cover spec
* VFS-1340 merge with 1404
* VFS-1378 please dialyzer
* Squashed 'op_ccm/' changes from 2197200..a6681d8
* Squashed 'op_ccm/' changes from 2197200..a6681d8
* Squashed 'appmock/' changes from 35a0fc4..5490d65
* Squashed 'bamboos/' changes from 5a48c50..4a4a179
* VFS-1378 update ctool ref
* VFS-1378 update ctool ref
* VFS-1378 remove debug from tests
* VFS-1378 tests debug
* VFS-1378 tests debug
* VFS-1382 ip discovery is pluggable
* VFS-1340-bug fixes
* Merge commit '9e8300fe02bd34ad295018205298edbedc27e322' into VFS-1269-update_bamboos
* Squashed 'bamboos/' changes from 8c643a4..df96833
* Update env decriptions for integration tests.
* VFS-1382 fixing node_manager specs
* VFS-1404 Extract streaming functions to separate module.
* Squashed 'appmock/' changes from 9bad307..35a0fc4
* Squashed 'op_ccm/' changes from 9e4a019..2197200
* VFS-1378 fix http_worker to use ssl2 for conneciton tests
* VFS-1340-changed Identity to Auth in cdmi_container_answer
* Squashed 'op_ccm/' changes from 527bc6f..9e4a019
* Squashed 'op_ccm/' changes from 527bc6f..9e4a019
* Squashed 'appmock/' changes from c3bc2cc..9bad307
* VFS-1405 test and fix helpers_nif bad_alloc
* VFS-1382 covering cluster_worker
* VFS-1404 Update ctool.
* Switch to CouchBase in example environments.
* Squashed 'bamboos/' changes from c1c5316..5a48c50
* VFS-1378 update appmock's example mocked app description, add verify_gr_cert env to env decsriptions
* VFS-1371 Implement ProviderIO.
* Squashed 'bamboos/' changes from 9200fc7..c1c5316
* VFS-1378 add verify_gr_cert env to test envs
* VFS-1404 Cdmi object get.
* VFS-1378 remove jiffy dependency
* VFS-1405: draft of final couchdb driver
* VFS-1378 fix appmock mocked app descriptions in tests to conform to new ctool API
* VFS-1405: draft of final couchdb driver
* VFS-1340 list_dir_test crashes when creating file
* VFS-1378 please dialyzer
* VFS-1378 adjust tests to the new ctool API
* VFS-1382 moved dns listener to cluster worker
* VFS-1382 flattened packages
* VFS-1378 update ctool ref
* VFS-1382 fixed nagios test
* VFS-1340 bug fixes after merge
* VFS-1382 fixed cache clearing test
* VFS-1340 code improvements
* VFS-1397 Fix missing attrs during cdmi dir creation.
* Remove obsolete files in example_env.
* Even better error handling.
* VFS-1397 Replace identity with auth in container_handler.
* Squashed 'appmock/' changes from 8af9658..c3bc2cc
* VFS-1378 update app.src
* VFS-1397 Replace identity with auth.
* Safe calls in escript, logs from escript.
* Squashed 'op_ccm/' changes from cd9fd66..527bc6f
* Squashed 'op_ccm/' changes from cd9fd66..527bc6f
* Squashed 'appmock/' changes from 740af5e..8af9658
* Update example_env file.
* Squashed 'bamboos/' changes from 64ed1f0..9200fc7
* VFS-1378 adjust to new ctool API
* VFS-1397 Fix wrong encoding.
* VFS-1405: draft couchdb driver
* VFS-1340 list_dir_test, update_file_test and create_file_test compiling
* VFS-1338 - fix dialyzer
* VFS-1338 - fix dialyzer
* VFS-1340 code style improvements
* VFS-1405 use json in couchbase_datastore_driver as value format
* VFS-1338 fix dialyzer
* VFS-1397 Introduce rest session.
* Make acceptance tests work.
* VFS-1340-fix dialyzer
* VFS-1382 moving nagios handler coverage
* VFS-1382 moving nagios handler to cluster_worker
* VFS-1338 Move datastore_ops to main test dir, increase timeout.
* VFS-1382 reverted caches control back to node manager
* VFS-1338 Fix test initialization.
* fix dialyzer
* Merge commit '22cef73425acabbcb9ea6f5ea20e34f0a8458224' into bugfix/fix_couchbase_version
* Squashed 'bamboos/' changes from 34d9756..8c643a4
* Squashed 'bamboos/' changes from 83f737d..34d9756
* fix couchbase rebalance
* fix couchbase rebalance
* fix couchbase rebalance
* fix compile
* VFS-1363 add some docs
* VFS-1363 fix eunit test
* add some docs
* VFS-1338 Fix some of dialyzer errors.
* Merge remote-tracking branch 'remotes/origin/feature/VFS-1338-2-create-a-container-object-http' into feature/VFS-1341-read-a-data-object-using-http
* add some docs
* Merge commit '9b05008753e70f0c58d29fe86986d40fc726ef16' into bugfix/fix_couchbase_version
* Squashed 'bamboos/' changes from 10ce6c0..34d9756
* fix couchbase docker version
* fix couchbase docker version
* fix couchbase docker version
* chown logs better.
* Merge commit '10cfd9b151edc45343f51ccb943a6f9328e32425' into bugfix/fix_couchbase_version
* Squashed 'bamboos/' changes from 8a5001a..10ce6c0
* fix couchbase docker version
* fix couchbase docker version
* fix couchbase docker version
* VFS-1363 fix undefined storage in file_blocks
* VFS-1363 disable umask in helpers_nif
* VFS-1363 fix auto parent mkdir
* VFS-1363 fix uid & gid attribute
* VFS-1382 moved models belonging to cluster_worker
* VFS-1382 removing duplicated hrls & proto relocation
* VFS-1338 Fix helpers_test.
* Merge remote-tracking branch 'remotes/origin/feature/VFS-1338-2-create-a-container-object-http' into feature/VFS-1341-read-a-data-object-using-http
* VFS-1382 fixed added test by merge
* VFS-1338 Update ctool.
* VFS-1338 Cdmi container put.
* VFS-1382 added timeout to test failing on bamboo
* VFS-1376 Remove unnecessary modules replaced by events logic.
* VFS-1338 Repair failing cdmi tests.
* VFS-1289 Apply recommended changes.
* Use Erlang cookie defined in env.json file while creating provider spaces.
* Merge remote-tracking branch 'remotes/origin/feature/VFS-1338-2-create-a-container-object-http' into feature/VFS-1341-read-a-data-object-using-http
* Refactor
* Directory structure for new client description.
* Refactor.
* Update example environemnt descriptions.
* Handle key- and cert-files.
* VFS-1341- refactoring
* VFS-1338 Merge changes.
* VFS-1338 Update ctool.
* VFS-1289 Fix CT tests and dialyzer.
* VFS-1363 update helpers
* Squashed 'helpers/' changes from 4bd6479..a38f2b2
* Squashed 'helpers/' changes from 1a0ec85..4bd6479
* VFS-1334 Apply recommended changes.
* VFS-1338 Cache file attrs.
* VFS-1341- handle reading cdmi_object plus tests
* VFS-1363 Add user context to all storage_file_manager operations
* Refactor.
* Switch to binaries and new ctool.
* VFS-1338 Rename fake to dummy.
* References to CDMI documentation.
* Refator.
* Remove obsolete escript.
* Refactor.
* Refactor.
* Merge commit '05c4adeefbc0e59e35665a15a2de84618a38d576' into feature/VFS-1363-add-user-context-to-helpers-operations
* Squashed 'bamboos/' changes from 8a5001a..83f737d
* VFS-1341- handle deleting cdmi_object plus tests
* VFS-1341- space names added to Config in CT tests
* VFS-1341- cdmi_tests moved from demo, removed sleep in end_per_testcase
* VFS-1341- fixed datastore 'no_file_meta' error
* VFS-1341- spelling fixes in docs
* VFS-1338 Dialyzer adjustments.
* Refactor.
* Reformat.
* Integration tests for capabilities.
* VFS-1338 Fix specs, increase timeouts.
* VFS-1289 Sending multiple events in one message.
* VFS-1334 Minor change in cache controller.
* VFS-1334 Apply recommended changes.
* VFS-1382 fixed task manager test changing wrong env
* VFS-1382 dns listener starts with cluster_worker supervisor
* VFS-1378 adjust to new ctool API
* VFS-1378 adjust to new ctool API
* VFS-1382 removed test cases present in cluster_worker
* Move storage creation to dedicated function.
* VFS-1382 extracted provider related code from dns_worker
* Move storage creation to dedicated function.
* VFS-1334 Increase CT tests timeouts.
* VFS-1382 sharing node addresses configuration
* VFS-1382 separation of env
* VFS-1334 Update dialyzer.
* Refactor.
* Move hardcoded docker path to global variable.
* Integration tests for CDMI capabilities.
* Integration tests for CDMI capabilities.
* Integration tests for CDMI capabilities.
* Refactor.
* get_token.escript
* Basic capability request.
* VFS-1334 Remove sleeps from CT tests.
* VFS-1334 Datastore CT tests refactoring.
* VFS-1334 CT tests refactoring.
* VFS-1382 integration of cluster_worker with op_worker - initial changes
* Squashed 'bamboos/' changes from 8a5001a..64ed1f0
* VFS-1378 add ewb_client dep
* Create storages on provider.
* VFS-1382 op-worker related work removed from cluster-worker
* VFS-1382 node manager address checing is plugin responsibility
* VFS-1382 node_manager config extracted
* VFS-1382 datastore config related docs added
* VFS-1341- tests for cdmi GET request, only application/binary content-type
* VFS-1382 separated basic models in helper macros definition
* VFS-1382 extracted datastore config
* VFS-1382 fixed models not being used
* VFS-1382 separation of datastore models
* VFS-1343-2 updated doc
* VFS-1343-2 chenged is_authorized function doc
* VFS-1382 moved task_manager & fixed headers
* VFS-1343-2 deleted comment
* Merge remote-tracking branch 'remotes/origin/feature/VFS-1338-2-create-a-container-object-http' into feature/VFS-1343-2-authorization
* VFS-1338 Update ctool.
* VFS-1338 Implement mkdir operation, add tests of container creation to cdmi test SUITE.
* VFS-1382 moved datastore include files
* VFS-1382 moved dns for extraction
* VFS-1382 moved datastore for extraction
* Mock user auth in cdmi test suite.
* VFS-1382 separated packages meant to form cluster repo
* VFS-1382 node_manager plugin - extracted behaviour & ported implementation
* Merge changes.
* VFS-1338 Extract test user creation and file management to util module.
* Storage creation improvement
* VFS-1361 Readd appmock/deps
* Squashed 'appmock/' content from commit 740af5e
* VFS-1361 Remove appmock dir.
* Squashed 'op_ccm/' changes from 42ad2a7..cd9fd66
* VFS-1361 Update Jiffy to 0.14.4.
* Remove mounting oneclient.
* VFS-1338 Extract test session and test storage init functions to util module.
* VFS-1382 extracted node_manager listeners (cherry picked from commit 4611b74)
* VFS-1343-2 is_authorized moved to onedata_auth_api
* VFS-1339 Move cdmi modules to different packages. Implement binary dir put callback.
* VFS-1218 check permissions while opening a file based on "open flags"
* VFS-1289 Fix some of integration tests.
* VFS-1268-for compliance with acceptance tests
* VFS-1289 Add performance tests for sequencer API.
* Refactoring: move some code to new functions in common.py; move escript to its own file.
* VFS-1289 Add performance tests for events API.
* Fix pattern matching on maps.
* VFS-1289 Extend set of event and sequencer tests.
* VFS-1338 Extract api for protocol_plugins. Implement dir exists callback.
* VFS-1218 reformat of translator.erl
* VFS-1218 fix fslogic_blocks:lower test and implementation
* VFS-1218 fix compile
* VFS-1218 remove old and unused lfm_utils module
* VFS-1218 fix fslogic_blocks:lower and implement test for both lower and upper functions
* VFS-1218 add lfm_utils:call_fslogic
* VFS-1218 several code style changes
* Remove pattern matching on maps.
* Remove include of non-existing file.
* Remove include of non-existing file.
* Remove old router.
* VFS-1289 Extend set of event and sequencer tests.
* Refactor of malformed_request/2 and get_cdmi_capability/2.
* VFS-1338 Add fake rest session.
* Refactor.
* VFS-1218 fix open_mode type
* VFS-1218 fix tests
* VFS-1218 fix assertReceived test macro
* Fixed missing and redundant callbacks, docs linked to pre_handler, &#35;{} instead of dict().
* Remove unused cdmi_arg_parser:trim/1.
* Store path in State.
* VFS-1218 fix connection test
* Fixed maps in malfermed_request/2.
* Replace dict() with &#35;{}.
* VFS-1218 fix dialyzer
* VFS-1218 fix event_manager and helpers tests
* VFS-1218 fix event_manager and helpers tests
* VFS-1218 fix event_manager and helpers tests
* VFS-1218 fix event_manager and helpers tests
* Integration tests for CDMI version parsing.
* Map instead of dict.
* Fix handling undefined CDMI version in cdmi_arg_parser.erl.
* Include guard for cdmi_errors.hrl.
* Skeletons of capabilities handlers.
* Skeletons of capabilities handlers.
* New version parser.
* VFS-1218 fix connection and helpers tests
* VFS-1289 Extend event manager with client subscription mechanism.
* VFS-1327 Slightly update protocol plugins documentation and register protocol_plugin_behavior as erl_first_file.
* Extracting CDMI version and options from request.
* Extracting CDMI version and options from request.
* VFS-1327 Improve documentation, extend exception handler, new ct tests.
* VFS-1327 Separate rest and cdmi as abstract protocol plugins.
* VFS-1343 - is_authorized function moved to rest_auth module
* Remove creating spaces and users from provider_ccm.py
* Example env_json file.
* OS setup functions moved to common.py.
* VFS-1291 Add possibility to declare custom cowboy callbacks for different content-types.
* Fix indentation.
* VFS-1218 fix connection and helpers tests
* VFS-1218 fix connection and helpers tests
* VFS-1218 fix connection tests
* VFS-1291 Refactor rest handler modules.
* VFS-1291 Add routing to cdmi object/container modules and add some tests.
* VFS-1291 Fix typo in ct_run.py
* VFS-1291 Fix typo in ct_run.py
* Shared storage, system users and system groups for all provider nodes.
* TODO: mounting oneclient.
* VFS-1218 compile fix
* VFS-1218 code cleanup
* Done users and groups; done getting token; in progress mounting oneclient.
* VFS-1291 Add rest pre_handler that deals with exceptions. Update ctool.
* VFS-1291 Add rest test: internal_error_when_handler_crashes.
* VFS-1291 Add auto_compile option to ct_run.py.
* VFS-1291 Add auto_compile option to ct_run.py.
* VFS-1291 Do not use opn_cowboy_bridge in rest.
* fix mocked_app parameter not being optional
* VFS-1218 update some specs
* VFS-1218 update some specs
* VFS-1218 update some specs
* VFS-1218 update some specs
* VFS-1218 update some specs
* VFS-1218 revert bamboos
* VFS-1218 update some specs
* VFS-1218 update some docs
* VFS-1218 update some docs
* VFS-1218 update clproto
* VFS-1218 update docs
* VFS-1291 Rearrange http_worker modules hierarchy.
* VFS-1291 Add targets for appmock and ccm.
* VFS-1255 Bump Boost to 1.58 for compatibility with client.
* VFS-1228, Spec for rollback fun added
* VFS-1255 Fix package build.
* VFS-1293, Fix dialyzer
* VFS-1261 add docs
* Create non-root users on client dockers.
* VFS-1218 add some docs
* Shared storage on docker-environment.
* VFS-1148 update giu ref
* VFS-1148 differentiate relative and absolute redirection urls
* VFS-1148 add more funcitonalities to file namanger prototype
* VFS-1258, transactions tests
* VFS-1255 Update ctool.
* VFS-1218 merge delete_file with unlink
* VFS-1218 implement truncate and chmod
* VFS-1148 dynamic buttons in file manager
* VFS-1218 update protocol
* VFS-1258, transactions docs
* VFS-1258, transactions skeleton
* VFS-1218 remove several blocks
* VFS-1148 remove old gui
* VFS-1148 update path to gui files in gui config
* VFS-1148 restrucutere http files
* VFS-1148 restrucutere http files
* VFS-1218 fix some minor bugs
* VFS-1148 rework gui file structure
* VFS-1148 rework gui file structure
* VFS-1218 implement attributes and location notification
* VFS-1148 experiments with ember js
* VFS-1218 several bug fixes
* VFS-1148 change session callbacks used during login
* VFS-1218 update proto
* VFS-1218 update proto and its translations
* VFS-1148 add new callbac to session plugin
* Run client docker in privileged mode - FUSE needs it.
* VFS-1244 newest changes from GR
* VFS-1218 fix compile
* VFS-1218 implement truncate and unlink
* VFS-1148 update annotrations ref
* VFS-1148 update gui ref
* VFS-1148 logim and logut mechanisms
* Squashed 'bamboos/' changes from 9a972f6..b25f394
* VFS-1148 logim and logut mechanisms
* VFS-1148 add login page
* VFS-1218 fix lfm read/write test
* VFS-1218 fix write events
* Squashed 'helpers/' changes from 338ad98..db2a414
* VFS-1148 update gui ref, some compatibility changes
* VFS-1193 improve code style
* Merge commit 'fb419fd41733f4d7eda83b4a1bc738871e220258' into feature/VFS-1193-couchbase-for-production
* VFS-1148 update some behaviours
* VFS-1148 newest changes
* VFS-1148 update gui ref
* VFS-1148 move gui files out to gui repo
* VFS-1128, newest changes
* VFS-1217 Speed up test_rel compilation.
* VFS-1217 Speed up test_rel compilation.
* VFS-1128, updates to gui compiler plugin
* VFS-1128, updates to gui compiler plugin
* VFS-1148 add test files
* VFS-1218 fix test race
* VFS-1218 implement lfm write and read
* VFS-1148, add test pages for ember
* VFS-1218 fix new_file_location
* VFS-1148 merge VFS-1241
* VFS-1148 merge VFS-1241
* VFS-1148 merge VFS-1241
* VFS-1148 merge VFS-1241
* Squashed 'op_ccm/' changes from d5729c4..fe1eb68
* Merge commit '8024bb1ae581a7c1326a92bada3c14ab086657a4' into feature/VFS-1241-env_configurator-in-onedata-repo
* VFS-1241 add default values to logdir arg in up scripts
* Merge commit '76b8a7dbad6c2679bf6b7d40f33a8ba3e55077bd' into feature/VFS-1241-env_configurator-in-onedata-repo
* Squashed 'op_ccm/' changes from c6630fb..d5729c4
* VFS-1241 add default values to logdir arg in up scripts
* VFS-1241 add default values to logdir arg in up scripts
* VFS-1241 adjust panel.up to new standards
* VFS-1241 adjust panel.up to new standards
* VFS-1241 adjust panel.up to new standards
* Merge commit '3786248d8093fdcc111c5bebf0318a45d08ddbcd' into feature/VFS-1241-env_configurator-in-onedata-repo
* Merge commit '7b7e8c4901706ab8c0ce988cc918b6f07fe26941' into feature/VFS-1241-env_configurator-in-onedata-repo
* Squashed 'op_ccm/' changes from 5c785a6..e7794ec
* VFS-1241, change error message when timeout occurs
* VFS-1241, adjus wait for nagios time
* VFS-1241, make _up scripts crash when waiting for nagios fails
* VFS-1241, fix logs in gr and op all going to wrong dirs
* VFS-1241 remove unwanted print
* VFS-1241, add makefile to bamboos
* VFS-1241, add makefile to bamboos
* Merge commit '66afec4e5324fe8e0d083ed1aa832bddee235429' into feature/VFS-1193-couchbase-for-production
* fixup dns startup
* Merge commit '2eaae18ec2dba43fc0ee63b39bd85b952395d40b' into feature/VFS-1193-couchbase-for-production
* fixup dns startup
* Merge commit '1106d68a9a5e6a01dbc28902ed850fb6baa9393d' into feature/VFS-1193-couchbase-for-production
* fixup db_driver detections
* VFS-1218 add file_watcher model
* VFS-1218 add file_location and storage models
* Removed one ugly slash and some debugging code.
* Better coverage.escript.
* Test coverage escript.
* VFS-1218 add file_location and storage models
* Squashed 'op_ccm/' changes from 8899602..5c785a6
* Merge commit '10e78a2ffac02fbc07716519ecf7138ccd0dc65d' into feature/VFS-1191-dns-and-token-flow
* VFS-1191 modify one env.json to be more representative
* Merge commit '9885cf49b776fdd2f4b981a34a716da14fa28b02' into feature/VFS-1191-dns-and-token-flow
* Squashed 'op_ccm/' changes from 604de72..8899602
* VFS-1191 change dns waiting condition
* Squashed 'op_ccm/' changes from 2c0cd1f..604de72
* Merge commit '22b9a4796bb90310ac608f8f5cbef1dc52eed8d0' into feature/VFS-1191-dns-and-token-flow
* VFS-1191 adjust ct_run to new env json style
* Merge commit 'b532697af6ec33a8807d61868e05c4319c598f5e' into feature/VFS-1191-dns-and-token-flow
* Squashed 'op_ccm/' changes from 1496df2..2c0cd1f
* VFS-1191 update env
* VFS-1191 remove commented code
* Squashed 'op_ccm/' changes from beac3a1..1496df2
* Merge commit '7d96c7598d3678516e19730fcd22737e8904af6c' into feature/VFS-1191-dns-and-token-flow
* VFs-1191, update env
* VFs-1191, update env
* VFs-1191, update env
* VFS-1191, move changes from globalregsitry bamboos
* VFS-1191, move changes from globalregsitry bamboos
* Merge commit 'd58e4a47fbf9f623fe0c0f00535cc0eb79eae6aa' into feature/VFS-1193-couchbase-for-production
* VFS-1193 update bamboos
* Merge commit 'e8471928e3dc9d365059be3c5fcf3c6f08d530b3' into feature/VFS-1193-couchbase-for-production
* VFS-1193 revert docker/ct_run.py
* Merge commit '80721cdc62f2bfa3eb741fd37babcca4ddd59a9e' into feature/VFS-1193-couchbase-for-production
* VFS-1193 add missing files
* Merge commit '8cf48f0518612e0910670ab4db82403c3f521a02' into feature/VFS-1193-couchbase-for-production
* VFS-1193 remove unused files
* VFS-1193 fix compile
* VFS-1193 add couchbase driver support
* VFS-1193 add configurable persistence driver
* Squashed 'op_ccm/' changes from 07309d2..efb6bc2
* Merge commit 'd33586b5b5c95c155b8701c90dd049fd0d9dae7b' into feature/VFS-1172-move-provider-registration-logic
* Merge commit '7d41271791722710c481d61500f963ade1beed4d' into feature/VFS-1172-move-provider-registration-logic
* Merge commit 'c3fb8a4803b7e9edfbde961f29dacedaef1c509b' into feature/VFS-1172-move-provider-registration-logic
* VFS-1188, update spec
* Merge commit '576d0bf23c8f94a4a79dbe81feb6839ee4d4aa8b' into feature/VFS-1172-move-provider-registration-logic
* VFS-1172, merge with develop
* VFS-1172, merge with develop
* VFS-1172, merge with develop
* VFS-1174, Cover update
* Merge commit '029853e4fbeedc6d2c86793b05e89fa60981f18f' into feature/VFS-1172-move-provider-registration-logic
* VFS-1172, add new example env desc
* Merge commit '9f9fc740aeed3f3b49a59fba3e2e98472241a440' into feature/VFS-1172-move-provider-registration-logic
* VFS-1172, add new example env desc
* VFS-1188, update ctool ref
* Merge commit '9c437760fa3093ee43a5df1ab94bd60e9f1013bc' into feature/VFS-1172-move-provider-registration-logic
* VFS-1172, merge with bamboos
* VFS-1172, update stree test env json
* Merge commit '0833ea04a637d3fad88070637c7b6a5f56da5e59' into feature/VFS-1172-move-provider-registration-logic
* Squashed 'op-ccm/' changes from ea695dc..c2d9104
* Update after merge
* VFS-1164, stress tests minor update
* VFS-1174, cover minor update
* VFS-1172, update env up not starting properly without global setup
* VFS-1172, update appmock
* VFS-1145 Integrate SSL2 into oneprovider.
* Squashed 'op_ccm/' changes from a94a688..07309d2
* Merge commit 'dff7ec5a3c7b039c5968512b6b576f71ca5e175e' as 'appmock'
* VFS-1174, Cover analysis update
* VFS-1164, minor stress tests update
* VFS-1164, Stress test framework basis
* VFS-1148, newest changes
* use couchbase 4.0
* use couchbase 3.0
* use riak
* use couchbase 3.0
* use couchbase 4.0 and memcached client
* use couchbase 3.0
* use couchbase 4.0
* improve performance get_test
* improve performance get_test
* use couchbase 3.0 with async client; fix typo
* use couchbase 4.0 with async client
* use couchbase 3.0 with async client
* use couchbase 4.0
* fix couchbase rebalancing
* Merge commit '937b89221fcdf25117d9e14aa2c1cdc8fe565ab8' into feature/VFS-1176-couchbase-test
* VFS-1148, add n2o support
* VFS-1148, add gui compiler
* VFS-1148, add gui compiler
* VFS-1148, add gui compiler
* Merge commit '989afd20ad6bd235296701b41b5775b90a989172' as 'appmock'
* VFS-1148, newest changes
* Cover - disable during performance testing
* Pythonize cover file generation.
* Cover analysis minor update
* Cover analysis extended (ccm added to cover)
* Merge commit '653abccfe806b1797b2f47814b1b12f8ec741ebe' into feature/VFS-1138-node-package-distribution-selection
* cover minor update
* Cover analysis update
* Cover analysis added
* VFS-1149, add new endpoint for all messages count
* VFS-1149, add new endpoint for all messages count
* VFS-1129 Change default worker directory to op_worker
* Squashed 'op_ccm/' changes from 87efa84..f942d61
* Squashed 'op_ccm/' changes from 7eded3e..87efa84
* VFS-1115 Pass envs to docker run with make.py
* VFS-1053 fix oneprovider install/start/stop paths


### v43

* Test
* VFS-1193, Docs update
* VFS-1193, Update after merge
* VFS-1193, Minor updates of code style
* VFS-1193, Cache controller update
* VFS-1193, update_or_create operation added to datastore
* VFs-1244 fix bad test descriptor
* VFS-1244 fix typo
* VFS-1244 several fixes
* VFS-1244 update deps
* VFS-1193, Cache controller update
* VFS-1193, Cache controller update
* VFS-1193, Tests update
* VFS-1193, Cache controller update
* VFS-1193, Cache controller update
* VFS-1193, Create delete test in cache controller
* VFS-1193, Integration with new cache controller
* VFS-1242, Docs update
* VFS-1242, Docs update
* VFS-1244 Compile helpers less.
* Squashed 'op_ccm/' changes from d336f80..42ad2a7
* VFS-1244 update gitignore
* VFS-1244 remove unwanted file
* VFS-1217 Update clproto.
* VFS-1244 update clproto ref
* VFS-1244 fix extra semicolon warning
* VFS-1217 Update ctool and clproto.
* VFS-1223 update ctool ref
* Squashed 'op_ccm/' changes from fe1eb68..d336f80
* VFS-1244 fix macaroon in user_auth_test
* VFS-1244 add possibility for client to update auth
* VFS-1242, Tests update
* VFS-1244 revert unwanted changes
* VFS-1244 fix tests basing on tokens rahter than macaroons
* VFS-1244 add tests of gui_auth_manager
* VFS-1244 rename auth_manager, move macaroon dep to ctool
* VFS-1244 fix bad,atch
* VFS-1242, Docs update
* VFS-1244 update some specs
* VFS-1244 update some specs
* VFS-1244 update jiffy version
* VFS-1244 change token record to auth record, adjust all the code
* VFS-1244 change token record to auth record, adjust all the code
* VFS-1194 add missing -Wall compile flag
* VFS-1244 use macaroons in client handshake
* VFS-1244 change provider login endpoint, fix auth_logic bugs
* VFS-1244 add stub session for GUI, create auth_logic module
* VFS-1244 add stub session for GUI, create auth_logic module
* VFS-1242, Datastore test update
* VFS-1242, Datastore test update
* VFS-1242, Datastore test update
* VFS-1242, Datastore test update
* VFS-1193 fix dialyzer
* VFS-1193 fix dialyzer
* VFS-1242, Cache controller uses tasks
* VFS-1193 fix compile
* VFS-1193 fix dialyzer
* VFS-1194 fix compile
* VFS-1194 use static helpers
* Squashed 'helpers/' changes from db2a414..1a0ec85
* VFS-1242, Cache controller reorganization into one model
* VFS-1193 minor bug fixes
* VFS-1194 fix compile
* VFS-1194 fixup nif send
* VFS-1194 revert ctool
* VFS-1242, New task manager test
* VFS-1242, First task manager test
* VFS-1193 fix async save
* VFS-1194 fix cover
* VFS-1242, Task pool tests
* VFS-1194 fix cover
* Squashed 'helpers/' changes from 63e3040..db2a414
* VFS-1193 compile fix
* VFS-1193 rename RiakNodes
* Squashed 'bamboos/' changes from 19a59ab..8e6fcee
* VFS-1242, Task manager skeleton update
* Squashed 'helpers/' changes from 338ad98..db2a414
* VFS-1242, Task manager skeleton
* VFS-1227, Tests minor update
* VFS-1217 Use RoXeon/annotations.
* VFS-1217 Speed up test_rel compilation.
* Bump dependencies for R18 compatibility.
* VFS-1194 fix nif compile
* Squashed 'helpers/' changes from 20319fc..338ad98
* Squashed 'helpers/' changes from 63e3040..20319fc
* VFS-1194 fix error_code generation
* Squashed 'op_ccm/' changes from d5729c4..fe1eb68
* Squashed 'appmock/' changes from 7eafbb0..ee5c0aa
* Squashed 'bamboos/' changes from 7797628..9a972f6
* Squashed 'appmock/' changes from d14b8e7..7eafbb0
* Squashed 'op_ccm/' changes from c6630fb..d5729c4
* Squashed 'bamboos/' changes from 63771a0..7797628
* Squashed 'op_ccm/' changes from e7794ec..c6630fb
* Squashed 'appmock/' changes from c87360c..d14b8e7
* VFS-1241 update bamboos compilation method
* Squashed 'appmock/' changes from e651864..c87360c
* Squashed 'op_ccm/' changes from 5c785a6..e7794ec
* Squashed 'bamboos/' changes from ba784f3..63771a0
* VFS-1193: fix foreach_link
* Squashed 'bamboos/' changes from 57d7d02..19a59ab
* Squashed 'bamboos/' changes from b0174a2..57d7d02
* Squashed 'bamboos/' changes from ab8ff59..b0174a2
* VFS-1193 add some logs
* VFS-1194 move internal test functions
* VFS-1194 update docs
* VFS-1199, Update tests
* VFS-1199, Update tests
* Trash removed.
* VFS-1199, Update tests
* VFS-1199, Update tests
* VFS-1194 fix ct config
* VFS-1199, Update tests
* Support for coverage.escript.
* Squashed 'bamboos/' changes from b17e4d1..ba784f3
* VFS-1199, Update with fun added to local store
* VFS-1199, Update with fun added to local store
* VFS-1199, Setting scope update
* VFS-1199, Logging update
* VFS-1199, Logging update
* VFS-1194 fix merge
* VFS-1194 add user context to StorageHelperCTX
* VFS-1194 clang-format helpers_nif.cc
* VFS-1194 several code-style improvements
* Squashed 'helpers/' content from commit 63e3040
* VFS-1199, White spaces and names update
* VFS-1199, White spaces update
* VFS-1194 fix typo
* VFS-1199, Minor corrections after review
* VFS-1194 compile fix
* VFS-1194 fix flags detection
* VFS-1199, Minor corrections after review
* VFS-1194 fix typo
* VFS-1194 add docs and tests
* VFS-1199, Minor corrections after merge
* VFS-1199, Minor corrections after merge
* VFS-1199, Minor corrections after review
* VFS-1222 use new helpers callback
* VFS-1192, Add schedulers to config
* VFS-1192, Minor test update
* Squashed 'helpers/' changes from c6ad849..63e3040
* VFS-1193 update ctool
* VFS-1194 fix eunit tests
* VFS-1194 add set_fd
* Squashed 'op_ccm/' changes from 8899602..5c785a6
* Squashed 'appmock/' changes from 2acf352..e651864
* Squashed 'bamboos/' changes from d8cbc41..b17e4d1
* Squashed 'appmock/' changes from fc832b8..2acf352
* Squashed 'op_ccm/' changes from 604de72..8899602
* Squashed 'bamboos/' changes from b800f26..d8cbc41
* VFS-1191 adjust dns worker tests
* VFF-1191 fix wrong env descs
* Squashed 'op_ccm/' changes from 2c0cd1f..604de72
* Squashed 'appmock/' changes from d892861..fc832b8
* Squashed 'bamboos/' changes from 89211d2..b800f26
* Squashed 'appmock/' changes from 41dc8df..d892861
* Squashed 'op_ccm/' changes from 1496df2..2c0cd1f
* Squashed 'bamboos/' changes from a52ffd1..89211d2
* VFS-1191 adjust env jsons to new bamboos
* Squashed 'op_ccm/' changes from beac3a1..1496df2
* Squashed 'appmock/' changes from ecd7962..41dc8df
* Squashed 'bamboos/' changes from 81078ca..a52ffd1
* VFS-1147 fix rename
* VFS-1191 update ctool ref
* VFS-1191, fix getting GR domain
* Squashed 'bamboos/' changes from 1b97f45..81078ca
* VFS-1191, add support for diffenret types of providers in dns
* VFs-1191, add support for DNS requests when OP is in the same domain asGR
* Squashed 'bamboos/' changes from 97e5773..1b97f45
* Squashed 'bamboos/' changes from 671ba89..97e5773
* Squashed 'bamboos/' changes from 950ffd4..671ba89
* VFS-1191, move changes from globalregsitry bamboos
* VFS-1191, move changes from globalregsitry bamboos
* VFS-1199, cache dump to disk management - spec update
* VFS-1199, cache dump to disk management - minor update
* VFS-1191 revert unwanted changes
* VFS-1191, add mechanisms to ensuer corrent node hostname, adjust DNS worker to new domains concept
* VFS-1194 implement all void-returning functions
* VFS-1199, cache dump to disk management - minor update for dialyzer
* VFS-1199, cache dump to disk management - tests update
* VFS-1199, cache dump to disk management - eunit update
* VFS-1199, cache dump to disk management - local cache support
* VFS-1199, cache dump to disk management - code refactoring
* VFS-1194 implement all void-returning functions
* VFS-1191, remove idiotic env
* Ready to integrate.
* VFS-1199, cache dump to disk management - docs update
* Fixed test generators
* VFS-1199, cache dump to disk management - links support
* VFS-1147 update some docs
* VFS-1194 first working op
* VFS-1199, cache dump to disk management - forced dump after time
* VFS-1199, cache dump to disk management - minor update
* VFS-1199, cache dump to disk management - tests update
* VFS-1193 better connection handling
* VFS-1199, cache dump to disk management - tests update
* VFS-1193 better connection handling
* Squashed 'bamboos/' changes from 16d6384..ab8ff59
* VFS-1199, cache dump to disk management minor update
* VFS-1194 initial helpers support
* VFS-1199, cache dump to disk management update
* Squashed 'helpers/' content from commit c6ad849
* VFS-1193 restart mcd_cluster after connection failure
* VFS-1193 fix dialyzer
* VFS-1193 update deps versions
* VFS-1193 add docs
* VFS-1193 revert test
* Squashed 'bamboos/' changes from 875cd03..16d6384
* VFS-1199, forcing cache clearing once a period
* VFS-1199, Saving cache to disk status management
* VFS-1199, Add info about cache saving on disk status
* VFS-1193 update package builder deps
* Squashed 'bamboos/' changes from cb11045..875cd03
* Squashed 'bamboos/' changes from 950ffd4..cb11045
* VFS-1193 remove unused files
* VFS-1193 fix compile
* VFS-1193 add configurable persistence driver
* VFS-1193 update ctool
* VFS-1193 code cleanup
* fix user creation at login
* fix user creation at login
* make storage IDs boundaries configurable
* add missing spec and doc
* add missing spec and doc
* fix connetion_test_SUITE
* VFS-1192, update requests handling in worker host
* VFS-1192, update requests handling in worker host
* VFS-1172, remove unneded comment
* fix connection_test_SUITE
* fix types
* fix several appmock configs
* fix type error
* fix type error
* fix fsmodel list_children test
* add some docs
* fix fsmodel:rename test
* Squashed 'op_ccm/' changes from efb6bc2..beac3a1
* fix tests
* revert bamboos
* revert bamboos
* Squashed 'op_ccm/' changes from 07309d2..efb6bc2
* FS-1172, revert adding op-ccm dir
* add update_times test
* Squashed 'appmock/' changes from ef98287..ecd7962
* Squashed 'appmock/' changes from f2398b1..ef98287
* VFS-1172, merge with develop
* VFS-1172, merge with develop
* VFS-1172, merge with develop
* Squashed 'bamboos/' changes from 72b50f4..950ffd4
* Squashed 'bamboos/' changes from 72b50f4..950ffd4
* Squashed 'appmock/' changes from 4d32a5a..f2398b1
* Squashed 'appmock/' changes from e3e2f80..4d32a5a
* Squashed 'bamboos/' changes from 85d7862..72b50f4
* Squashed 'bamboos/' changes from 0c36605..72b50f4
* Squashed 'bamboos/' changes from 2db7926..0c36605
* Squashed 'bamboos/' changes from 85d7862..2db7926
* VFS-1174, Timeouts for asynch operations for slow bamboo
* VFS-1174, minor cover update
* VFS-1174, minor cover update
* VFS-1174, minor cover update
* Squashed 'appmock/' changes from 2cd34a3..e3e2f80
* Squashed 'bamboos/' changes from b9e5c93..85d7862
* Squashed 'bamboos/' changes from b9e5c93..85d7862
* Squashed 'bamboos/' changes from 725768f..b9e5c93
* Squashed 'appmock/' changes from 0adf1fb..2cd34a3
* Squashed 'bamboos/' changes from 725768f..b9e5c93
* Squashed 'appmock/' changes from 08723fd..0adf1fb
* Squashed 'bamboos/' changes from caac3db..725768f
* Squashed 'appmock/' changes from f49ac5a..08723fd
* Squashed 'bamboos/' changes from caac3db..725768f
* VFS-1172, merge with bamboos
* Squashed 'bamboos/' changes from e8250b3..caac3db
* VFS-1172, update stree test env json
* Added mock objects validation in dns_worker_test.erl
* Squashed 'appmock/' changes from e9e9fe2..f49ac5a
* Squashed 'op-ccm/' changes from ea695dc..c2d9104
* Squashed 'bamboos/' changes from e8250b3..caac3db
* VFS-1172, use botan on host machine rather than throw in so files
* use 10 connections per riak node
* VFS-1164, stress tests deps update
* VFS-1164, stress tests deps update
* Update after merge
* VFS-1164, stress tests deps update
* VFS-1174, cover deps update
* VFS-1174, cover deps update
* fix fsmodel rename test
* use 100 riak connection per node
* VFS-1164, stress tests minor update
* VFS-1174, cover minor update
* VFS-1174, cover minor update
* VFS-1172, use botan on host machine rather than throw in so files
* VFS-1172, use botan on host machine rather than throw in so files
* VFS-1172, use botan on host machine rather than throw in so files
* VFS-1172, use botan on host machine rather than throw in so files
* VFS-1172, use botan on host machine rather than throw in so files
* use several riak connections per node
* add update_times
* VFS-1180-unit tests of translator:translate_from_protobuf - corrected
* VFS-1172, move clib to priv dir
* VFS-1172, change c_lib dir to to priv
* VFS-1172, update env descipriotns
* VFS-1172. upadte ctool ref
* Squashed 'bamboos/' changes from 4413c96..e8250b3
* Squashed 'appmock/' changes from 6466104..e9e9fe2
* Squashed 'bamboos/' changes from 4413c96..e8250b3
* VFS-1172, update appmock
* VFS-1172, update specs
* VFS-1172, update specs
* VFS-1163, Caches controller and datastore tests minor update.
* remove unused file
* Tests for fslogic_context module.
* VFS-1172, disable hsts
* VFS-1172, move env to bamboo
* VFS-1172, slight optimisations in env configurator
* VFS-1180-unit tests of translator:translate_from_protobuf
* Tests for translate_to_protobuf() function from proto/translator module.
* VFS-1172, fix key creation with password
* enable openid login from GR
* VFS-1145 Integrate SSL2 into oneprovider.
* Squashed 'op_ccm/' changes from a94a688..07309d2
* Squashed 'appmock/' content from commit 6466104
* VFS-1145 Remove appmock dir.
* VFS-1145 Integrate SSL2 into oneprovider.
* VFS-1172, improve env.up to call env configurator
* Tests for multiple cases of handle() and parse_domain(). Code refactor.
* VFS-1174, Cover analysis deps update
* VFS-1174, Cover analysis deps update
* make rename usable
* fix type typo
* implement generic transactions in datastore ensure file_meta name uniqueness witihin its parent scope
* VFS-1174, Cover analysis update
* VFS-1174, Cover analysis update
* Tests for init() and healthcheck().
* VFS-1172, add possibility to start separate riak clusters
* VFS-1164, Deps update
* VFS-1164, minor stress tests update
* VFS-1178, minor local cache update
* VFS-1178, Minor test update
* VFS-1178, Cache controller uses non-transactional saves
* fixup tests
* compile fix
* implement chown, posix permissions, rename
* VFS-1164, Stress test deps update
* disable multi_ping_pong_test
* VFS-1164, Stress test with no clearing option added
* VFS-1164, First stress tests added
* change worker_host's ETS name
* fixup and test readdir
* change worker_host's ETS name
* add some docs
* move worker_host's state to ETS table
* fix db nodes setup in datastore perf tests
* VFS-1164, Stress test framework basis
* use couchbase 4.0
* use couchbase 3.0
* use riak
* update protocol
* use riak
* use riak
* use couchbase 3.0
* use couchbase 4.0 and memcached client
* use couchbase 4.0 and memcached client
* code cleanup and test improvements
* use couchbase 3.0
* use couchbase 3.0
* use couchbase 3.0
* use couchbase 4.0
* use couchbase 4.0
* improve performance get_test
* improve performance get_test
* VFS-1176 revert test cleanup
* use couchbase 3.0 with async client; fix typo
* use couchbase 4.0 with async client; fix typo
* use couchbase 4.0 with async client
* use couchbase 3.0 with async client
* use couchbase 4.0
* fix couchbase rebalancing
* always use new connection
* always use new connection
* ignore poolboy in cberl
* ignore poolboy in cberl
* increase connection pool size for couchbase driver
* increase connection pool size for couchbase driver
* increase connection pool size for couchbase driver
* improve call retry in couchbase driver
* Squashed 'bamboos/' changes from 4413c96..b6d4432
* VFS-1176 implement couchbase_datastore_driver
* VFS-1164, Tests bugs correction
* enable multi_ping_pong_test
* Test of packages
* VFS-1147 Fix dialyzer.
* VFS-1147 Fix dialyzer.
* VFs-1147 Fix ct tests.
* remove unused code
* VFS-1147 Integration with new protocol.
* VFS-1147 Implementation of first operations on directories.
* fix typo
* fix function spec
* rename transactional_cache option
* add disable mnesia transactions option
* VFS-1145 Integrate SSL2 into oneprovider.
* Squashed 'op_ccm/' changes from a94a688..c2f7859
* Squashed 'appmock/' content from commit 61077f7
* VFS-1145 Remove appmock dir.
* VFS-1142 Remove link_shared script.
* Cover analysis off for performance (deps update)
* Cover - disable during performance testing (deps update)
* Cover - disable during performance testing
* Cover - deps update
* Pythonize cover file generation.
* Cover - minor update after merge
* Cover analysis minor update
* Cover analysis extended (ccm added to cover)
* Squashed 'bamboos/' changes from e61fac1..4413c96
* Squashed 'bamboos/' changes from 0bc7c51..4413c96
* VFS-1138 op-ccm node_package distro selection.
* VFS-1138 op-ccm node_package distro selection.
* VFS-1138 node_package distro selection.
* VFS-1128 Adjust parameters values in performance tests.
* VFS-1128 Adjust parameters values in performance tests.
* cover minor update
* Cover analysis update
* Cover analysis update
* Cover analysis added
* VFS-1118, tests update
* VFS-1118, cache clearing during tests update
* VFS-1118, tests config
* VFS-1118, cache clearing during tests update
* VFS-1118, cache clearing during tests update
* VFS-1118, cache clearing during tests added
* VFS-1118, minor caches update
* VFS-1118, minor caches update
* VFS-1118, minor caches update
* VFS-1118, minor update
* VFS-1118, get/fetch_link datastore bug correction
* VFS-1118, minor cache controller update
* VFS-1118, ets limit update
* VFS-1118, minor posthook update
* VFS-1118, datastore healthcheck update
* VFS-1118, test
* Disable failing sequential_ping_pong test from performance plan.
* Revert test commits + add debug log and increase timeout.
* test
* test
* test
* fix mensia:save transaction
* make mnesia:list transactionless
* make mnesia:list transactionless improve datastore's generic_list_test
* VFS-1128 Removing pending messages in performance tests.
* VFS-1128 Fix dialyzer.
* VFS-1127 Adjust events integration tests to performance framework.
* VFS-1118, update after merge
* update datastore existing models to use new store_level macros
* update datastore internals to use new store_level macros
* add docs for internal functions
* fix typo
* implement 'list' callback on ETS datastore driver use only random keys in datastore tests
* VFS-1118, tests update
* VFS-1118, async datastore operations
* VFS-1129 Merge changes.
* VFS-1129 Merge changes.
* Squashed 'op_ccm/' changes from f942d61..a94a688
* VFS-1118, cache clearing update
* VFS-1118, test
* VFS-1118, test
* VFS-1118, test
* VFS-1129 Update clproto. Add deb build depends to pkg.vars.config.
* VFS-1118, minor corrections after merge
* VFS-1118, minor corrections after merge
* VFS-1118, after merge
* VFS-1129 Remove unnecessary distclean dependency in Makefile.
* VFS-1129 Add deb build dependencies
* VFS-1129 Update node_package
* VFS-1129 Update ctool.
* VFS-1129 Update ctool.
* VFS-1118, test update
* VFS-1118, deps update
* VFS-1118, docs update
* VFS-1118, docs update
* update env_desc for fsmodel tests
* add fsmodel performance tests
* fix mnesia expand
* fix some types
* VFS-1129 Update ctool
* VFS-1129 Update ctool
* VFS-1129 Change default worker directory to op_worker
* VFS-1118, minor tests update
* VFS-1129 Revert Moving request_dispatcher definition to ctool
* VFS-1129 Move request_dispatcher definition to ctool
* Merge commit 'e2aeea7a78077e61afdfb252c401e7f96b611d81' into feature/VFS-1129-packages-deb-and-rpm
* Merge commit 'e2aeea7a78077e61afdfb252c401e7f96b611d81' into feature/VFS-1129-packages-deb-and-rpm
* Squashed 'op_ccm/' changes from 87efa84..f942d61
* Squashed 'bamboos/' changes from 255475d..0bc7c51
* VFS-1129 Update ctool to develop
* improve fetch_link performance
* VFS-1118, minor tests update
* VFS-1118, local tests controller added
* fix some types
* improve mnesia writes
* fix typo
* compile fix
* compile fix
* compile fix
* implement mnesia links
* VFS-1118, minor tests update
* VFS-1118, cache cleaning method update
* VFS-1129 Remove old bamboos config
* VFS-1129 Remove old bamboos config
* VFS-1129 Remove old bamboos scripts
* VFS-1129 Remove old bamboos scripts
* VFS-1129 Rename oneprovider_node -> op_worker in base repo
* VFS-1129 Rename oneprovider_node -> op_worker in base repo
* VFS-1118, tests added
* VFS-1025, update ctool ref
* VFS-1025, update ctool ref
* VFS-1025, fix typo
* VFS-1025, remove unused module
* VFS-1118, global cache controller added
* VFS-1118, global cache controller added
* VFS-1025, please dialyzer
* VFS-1025, please dialyzer
* VFS-1118, cache clearing skeleton
* VFS-1118, cache clearing skeleton
* revert removed .keep file
* VFS-1025, update ctool ref
* VFS-1025, update ctool ref
* VFS-1025, minor changes
* Squashed 'op_ccm/' changes from 7eded3e..87efa84
* VFS-1025, update app cfg
* VFS-1025, update app cfg
* implement file_meta:rename
* BFS-1118, update after merge
* fix model_file_meta env desc
* fix type missmatch
* fix model_file_meta env desc
* fix model_file_meta env desc
* fix datastore link_walk test & improve mnesia performance
* improve exists performance
* improve datastore logic
* VFS-1025, remove redundant case clause
* VFS-1025, remove redundant case clause
* VFS-1025, remove redundant case clause
* VFS-1025, add start env log to file
* VFS-1025, add start env log to file
* VFS-1025, add start env log to file
* VFS-1025, add start env log to file
* VFS-1025, remove debugs
* VFS-1025, remove case clause error
* VFS-1115 Gitignore package/
* VFS-1115 Allow building RPM package.
* Squashed 'bamboos/' content from commit 255475d
* VFS-1115 Remove bamboos.
* VFS-1118, minor model tests update
* VFS-1025, change healthchecks in DNS and disaptcher
* VFS-1025, change healthchecks in DNS and disaptcher
* VFS-1118, performance model tests added
* VFS-1025, change healthchecks in DNS and disaptcher
* VFS-1025, update node manager restart strategy when connectio to CCM is lost
* VFS-1118, minor model tests update
* VFS-1118, model tests extended
* VFS-1025, update node manager restart strategy when connectio to CCM is lost
* VFS-1025, change make all target
* VFS-1025, change make all target
* VFS-1025, update a test
* VFS-1025, update a test
* VFS-1025, change perf tests in requests_routing_test_SUITE
* VFS-1025, change perf tests in requests_routing_test_SUITE
* VFS-1025, revert unwanted changes
* VFS-1025, fix some dialyzer errors
* VFS-1025, fix some dialyzer errors
* VFS-1025, add kernel ports envs
* VFS-1025, add kernel ports envs
* VFS-1025, update ctool
* VFS-1025, update ctool
* VFS-1025, update ctool ref
* VFS-1025, update ctool ref
* VFS-1025, fix typo
* VFS-1025, fix typo
* VFS-1025, fix typo
* Squashed 'op_ccm/' changes from d1d9b51..7eded3e
* Squashed 'bamboos/' changes from 6a371b1..e61fac1
* VFS-1025, update nagios handler
* VFS-1025, update nagios handler
* VFS-1025, change nagios handler
* VFS-1025, pdate ctool
* VFS-1025, pdate ctool
* VFS-1025, pdate ctool
* VFS-1025, pdate ctool
* VFS-1025, merge lb with develop
* VFS-1025, merge lb with develop
* VFS-1025, merge lb with develop
* VFS-1100 Remove unnecessary dependency to 'distclean'
* VFS-1100 Remove unnecessary dependency to 'distclean'
* VFS-1100 Doc update
* VFS-1100 Rename old dispatcher port
* fix some typo
* VFS_1097, performance tests cases description added
* VFS_1097, performance tests params and results description added
* VFS-1100 Add test_rel target to Makefile
* VFS-1100 package.py script
* add gen_path/1
* VFS_1097, datastore performance tests config update
* VFS_1097, datastore performance tests bug correction
* VFS-1100 cleaning release files in 'make clean'
* VFS-1010 cleaning release files in 'make clean'
* VFS_1097, datastore performance tests config update
* VFS_1097, datastore performance tests results update
* VFS_1097, datastore performance tests config update
* VFS_1097, datastore performance tests config update
* VFS_1097, datastore tests update
* VFS_1097, datastore tests config update
* VFS_1097, datastore tests config update
* VFS_1097, get and update tests update
* VFS_1097, deps update
* VFS-1097, updates after merge
* VFS-1097, datastore tests update
* VFS-1053 Change node_manager to crash if datastore is not loaded, define mnesia timeout value
* Merge commit '5471c449bf400533a81e05e495b9bb70cfd66efd' into feature/VFS-1053-generation-of-distribution-files
* Merge commit '5471c449bf400533a81e05e495b9bb70cfd66efd' into feature/VFS-1053-generation-of-distribution-files
* Squashed 'bamboos/' changes from ff2356a..6a371b1
* VFS-1053 doc update
* VFS-1053 Remove appmock waiting
* VFS-1053 Temporarily disable rest cert auth test
* VFS-1053 Revert back appmock healthcheck
* VFS-1053 Revert debug changes
* VFS-1053 Remove synchronizing mnesia operations through node manager
* VFS-1097, testing
* VFS-1053 Increase timeout on waiting for mnesia
* VFS-1097, model performance tests config update
* VFS-1097, datastore tests update
* VFS-1053 Increase timeout on session delete
* VFS-1072 Fix file block translation.
* VFS-1053 Change order of selecting master nodes during initialization v2
* VFS-1053 Change order of selecting master nodes during initialization v2
* VFS-1053 Change order of selecting master nodes during initialization
* VFS-1053 Change order of selecting master nodes during initialization
* VFS-1053 increase appmock waiting time
* VFS-1097, save test update
* VFS-1053 temporarily remove appmock healthcheck
* VFS-1097, deps update
* VFS-1097, datastore performance tests
* VFS-1053 additional logs
* VFS-1053 change wait_for_tables mnesia call
* VFS-1053 Typo fix
* VFS-1053 Typo fix
* fix type typo
* test list_uuids/delete in file_meta
* VFS-1053 Waiting for appmock
* VFS-1053 Selecting explicit node for mnesia to join, instead of finding it randomly
* VFS-1053 Selecting explicit node for mnesia to join, instead of finding it randomly
* VFS-1053 Waiting for mnesia init, another attempt
* VFS-1053 Waiting for mnesia init, another attempt
* VFS-1097, data store tests skeleton
* VFS-1053 fix possible deadlock
* test create/get/get_scope in file_meta
* test create/get/get_scope in file_meta
* Squashed 'bamboos/' changes from ff2356a..471694c
* initial file_meta impl
* VFS-1053 comment out temporary sleep
* VFS-1053 synchronize mnesia init operations through node_manager
* VFS-1053 rchange timeout value
* Squashed 'bamboos/' changes from ff2356a..f0aa65f
* VFS-1053 riak waiting
* VFS-1053 riak waiting
* VFS-1108 Wait for succesful 'riak-admin test' on riak start.
* VFS-1053 Increase sleep time during appmock wait
* VFS-1053 Add waiting for appmock v2
* VFS-1053 Add waiting for appmock
* VFS-1053 Merge changes v2
* VFS-1053 Merge changes v2
* VFS-1053 Merge changes
* VFS-1053 Merge changes
* VFS-1053 Update ctool
* VFS-1053 Tmp fix for race condition
* VFS-1053 Waiting for tables
* VFS-1053 Revert changes and add sleep
* VFS-1053 Minor debug changes
* VFS-1053 Minor debug changes
* VFS-1053 Change table_info(where_to_commit) to table_info(where_to_read) in mnesia init
* VFS-1053 Add some more mnesia:wait_for_tables calls
* VFS-1053 Compilation fix
* VFS-1049 improve code quality
* VFS-1053 Calling mnesia:wait_for_tables once again
* VFS-1053 calling mnesia:wait_for_tables
* VFS-1056 Remove sleep after starting appmock.
* Squashed 'appmock/' content from commit 804f60e
* Squashed 'appmock/' content from commit 804f60e
* VFS-1056 Remove appmock subtree.
* Squashed 'bamboos/' changes from 3c80b89..ff2356a
* VFS-1056 Fix env.up invocation in env_up.py .
* VFS-1053 sleep before joining mnesia nodes
* VFS-1053 sleep before joining mnesia nodes
* VFS-1056 Do not sched privileges in make.py if we're already root.
* VFS-1053 Mnesia init race fix attempt
* VFS-1053 Debug logs
* improve code quality
* VFS-1053 Debug logs
* VFS-1053 Debug logs
* VFS-1053 Debug logs
* VFS-1096 Update ctool reference.
* VFS-1056 Use requests library for HTTP connections.
* Provide default arguments for env.up, fix inter-module deps.
* VFS-1056 Modify createJson.js to accept container names with multiple '_'.
* VFS-1016 Move env_up.py logic to a module in environment.
* VFS-1056 Fix formatting missteps.
* VFS-1056 Wait for nagios when starting appmock.
* Fix logs creation.
* VFS-1098 Fix compilation error.
* VFS-1098 Integration with protocol based on NIFs.
* VFS-1098 Update ctool ref.
* VFS-1098 Update ctool.
* VFS-1053 Make dir for eunit' surefire test results
* VFS-1053 Make dir for eunit' surefire test results
* VFS-1053 generating eunit surefire report
* VFS-1053 generating eunit surefire report
* VFS-1053 ctool update
* Squashed 'bamboos/' changes from 9fd9b49..3c80b89
* VFS-1095 Wait for Riak to start by polling its REST endpoint.
* VFS-1050 Update ctool ref.
* VFS-1050 Update ctool ref.
* VFS-1050 Update ctool ref.
* VFS-1053 add eunit example test to ccm
* VFS-1053 add eunit example test to ccm
* VFS-1053 fix dialyzer errors
* VFS-1053 fix dialyzer errors
* VFS-1053 add dialyzer + remove unnecessary ping
* VFS-1053 add dialyzer + remove unnecessary ping
* VFS-1053 remove unused env
* VFS-1053 provider_up script which starts op_ccm and op_worker
* VFS-1053 provider_up script which starts op_ccm and op_worker
* VFS-1050 Adjustments for performance tests.
* VFS-1053 silence crash logs, increase event_manager_test timeout so stream_sup would have time to kill streams when deadlock happens
* VFS-1050 Adjustments for performance tests.
* VFS-1051 ct test fix
* VFS-1051 merge changes
* VFS-1051 merge changes
* VFS-1051 update ctool
* VFS-1049 compile fix
* VFS-1049 add check_permissions annotation
* VFS-1049 add initial fslogic file structure
* fix compile
* fix compile
* fix compile
* add driver level setting to links API
* Squashed 'bamboos/' changes from 737d10d..9fd9b49
* VFS-1050 Update ctool reference.
* VFS-1073 Wait for SkyDock and SkyDNS in dns.up
* revert test cleanup
* Cherry-pick improved make.py from feature/demo2.
* VFS-1050 Update ctool reference.
* VFS-1050 Returning values with units from performance tests.
* VFS-1051 connection_test extension
* VFS-1053 remove debug code, better mocking in user_auth_test
* VFS-1053 move certs to etc
* VFS-1053 revert appmock changes
* VFS-1053 gen_dev compilation in op_ccm
* VFS-1053 gen_dev compilation in op_ccm
* Squashed 'op_ccm/' changes from 73d4e7e..d1d9b51
* VFS-1051 change worker startup order
* VFS-1053 doc update v2
* VFS-1053 doc update
* VFS-1051 increase appmock timeout
* VFS-1051 get rid of wrapper module
* VFS-1053 disable building dependent modules
* VFS-1053 do not overlay vars in dependent modules
* VFS-1053 new node_package v2
* VFS-1053 new node_package
* VFS-1053 add PKG_VARS_CONFIG env
* VFS-1053 add PKG_VARS_CONFIG env v2
* VFS-1053 add PKG_VARS_CONFIG env
* fix dialyzer errors
* VFS-1004: fix compile
* fixup some dialyzer errors
* VFS-1004: add some link docs
* VFS-1004: add some link docs
* VFS-1004: implement links for models
* dockerfiles removed (moved to dedicated repo)
* Squashed 'op_ccm/' content from commit 73d4e7e
* Squashed 'op_ccm/' content from commit 73d4e7e
* VFS-1053 update op_ccm
* VFS-1053 rename oneprovider_ccm
* Squashed 'bamboos/' content from commit 89a1e45
* VFS-1053 remove bamboos
* VFS-1053 rename oneprovider_ccm -> op_ccm
* VFS-1053 change op ccm name
* Squashed 'op-ccm/' content from commit d04acd3
* VFS-1053 remove oneprovider_ccm
* VFS-1053 link to make.py
* Squashed 'bamboos/' content from commit 346fd7c
* VFS-1053 new ccm sources
* Initial Commit
* VFS-1053 change crash dump location
* VFS-1053 change wait process
* VFS-1053 minor refactoring
* VFS-1053 doc update
* VFS-1053 change install group to onedata
* VFS-1053 adjust gen_dev to config files in etc dir
* change runner user to root
* add runner user
* VFS-1053 change runner user to root
* VFS-1053 run from sbin
* VFS-1053 change runer_data_dir to platform_data_dir
* VFS-1053 add runer_data_dir to config
* some minor changes
* VFS-1052 Fix cluster state notifier and add storage paths to sys.config.
* VFS-1053 parametrize relative paths
* VFS-1053 move app config to ./etc and c_lib to ./lib
* VFS-1053 change etc dir
* VFS-1053 add OVERLAY_VARS to release generation
* VFS-1023, ct_run update
* VFS-1023, ct_run update
* ct_run update
* not needed files removed
* VFS-1023, deps update
* VFS-1023, deps update
* VFS-1052 Add onepanel dockerfile and setup script.
* VFS-1053 create separate users for ccm and worker
* VFS-1053 dummy data
* VFS-1053 dummy etc
* VFS-1053 add ccm as separate project
* VFS-1023, minor corrections after merge
* VFS-1023, minor corrections after merge
* Squashed 'oneprovider_ccm/' content from commit 2256c52
* VFS-1053 remove ccm
* VFS-1023, Minor changes after merge
* VFS-1053 Remove some responsibilities of CCM
* VFS-1023, Minor changes after merge
* VFS-1023, minor corrections after merge
* VFS-1023, minor corrections after merge
* VFS-1023, deps update
* VFS-1023, minor corrections after merge
* VFS-1023, scripts update
* VFS-1023, scripts update
* VFS-1023, scripts update
* VFS-1000, deps update
* VFS-1053 tmp change release target name
* VFS-1053 wrong string format fix
* VFS-1053 node_package first attempt
* fixup some type errors
* fixup some type errors
* includes typo
* update ctool
* VFS-1051 final changes
* VFS-1051 remove undef function call
* VFS-1051 remove unwanted changes
* VFS-1051 enable perf tests in rest_test_SUITE
* VFS-1051 rest cert auth with gr mock
* VFS-1019, test timeout update
* docs typo
* fix riak driver typo
* Squashed 'bamboos/' changes from 3429ac0..737d10d
* fix riak driver typo
* fix riak driver typo
* fix riak driver typo
* fix riak driver typo
* fix riak driver typo
* fix riak driver typo
* fix riak driver typo
* fix riak driver typo
* fix riak driver typo
* fix riak driver typo
* fix riak:create
* implement datastore: 'delete with predicates' and list
* implement datastore: 'delete with predicates' and list
* implement datastore: 'delete with predicates' and list
* implement datastore: 'delete with predicates' and list
* implement datastore: 'delete with predicates' and list
* VFS-1019 lower number of messages in perf tests
* implement datastore: 'delete with predicates' and list
* VFS-1019 add perf test
* VFS-1019 ctool update
* VFS-1019 ctool update
* implement datastore: 'delete with predicates' and list
* VFS-1019 Appmock rebar update
* VFS-1019 Disable random failing test (proto_version_test)
* VFS-1019 Tcp tests fixed
* VFS-1008 Move timeout values to top of their files.
* Merge commit '0de40ab1c085f50d87cfdca4417e6665a4ccbc82' into feature/VFS-1019-op-sessions-user-2
* Merge commit '0de40ab1c085f50d87cfdca4417e6665a4ccbc82' into feature/VFS-1019-op-sessions-user-2
* Merge commit '0de40ab1c085f50d87cfdca4417e6665a4ccbc82' into feature/VFS-1019-op-sessions-user-2
* Merge commit '0de40ab1c085f50d87cfdca4417e6665a4ccbc82' into feature/VFS-1019-op-sessions-user-2
* Merge commit '0de40ab1c085f50d87cfdca4417e6665a4ccbc82' into feature/VFS-1019-op-sessions-user-2
* VFS-1052 Add init scripts for Debian.
* VFS-1019 appmock merge changes
* VFS-1019 Removing pending messages in connection test SUITE
* VFS-1019 Appmock warnings
* VFS-1019 Recommended changes
* VFS-1051 add rest cert test
* VFS-1019 Typo fix
* VFS-1019 Define timeout value
* VFS-1019 Token auth in rest
* VFS-1019 Revert some unwanted changes
* VFS-1019 Remove unused envs
* VFS-1051 merge changes
* VFS-1019 Do not remove python client after test
* VFS-1051 Identity model preparation for rest auth
* VFS-1019 Merge changes
* VFS-1051 accidental sending of CertInfo in oneproxy http_session fixed, rest_test_SUITE added
* VFS-997 Add annotations header.
* VFS-997 Turn off performance tests for session manager test suite.
* VFS-997 Refactor session manager ct tests.
* dialyzer fix
* VFS-997 Refactor sequencer manager ct tests.
* VFS-997 Refactor event manager ct tests.
* VFS-997 Apply recommended changes.
* VFS-997 Apply recommended changes.
* recommended changes
* VFS-997 Change folder structure.
* VFS-997 Change folder structure.
* VFS-1019 increase nagios timeout
* VFS-1019 increase nagios timeout
* VFS-1019 increase nagios timeout
* VFS-1019 saving appmock description as .erl.cfg files
* VFS-1019 protocol handler bind addr changed to 0.0.0.0 during tests
* VFS-1019 fix dialyzer problems
* VFS-1019 fix some of dialyzer problems
* VFS-1019 fix some of dialyzer problems
* VFS-1019 fix some of dialyzer problems
* VFS-1019 nagios_test fix
* VFS-1019 merge changes
* VFS-1019 ctool update
* implement datastore: 'delete with predicates' and list
* VFS-1019 tests fix
* VFS-1019 tests fix
* VFS-1019 tests fix
* VFS-1019 user_auth_test_SUITE:token_authentication
* VFS-1019 Adjust env_up to appmock
* VFS-1019 Adjust env_up to appmock
* VFS-1019 Adjust env_up to appmock
* Squashed 'appmock/' content from commit 1e84ca2
* implement datastore: 'delete with predicates' and list
* implement datastore: 'delete with predicates' and list
* VFS-1000, test update
* VFS-1000, test update
* VFS-1000, test update
* VFS-1000, test update
* VFS-1000, test update
* VFS-1000, test update
* VFS-1000, provider.up update
* VFS-1000, provider.up update
* VFS-1000, provider.up update
* VFS-1000, provider.up update
* VFS-1000, tests utils update
* VFS-1000, tests utils update
* VFS-1000, tests update
* VFS-1000, tests update
* VFS-1000, tests update
* VFS-1037 Cleanup test files
* VFS-1023, tests update
* VFS-1037 python_client_test
* VFS-1037 Python ssl client
* Merge commit '6db2358c462b132287d3f6487eb2ab29f007ec5e' into feature/VFS-1023-all-plans-use-dockers
* VFS-1019 Integration test with gr_appmock, first attempt
* VFS-1000, minor update
* VFS-1000, deps update
* VFS-1008 Integrate riak into oneprovider.
* VFS-1019 Get rid of random 'message_stream_reset' request in connection test suite
* VFS-1000, deps update
* VFS-1019 Communicator crash when connection_pool is empty fixed v2
* VFS-1019 Communicator crash when connection_pool is empty fixed
* VFS-1019 Change perf config.
* VFS-1019 ct tests adjustments to merged code
* VFS-1019 delete old code
* VFS-1019 merge ctool and clproto
* VFS-1000, perf test update
* VFS-1019 user & identity models first integration
* VFS-1019 user & identity models first integration
* implement datastore: 'delete with predicates' and list
* implement datastore: 'delete with predicates' and list
* implement datastore: 'delete with predicates' and list
* VFS-1000 Fix dialyzer.
* VFS-1000, Perf tests update
* implement datastore: 'delete with predicates' and list
* implement datastore: 'delete with predicates' and list
* implement datastore: 'delete with predicates' and list
* implement datastore: 'delete with predicates' and list
* VFS-1037 bandwidth_test with tcp
* VFS-1037 bandwidth_test
* VFS-1037 multi_ping_pong test using gen_tcp
* VFS-1037 New test using gen_tcp + oneproxy uses one ioservice
* VFS-1035 Allow make.py to execute arbitrary commands.
* VFS-1000, deps update
* VFS-1035 Allow appmock TCP to user Erlang packet header.
* VFS-1000, test minor update
* VFS-1000, minor update
* VFS-1000, minor update
* multi_connection_test change connection_num
* multi_connection_test
* VFS-1035 Move appmock_client.py to the appmock repository.
* VFS-1035 Move docker binary into the base image.
* VFS-1000, minor update
* VFS-1000, merge
* VFS-1000, deps update
* VFS-1000, minor update
* VFS-997 Add event stream periodic emission ct test.
* VFS-997 Add event stream crash ct test.
* VFS-997 Event manager ct test.
* VFS-997 Add event utils and unit test.
* VFS-997 Fix ct tests.
* communicator improvements
* VFS-997 Default event stream values.
* communicator first implementation
* VFS-1000, ccm minor update
* VFS-1000, ccm minor update
* VFS-997 More ct tests for session and sequencer managers.
* remove debug logs
* clproto update
* ctool update
* add seq ping test to perf tests
* VFS-997 More session tests.
* change perf test config
* wrap oneproxy io_services with strands
* sequential_ping_pong_test
* VFS-997 Add logs.
* VFS-1035 Add dnspython and dig to base image.
* VFS-1041, test
* VFS-1041, test
* VFS-1047 Update ct_run.py with new path to docker lib.
* VFS-1041, test
* VFS-1047 Update ctool and references to its functions.
* VFS-1041, test
* VFS-1041, test
* VFS-1041, test
* VFS-1041, test
* VFS-1041, test
* Squashed 'bamboos/' changes from fcd8af8..6db2358
* VFS-1041, hotfix for verify_rest_history
* VFS-1047 Save logs of oneprovider running in dockers.
* VFS-1047 Break up environment python scripts into backend and frontend.
* VFS-1041, hotfix for verify_rest_history
* VFS-1041, hotfix for verify_rest_history
* VFS-997 Supervision tree for session.
* VFS-997 Apply recommended changes.
* update ctool & clproto
* add @private to some functions
* doc update
* remove unknown record matching
* fix CertificateInfo race condition
* VFS-1041, some changes to please dialyzer
* VFS-1041, some changes to please dialyzer
* VFS-1041, some changes to please dialyzer
* VFS-1041, some changes to please dialyzer
* VFS-997 Add missing protocol.
* VFS-1041, fix some specs
* remove unused server_port
* VFS-997 Apply recommended changes.
* VFS-1041, add send data endpoint to remote control
* remove debug logs
* ping_pong performance test
* VFS-1047 Wait for positive health check in provider_up.py .
* VFS-1041, add healthcheck for tcp server mock
* VFS-1041, add tcp server verify functionality
* VFS-1041, add gen server for handling tcp server mocks
* VFS-1041, add gen server for handling tcp server mocks
* checking endpoints during healthcheck of http_worker and dns_worker
* VFS-1041, reorganize the project, rename some records and macros
* VFS-1041, reorganize the project, rename some records and macros
* merge changes
* VFS-1035 Restructure directories and add pip to base docker.
* VFS-1000, fix some dialyzer errors
* VFS-1000, fix some dialyzer errors
* VFS-1000, fix some dialyzer errors
* client communicate async test 2
* VFS-1000, fix some dialyzer errors
* VFS-1000, fix skeleton of file mangers so as not to generate errors
* client communicate async test
* VFS-1000, update ctool ref
* VFS-1041, add gen_server to handle remote control requests
* VFS-1046, update bamboos
* Merge commit '0824c41721f3ee66de0363e5fef876f8c792eca8' into feature/VFS-1000-env-starting-scripts
* VFS-1000 Add whitespace in set_up_dns in common.py
* VFS-1000 Decode subprocess pipe output into UTF-8 string.
* VFS-1000 Add set_up_dns common function for python scripts.
* Merge commit '805c43214fc3a3a67fcb6b7b1d40969a0f9bc578' into feature/VFS-1000-env-starting-scripts
* VFS-1000, add some comments
* VFS-1000, add some comments
* VFS-1000, use common.py where possible
* VFS-1000, use common.py where possible
* VFS-1000, fix docstrings
* VFS-1000, fix docstrings
* VFS-1000, several fixes
* VFS-1000, several fixes
* VFS-1000, several fixes
* VFS-1000, several fixes
* VFS-1046, several fixes
* VFS-1046, several fixes
* apply protocol changes
* VFS-1000, some updates in bamboos
* VFS-1000, some updates in bamboos
* client_communicate_test
* client_communicator bugfixes + ct test
* merge similar inits
* move ssl start/stop to init/end per testcase
* typo fix
* doc update
* dialyzer fix v1
* VFS-997 Change sequencer manager connection logic.
* VFS_1008 Fix providing custom node number for Riak docker.
* VFS-997 Refactoring.
* VFS-997 Fix dialyzer.
* VFs-997 Extend event manager tests.
* VFS-1008 Create a riak docker.
* VFS-997 Event manager is working.
* multi_message test
* VFS-1000, minor fix
* VFS-1000, minor fix
* VFS-1000, improve appmock implementation
* VFS-997 WIP.
* Add doxygen package to the builder.
* protocol_handler refactoring v2
* protocol_handler refactoring
* moving around modules
* move session definitions to separate header
* change location of message_id header
* extract certificate_info to separate header
* client_communicator lib
* VFS-1000, add logical and storage file manager's API design
* session management
* VFS-1000, deps update
* VFS-1000, add crach to throw if app desc module cannot be loaded
* VFS-1000, minor update
* certificate handshake ct test
* VFS-1000, minor update
* VFS-1000, minor update
* VFS-1000, annotations moved to ctool
* VFS-1000, minor update
* VFS-997 Add event dispatcher.
* VFS-1006 Update Docker images with new project dependencies.
* VFS-1000, perf tests added
* dialyzer fixes
* oneproxy CertificateInfo message
* new handshake
* VFS-1000, some fixes to please dialyzer
* VFS-1000, some fixes to please dialyzer
* VFS-1000, some fixes to please dialyzer
* VFS-1000, some fixes to please dialyzer
* VFS-1000, remove unneeded ets object deleting
* VFS-1000, update example app desc file
* VFS-1000, update example app desc file
* VFS-1000, apply all recommended fixes
* VFS-1000, apply all recommended fixes
* VFS-1000, add a lot of specs
* VFS-1000, delete iml file from repo
* VFS-1000, add sequence support for response mocking
* minor improvements
* VFS-1000, annotations for perf tests
* VFS-1000, update bamboos
* VFS-1000, update bamboos
* VFS-1000, fix typo
* VFS-1000, fix typo
* VFS-1000, add clioent starting to env.py
* VFS-1000, add clioent starting to env.py
* extend certificate message
* VFS-1000, add common.py with utils
* VFS-1000, add common.py with utils
* VFS-1000, add common.py with utils
* VFS-1000, add common.py with utils
* VFS-997 Fix dialyzer.
* VFS-1000, first script for client
* VFS-1000, first script for client
* VFS-1000, nodes synchronization update
* VFS-997 Fix some dialyzer errors.
* VFS-1000, script for starting sholw onedata environment
* VFS-1000, script for starting sholw onedata environment
* VFS-1000, script for starting sholw onedata environment
* VFS-1000, script for starting sholw onedata environment
* VFS-1000, script for starting sholw onedata environment
* VFS-1000, script for starting sholw onedata environment
* VFS-1000, script for starting sholw onedata environment
* VFS-1000, script for starting sholw onedata environment
* VFS-1000, script for starting sholw onedata environment
* VFS-1000, script for starting sholw onedata environment
* VFS-1000, add start script for appmock
* VFS-1000, add start script for appmock
* VFS-1000, add scripts to start globalregistry
* VFS-1000, add scripts to start globalregistry
* test fix
* VFS-1000, add scripts to start globalregistry
* VFS-1000, add scripts to start globalregistry
* client_auth adjustment to new handshake
* VFS-1000, minor changes in make.py
* VFS-1000, minor changes in make.py
* proto update
* protocol adjustments
* VFS-1011, remove all debugs
* VFS-1011, remove all debugs
* VFS-1011, remove all debugs
* VFS-1011, remove all debugs
* VFS-1011, remove all debugs
* VFS-1011, remove all debugs
* VFS-1011, turn off some test suites
* VFS-1011, turn off some test suites
* VFS-1011, turn off some test suites
* VFS-1011, turn off some test suites
* VFS-1011, turn off some test suites
* VFS-1011, turn off some test suites
* VFS-997 Fix some dialyzer errors.
* VFS-1011, some fixes for dialyzer
* VFS-1011, some fixes
* VFS-1011, some fixes
* VFS-1011, some fixes
* VFS-997 Fix ct tests.
* init macro
* test fix
* VFS-997 Add sequencer worker.
* mock router
* sending server_messages
* VFS-1011, comment out not working code in datastore workerk
* VFS-1011, comment out not working code in datastore workerk
* VFS-997 Sequencer ct test.
* translation improvements
* serialization improvements
* VFS-1010 Make test master node discoverable through DNS.
* VFS-1010 Update docker templates.
* client_auth + basic integration with protobuf
* VFS-997 Add sequencer dispatcher ct test.
* VFS-997 Sequencer logic.
* VFS-1011, enable cleanup in nagios test
* VFS-1011, fix env for nagios test
* VFS-1011, add errors support to healthcheck
* VFS-1011, add errors support to healthcheck
* Dummy unit test.
* WIP.
* VFS-997 WIP.
* VFS-997 Add sequencer.
* VFS-997 Change cluster init.
* VFS-1010 Automatically .dialyzer.plt as needed.
* VFS-1010 Update ctool.
* atomic update ct test - timeout
* atomic update ct test
* atomic update ct test
* atomic update ct test
* atomic update ct test
* update docs for ct
* fix typo
* fix typo
* add healthcheck
* add datastore ct test
* add datastore ct test
* minor changes
* add basic authentication mechanisms
* VFS-1010 Modify dialyzer-related make targets.
* VFS-997 Add remote Event Manager.
* VFS-1011, update specs
* add echo protocol with ct test
* VFS-1011, change update srtate flow
* VFS-1011, add type for healthcheck reposnse
* VFS-1011, add type for healthcheck reposnse
* simplify datastore callback context
* update docs
* fixup mnesia init
* move datastore init to node_manager
* Merge branch 'develop' of ssh://git.plgrid.pl:7999/vfs/oneprovider into feature/VFS-996-model-layer-first-implementation
* fix typo
* fix typo
* reorganize some files
* VFS-997 Minor change.
* VFS-1000 Stop fetching deps on each make.py
* VFS-1000 Stop fetching deps on each make.py
* Merge commit 'a08ab98ad59134d72baab33cc80976a0f14c85b5' into feature/VFS-1010-acceptance-test-framework
* VFS-1010 Don't shed root privileges on non-Linux platforms.
* VFS-1010 Don't shed root privileges on non-Linux platforms.
* VFS-1000 Add newer protobuf to builder image.
* VFS-1000 Add newer protobuf to builder image.
* VFS-1011, update ctool ver
* VFS-997 Add event translator.
* VFS-1011, typo
* VFS-1011, eliminate some dialyzer errors
* remove conditional notifing on state change + add new ct test
* ct tests fix
* ct tests fix
* VFS-1000, specs update
* VFS-1010 Run scripts in make.py as a running user.
* VFS-1010 Run scripts in make.py as a running user.
* VFS-997 Fix ct tests.
* VFS-1011, docs update
* VFS-997 Minor update.
* VFS-1011, spec updates
* move gen_dev_args to bamboos
* VFS-1000 Update ctool
* VFS-997 Using gpb library.
* VFS-1011, dist test for nagios
* ctool update
* reorganize gen_dev helper modules
* reorganize gen_dev helper modules
* Merge commit 'c34bb9d937883c03028e770146f60d809c22db0d' into feature/VFS-1007-global-registry-gendev
* Merge commit 'c34bb9d937883c03028e770146f60d809c22db0d' into feature/VFS-1007-global-registry-gendev
* Merge commit 'c34bb9d937883c03028e770146f60d809c22db0d' into feature/VFS-1007-global-registry-gendev
* VFS-1000 Document python scripts.
* VFS-1000 Document python scripts.
* VFS-1010 Remove test starting cruft.
* improve compilation
* improve compilation
* VFS-1011, docs update
* VFS-1011, docs update
* VFS-1011, update gitignore
* Squashed 'bamboos/' changes from 8310c12..c34bb9d
* VFS-1011, ct test for nagios
* VFS-1000, deps update
* ctool update
* ctool update
* wip
* add some specs
* fix compile error
* add some docs
* capitalize macro
* Merge branch 'feature/VFS-998-refactoring-disp-wh-ccm-nm' into feature/VFS-1007-global-registry-gendev
* VFS-1011, nagios handler
* VFS-1011, nagios handler
* VFS-1010 Cleanup scripts associated with running distributed tests.
* VFS-1010 Cleanup scripts associated with running distributed tests.
* VFS-1010 Cleanup scripts associated with running distributed tests.
* beautify
* naming changes
* preety -> pretty
* heart beat rename
* refactoring of confusing lambdas
* heart_beat -> heartbeat
* apply recommended changes
* remove long lines
* add some docs
* change save level names
* remove camel case atom names
* add log_bad_request macro
* add some docs
* revert unwanted changes
* remove unused includes and rename some variables
* VFS-1011, eunit for nagios_handler
* refactoring of request_dispatcher:check_state
* apply recommended changes
* remove wrong clause
* rename MaxSecondsBetweenRestarts to RestartTimeWindow
* apply recommended changes
* rest protocol repair
* remove invalid case
* refactoring
* ctool update
* typo
* adjust gen_dev to relative paths
* adjust gen_dev to relative paths
* move gen_dev to bamboos + gen_dev modularization
* move gen_dev to bamboos + gen_dev modularization
* preety print app name
* Squashed 'bamboos/' changes from 95045ce..8310c12
* VFS-1000 Fix docker.py Python 3 compatibility and interactive mode.
* generalize gen_dev input to any type of erlang app
* VFS-1000 Fix ct.py to work with new docker scripts.
* Squashed 'bamboos/' content from commit 95045ce
* VFS-1000 Cleanup and rewrite scripts on top of new docker.py module.
* VFS-1000 Rewrite provider_up and make on console-based docker.py
* VFS-1000 Set permissions for ssh keys in make.py.
* riak driver full impl
* ct fixes
* VFS-1000, deps update
* VFS-1000, deps update
* VFS-1000 Output info about pulling images to stderr.
* riak driver initial impl
* VFS-1000, deps updata
* VFS-1000, cleaning after test
* VFS-1000 Add cleanup.py docker script.
* VFS-1000 Update make.py
* VFS-1000 Update worker image and make.py
* VFS-1000 Add make.py.
* VFS-1000 Run CTests through a handy python script.
* VFS-1000, minor update
* VFS-1000, tests start files update
* notifier fix
* VFS-1000 Return started docker ids from provider_up.
* VFS-1000, minor update
* VFS-1000, tests start files update
* VFS-1000, tests update
* VFS-1000, tests update
* gen_dev_args change "cookie" to "setcookie"
* makefile reorganization
* VFS-1000, tests update
* VFS-1000, tests update
* VFS-1000 Implement initial version of client_up script.
* VFS-1000 Finish globalregistry_up implementation.
* datastore hooks
* VFS-1000, work with tests
* get rid of absolute paths
* ctool update
* add bamboos as rebar dependency
* VFS-1000 Don't set up a custom cookie for oneprovider.
* VFS-1000 Add Bigcouch docker definition.
* VFS-1000 Reformat a small chunk of cluster_up code.
* VFS-1000, start dockers from ct
* VFS-1000 Stop using tempfiles for docker commands in make.py.
* VFS-1000 Change provider_up to allow starting from dockers.
* add state to worker_host plugin
* VFS-1000 Update CMake PPA in builder docker.
* VFS-1000, work with tests
* VFS-1000 Add a first version of globalregistry_up.
* enable cluster state notifications, so ct tests don't need to active wait for cluster init
* VFS-1000 Remove redundant master docker.
* VFS-1000 Move the dockers on Ubuntu 14.10 base.
* initial impl of distributed and local cache
* VFS-1000 Rewrite make.py to work without docker-py.
* VFS-1000, Tests update
* VFS-1000 Output a cookie from cluster_up.
* VFS-1000, Tests update
* VFS-1000 Print configuration on cluster up.
* refactoring and doc update
* VFS-1000 Add a makefile, fetch required images automatically.
* move defs from headers to modules
* naming changes
* minor improvements
* env name typo fixed
* typo fix
* update plugin state with lambda
* new supervision tree
* VFS-1000 Configure the cluster through gen_dev_args.json
* dialyzer warnings fix
* VFS-1000, Test scripts update
* VFS-1000, Test scripts update
* VFS-1000, Test scripts update
* dialyzer errors fix
* new gen_dev
* VFS-1000, update rebar config
* VFS-1000, update readme
* VFS-1000, update readme
* VFS-1000, update readme
* VFS-1000, update readme
* VFS-1000, update readme
* VFS-1000, change example
* VFS-1000, add starting script
* VFS-1000 Implement a make.py script and greatly improve cluster_up.
* VFS-1000, add starting script
* improve in dinstributed erlang app configuration + ct test
* change error logs in gen_dev
* add code path
* arg getting fix
* fix
* change created beam location to target dir
* refactor worker_host header
* oneproxy state doc
* cdmi/rest state doc
* enable args_file_path customization
* VFS-1000 Add first versions of Docker files.
* VFS-1000, massive specs update
* test_fix
* supervisor starting typo fix
* add input_dir/target_dir configuration
* enable init_cluster triggering when all nodes have appeared
* rest/ccdmi function headers
* doc update + gen_dev improvement
* remove unwanted comment
* apply recommended changes
* sys.config comments
* VFS-1000, added some comments
* doc update
* remove includes of deleted headers
* remove meck from release
* remove supervision macros
* VFS-1000, mock verification and client
* move some code from headers to modules
* merge changes
* remove request_dispatcher.hrl
* remove node_manager.hrl
* change name of dispatcher_listener
* remove ccm header
* additional documentation
* worker_host api changes
* VFS-1000, add verification callback for mocks
* fix osx compatibility
* remove dead code
* enable target_dir configuration v2
* enable target_dir configuration
* enable cookie configuration
* VFS-1000, working request mappings
* worker_map ets introduced
* request_dispatcher pre-refactoring cleanup
* add functionality of dynamic file loading
* VFS-1000, more advanced skeleton
* VFS-1000, introduce cowboy server
* introduce cowboy server
* initial rebar skeleton
* initialize rebar project
* Initial Commit
* remove 'state_loaded' field from cm_state
* unused functions cleanup
* cluster manager refactoring first attempt
* node_manager refactoring
* add annotations dependency
* test fix
* include guards naming fix
* code standards adjustments
* code standards adjustments
* remove example test + change dist app config
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
* gen_dev cleanup function extracted
* remove unused config
* configurator documentation
* capitlize macros in headers v2
* capitlize macros in headers
* hrl adjustment to new code standards
* remove rpm scripts, add gui
* add dns
* rest + cdmi mock handlers
* oneproxy + client and provider websockets servers
* additional test
* add stub worker
* add dispatcher and worker_host
* gen_dev fix + add logger plugin for ctool
* enable ccm gen_server starting
* cluster_manager refactoring
* remove dead code
* temporarily delete wss
* basic ct test
* changes in gen_dev scripts
* cleanup + node_manager draft + config script rewritten in erlang
* VFS-818 Minor changes.
* VFS-818 Copy authorization data for  onepanel.
* VFS-818 Fix provider name.
* VFS-818 Fix provider name.
* VFS-818 Add alias support.
* VFS-818 Change scripts description.
* VFS-818 Fix user privileges.
* VFS-818 Final deployment commit.
* VFS-818 Move files.
* VFS-818 Rename files.
* VFS-797 Change from /bin/bash to /bin/sh.
* VFS-797 Extend ONEPANEL_MULTICAST_ADDRESS and move it to the beginning of a script.
* VFS-797 Refactoring.
* VFS-797 Fix registration setup.
* VFS-797 Fix install.cfg.
* VFS-797 Switch to onepanel_admin.
* VFS-797 Registering in Global Registry.
* VFS-797 Registering in Global Registry.
* VFS-797 Registering in Global Registry.
* VFS-797 Registering in Global Registry.
* VFS-797 Fix VeilCluster start.
* VFS-797 Fix VeilCluster start.
* VFS-797 Fix VeilCluster start.
* VFS-797 Fix VeilCluster start.
* VFS-797 Fix VeilCluster start.
* VFS-797 Fix VeilCluster start.
* VFS-797 Fix VeilCluster start.
* VFS-797 Open VeilCluster DB.
* VFS-797 Adding storage paths on cluster nodes.
* VFS-797 Fix VeilCluster start.
* VFS-797 Fix VeilCluster start.
* VFS-797 Fix VeilCluster start.
* VFS-797 Fix VeilCluster start.
* VFS-797 Fix VeilCluster start.
* VFS-797 Starting VeilCluster.
* VFS-797 Refactoring.
* VFS-797 Installing VeilCluster RPM.
* VFS-797 Refactoring.
* VFS-797 Try to fix ssh -tt errors.
* VFS-797 Try to fix ssh -tt errors.
* VFS-797 Try to fix ssh -tt errors.
* VFS-797 Try to fix ssh -tt errors.
* VFS-797 Try to fix ssh -tt errors.
* VFS-797 Try to fix ssh -tt errors.
* VFS-797 Fix Global Registry start.
* VFS-797 Fix Global Registry start.
* VFS-797 Remove "sleep" command from nohup.
* VFS-797 Starting Global Registry node.
* VFS-797 Fix Global Registry DB extension.
* VFS-797 Extending Global Registry DB.
* VFS-797 Change location of BigCouch DB files.
* VFS-797 Add additional "sleep" to nohup.
* VFS-797 Fix starting Global Registry DB nodes.
* VFS-797 Fix starting Global Registry DB nodes.
* VFS-797 Fix starting Global Registry DB nodes.
* VFS-797 Starting Global Registry DB nodes.
* VFS-797 Add global_registry_setup script.
* enable --no-check-certificate
* revert all debug
* more debug logs
* more debug logs
* debug logs
* revert debug logs
* debug logs
* change env name
* remove debug logs
* more debug logs
* debug log
* change env name so it fits bamboo env setting
* VFS-695 Remove logs.
* VFS-695 Change db bind address.
* allow to disable user creation
* VFS-695 Add logs.
* VFS-699 Add license and headers to files.
* VFS-699 Move scripts from EUSAS server to git repository.
* Initial Commit


### v2.5.0

* increase timeout
* VFS-965, several fixes
* VFS-965, several fixes
* VFS-965, several fixes
* VFS-965, several fixes
* revert changes
* change default perms
* correct mode for remote files
* VFS-965, minor fix
* remove unnecessary spawn
* VFS-965, fix some css
* VFS-965, minor fix
* VFS-965, fix some css
* compilation fix
* truncate file during exceeding write
* VFS-965, update submodules
* VFS-965, add func to create groups for spaces
* VFS-937 Fix oneproxy restart.
* VFS-965, issue sync of spaces every time
* VFS-965, revert accidental changes
* VFS-965, full functionality of spaces page
* Fix test compilation.
* Update client ref.
* Perform operations asynchronously in ws_handler.
* VFS-965, default space info
* VFS-965, several funcionalities of page spaces
* VFS-965, visial aspects of spaces page
* remove ct:print
* add user context to event (just after receiving), so we can reroute message to other provider
* VFS-937 Update onepanel ref.
* VFS-965, first code for spaces page
* add missing bock maps
* VFS-937 Using provider name in GUI.
* VFS-937 Update onepanel ref.
* test fix v2
* test fix
* VFS-937 Change push channel connection approach.
* recommended changes
* friday bugfixes
* VFS-936 do not save file_watcher for remote gui requests
* VFS-936 fix some types
* VFS-936 fix user creation
* VFS-936 improve fuse session clearing
* VFS-936 update ctool
* VFS-936 improve dbsync reliability
* VFS-965, update submodules
* VFS-965, update submodules
* VFS-965, update submodules
* VFS-965, add support for issuing remote sync
* VFS-954, code refactoring
* VFS-954, code refactoring
* VFS-954, add sorting for users and spaces lists
* VFS-965, add no groups and no spaces info
* VFS-965, one can now leace a space he has no rights to view
* VFS-965, one can now leace a space he has no rights to view
* VFS-965, one can now leace a space he has no rights to view
* VFS-965, one can now leace a space he has no rights to view
* VFS-965, privileges edition on groups page
* remove debug logs
* size cache fix
* storing old file size
* VFS-965, add functionalities on groups page
* debug
* debug
* VFS-959 Apply recommended changes.
* VFS-965, add functionalities on groups page
* add guards
* VFS-936 fix unset size detection
* debug logs
* VFS-936 fix for file_attr_watcher
* VFS-936 add missign views
* VFS-959 Apply recommended changes.
* VFS-959 Fix GUI upload.
* VFS-936 fix attrunsubscribe rerouting
* VFS-936 update helpers
* VFS-936 code cleanup
* VFS-952 revert all ticket related changes
* VFS-952 revert all ticket related changes
* VFS-952 revert all ticket related changes
* helpers update
* VFS-952 revert rebar.config
* VFS-936 api for syncing file from remote provider
* wrong binary conversion fix
* remove call to fslogic internal function
* remove call to fslogic internal function
* new unit tests
* VFS-959 Fixes.
* VFS-936 do not update push file size on file_meta change
* VFS-959 Update refs.
* VFS-959 Apply recommended changes.
* change confusing name
* VFS-959 Change description.
* VFS-959 Not sending notifications for a fuse that modifies a file.
* remove debug logs
* undef fix
* compilation fix
* debug
* convert binary to list
* debug logs
* todowrong cache name fix
* VFS-959 Update refs.
* fix descriptor creation race
* VFS-936 update onepanel
* saving descriptor bugfix
* debug logs
* debug
* debug
* removing file from storage + debug logs
* todo v2
* remove debug logs
* todo
* VFS-965, groups user privileges prototype
* VFS-965, first prototype
* remove debug logs
* badarg fix
* compilation fix
* synchronize creation of file_location
* VFS-959 Extend communication protocol.
* funtion clause fix
* debug
* logs
* set fuseID to CLUSTER_FUSE_ID during creation of file_location
* log fix
* fix rece condition in get_file_location
* VFS-959 Update refs.
* debug logs
* debug log
* VFS-954, adjust to new file blocks API
* VFS-954, adjust to new file blocks API
* change logging
* VFS-954, adjust to new file blocks API
* Wait a little longer for rtransfer module init.
* VFS-954, adjust to new file blocks API
* VFS-954, update clproto version
* error handling
* logs + remove reading file size from storage
* Unskip RTransfer CT.
* change getfileblockmap request to include file name instead of uuid
* clproto update
* missing translator
* get_file_block_map by FullFileName
* badrecord fix
* list comprehension fix
* debug log v2
* debug log
* access to remote filemaps
* clproto update
* clproto update
* clproto update
* change loglevel to debug (for some logs)
* unicode print error fix v2
* unicode print error fix
* remove debug logs
* merge changes
* VFS-919 Cluster_rengine fix.
* VFS-919 Cluster_rengine fix.
* tmp change
* VFS-939 Manually init rtransfer_tab in CT.
* VFS-939 Fix rtransfer_tab init in eunit tests.
* VFS-954, fix several bugs
* VFS-954, fix several bugs
* VFS-919 Cluster_rengine fix.
* logs
* debug
* VFS-939 Minor improvements.
* VFS-954, fix several bugs
* temporary chnage
* VFS-954, fix several bugs
* VFS-919 Cluster_rengine log.
* file deleting tryfix
* VFS-939 Disable verbosity in rtransfer CT.
* some change in synchronization
* update ctool ref
* VFS-954 simple cache remembering provider names
* log
* VFS-954 simple cache remembering provider names
* uncomment do_stuff
* comment out some code
* VFS-953, minor update
* VFS-954, auto refres of ddist status
* VFS-954, auto refres of ddist status
* removing file from storage
* VFS-953 Fix gitignore
* VFS-954, auto refres of ddist status
* VFS-937 Update refs.
* VFS-939 Remove inline test, fix eunits.
* Fix float send_after delay value.
* setting fslogic context
* VFS-939 Implement rtransfer.
* logs
* missing coma
* debug
* VFS-919 Fixed logging in cluster_manager. Changed worker counts in caches tests and cluster_rengine_test.
* debug logs
* VFS-940 Use set for terms.
* fix truncate
* VFS-919 Added init_cluster_once cast, removed dbsync form cluster_code.
* debug
* Revert "Merge branch 'feature/VFS-952-file-descriptors-for-fuse-cache' into feature/VFS-938-oneclient-integration-with-blocks-2"
* remove ct logs
* VFS-954, available blocks: integrate GUI with fslogic
* add permission for attrunsubscribe on group files
* unwanted exports fix
* VFS-954, add specs
* VFS-937 Update onepanel ref.
* remove ct logs
* chanage var name
* VFS-954, implementation of data distribution panel yet without logic
* temporary hack
* VFS-940 Apply recommended changes.
* VFS-940 Apply recommended changes.
* VFS-940 Add new lines at the end of files.
* VFS-953, doc update
* VFS-940 Apply recommended changes.
* VFS-953, tests update and minor bugs correction
* VFS-919 Added todos for dbsync.
* debug logs
* debug logs
* change call to cast
* remove log
* remove logs
* VFS-953, id type update
* change call to cast
* do not update maps when non necessary
* increase timeouts and enable asynchronous calls
* add file_block_midified request handlers
* export newly created functions
* VFS-952 fix attr watchers clearing
* VFS-952 support for AttrUnsubscribe message
* VFS-953, minor code refactoring
* VFS-937 Fix oneprovider node supervisor.
* VFS-919 Changed number of parameters in update_workers function.
* VFS-952 update protocol
* VFS-952 update protocol
* VFS-952 add attr watcher descriptors
* VFS-593, GR push channel messages handling
* move some operations to one process in order to prevent db conflicts
* VFS-919 Fixed modules lifecycle tests, fixed bugs in other modules.
* VFS-940 Change list of terms to set of terms.
* VFS-940 Apply recommended changes.
* remove debug logs
* test + cache fix
* block cache test
* VFS-940 Spelling fixes.
* VFS-937 Minor change.
* VFS-940 Fix Makefile.
* debug logs
* VFS-940 Refactoring.
* debug logs
* VFS-940 Add RTransfer map unit tests.
* ensure uuid is list
* function clause fix
* debug
* VFS-954, prototype
* debug
* VFS-940 Fix unit tests warnings.
* ets:delete_object chnaged to ets:delete
* badarg fix
* remove unwanted code
* cache improvements
* bad naming improvements
* typo fix
* integration with pushing size changes to clients
* remove unused var
* push message file_id fix
* ensure file location is created
* size tryfix
* debug log
* remove file_decriptor creation
* debug log
* log format fix
* debug logs
* badmatch fix
* badarg fix
* getting size
* VFS-936 use erlang:send_after instead of timer:apply_after
* getting size from available blocks map, instead of storage
* get rid of logical_files_manager:write(Path, Buffer) function
* better caching events info
* VFS-940 Fix memory allocation bug.
* creating file location for remote files + some debug logs
* VFS-937 Minor change.
* VFS-919 Rearanged module lifecycle tests.
* VFS-936 some minor improvements
* VFS-940 Fix notification bug.
* Timeouts update update
* VFS-936 update docs
* VFS-936 aggregate file size events
* VFS-940 WIP.
* remove fixing storage owner in getfileattr
* badmatch fix
* bugfixes
* revert unwanted change
* fix db conflicts
* VFS-936 fillup new required field
* debug logs
* badarith fix
* debug logs
* badrecord fix
* erong return value fix
* file size change fix
* Minor update
* filesize support and cache + some refactoring
* Logging update
* VFS-940 WIP.
* debug logs v2
* debug logs
* VFS-940 Add change_counter method.
* extract ensure_file_location_exists function
* VFS-940 WIP.
* VFS-940 WIP.
* return value bugfix
* Dispatcher update
* VFS-940 WIP.
* VFS-940 WIP.
* VFS-936 add fslogic_events
* debug logs
* wrong truncate value v2
* wrong truncate value
* write/2 tryfix v3
* bugfix
* debug logs + conflict fix
* VFS-940 Add rt_exception.
* write/2 conceptual problem temporary fix v2
* write/2 conceptual problem temporary fix
* revert log change
* range conversion fix
* remove invalid log
* VFS-919 Rearanged module lifecycle tests.
* add missing argument
* convert path to FullFilePath in events
* VFS-919 Fixed typo in ccm_and_dispatcher_test.
* debug logs
* debug logs
* creating file location for empty remote files moved to get_file_location
* VFS-940 Subscribing for container state events.
* chmod on non existing file error fix
* badmatch fix
* wrong type fix
* VFS-919 Capitalized constant name in test_utils.
* VFS-919 Fixed typo in merge.
* VFS-940 Change list of pids to list of terms.
* Tests init update
* VFS-940 Add rt_map specialization.
* informing client about available blocks
* VFS-940 Add provider id to rt_block + clang-format.
* VFS-940 Add rt_container abstraction.
* VFS-939 Basic draft of rtransfer worker.
* VFS-919 Fixed typo in merge.
* VFS-940 Add list of pids to block.
* delete debug logs
* marking updated blocks in event handler
* VFS-919 Gateway merge, fixed tests.
* send file name in events
* register event handlers
* code reformat
* add get_file_size api
* VFS-937 Saving provider ID in CCM state.
* doc update
* VFS-937 Change connection supervision.
* wrong read_event bytes field fix
* typo
* handling {uuid, Uuid} instead of FullFileName
* log improvement
* remove debug logs
* dbg logs
* setting user ctx
* write event wrong value fix
* typo fix
* setting user context in created process
* debug logs
* surplus argument fix
* dbg logs
* missing coma
* dbsync_hook_registering_delay env
* change apply to spawn
* remote_location rename to available_blocks
* logical_files_manager temporary function calls
* VFS-937 Global Registry channel as worker host.
* range struct intersection implementation
* ranges_struct docs
* mark_other_provider_changes doc
* rename remote_location -> available_blocks
* remove debug logs
* VFS-937 Add Global Registry channel.
* VFS-889 revert build_helpers.sh
* VFS-889 fix docs for dbsync module
* VFS-889 add docs for dbsync module
* VFS-932 Update logical files manager.
* VFS-932 Update refs.
* VFS-919 Fixed test.
* VFS-919 Improved spec and formatting.
* VFS-919 Fixed unit tests.
* binary/list mismatch
* logs
* lambda error fix
* debug
* debug
* debug
* enable wite notifications
* more logs...
* more logs...
* too many lists error fixed
* typo fix
* VFS-889 revert makefile
* VFS-889 fix some ct tests
* debug logs
* doc format error fix
* wrong range format repair
* VFS-914, remove outdated tests
* VFS-914, specs updates
* remote_location creation fix
* VFS-914, remove outdates tests
* VFS-914, update gen_test.args
* VFS-914, update gen_test.args
* VFS-914, dns listeners on all nodes
* VFS-914, dns listeners on all nodes
* VFS-914, dns listeners on all nodes
* VFS-919 Fixed unit test checking new functinality.
* VFS-919 Fixed spec in gateway module.
* VFS-919 Removed unused logging, added specs to functions, refactored complex functions.
* register for db_sync changes
* VFS-914, fix submodules refs
* VFS-896 Change reading and writing to use logical_files_manager.
* VFS-914, update ctool ref
* debug logs
* debug logs
* maps wrong usage repair
* debug logs
* VFS-889 fix eunit
* VFS-889 docs for dbsync_hooks
* badrecord fix
* VFS-889 docs for dbsync_records
* typo
* some bugfixes
* VFS-889 docs for dbsync_state
* typo fix
* creating file location for remote files
* VFS-889 fix some tests, update some docs
* synchronization improvements
* VFS-889 fix some tests
* error fixes
* turn on logging
* sync fix
* silence some error logs
* storage maniupation on remote file fix
* compilation fix
* getattr fix
* write events integration
* VFS-889 fix tests
* VFS-919 Fixed tests, added new unit tests, improved ccm lifecycle.
* gateway integration
* VFS-931 Update refs.
* VFS-931 Export function.
* VFS-889 update docs
* tests fix
* stuff
* VFS-889 update docs
* VFS-889 update docs
* Makefile spaces replaced by tabs
* enable remote_location db sync
* VFS-931 Updating client files block map.
* VFS-889 add register_hook API
* VFS-889 remove dbsync_lib
* VFS-889 update rtproto release notes
* VFS-889 update rtproto release notes
* VFS-889 some code cleanup
* getting info about available blocks on other provider's storages
* remote location dao proxy + some bugfixes
* VFS-919 Fixed tests, added new unit tests, improved ccm lifecycle.
* VFS-896 Add 'Helper function' section to gateway_test.
* VFS-932 Add block info to events.
* VFS-896 Don't start SSL in test.
* VFS-896 s/start_link/spawn_link
* VFS-896 Remove a debug log from gateway_protocol_handler.
* VFS-920 Apply recommended changes.
* VFS-896 Init nodes with different gateway ports for high_load_test
* VFS-896 Add TODOs for Logical Files Manager usage in gw.
* VFS-896 Add CT test.
* remove debug logs
* eunit fix
* badarg fix
* new range storing data structure
* VFS-920 Clang-format.
* VFS-920 Apply recommended changes.
* VFS-896 Restore gateway_protocol.
* VFS-889 hack file_meta
* VFS-896 Add more documentation to oneproxy.
* VFS-889 add hooks support
* VFS-919 Rearanged test.
* VFS-919 Fixed spelling error.
* VFS-919 Fix in ccm test.
* VFS-919 Improved formatting in cluster manager.
* VFS-919 Reverts changes in rtransfer.
* VFS-919 Module monitoring lifecycle.
* VFS-919 Changed type of logging.
* VFS-889 update dao
* VFS-889 code cleanup
* VFS-896 Get rid of inline test function.
* VFS-896 Polish and document the code.
* VFS-920 Change some descriptions.
* VFS-920 Add more eunit tests.
* VFS-896 Enhance gateway communicator to a workable state.
* VFS-896 Allow oneproxy to function in proxy mode.
* VFS-889 first working dbsync prototype based on BigCouch long poll Rest API
* VFS-889 first working dbsync prototype based on DAO events
* VFS-920 Add eunit tests.
* VFS-920 Add docs.
* bugfixes
* VFS-920 RTHeap as a gen_server.
* prepare fslogic for multiple remote locations of file
* change db for remote_location records
* VFS-920 VIP.
* eunit fix
* move ceil to ctool, synchronize file before write
* revert unwanted changes + doc improvements
* some more bugfixes
* protocol update
* spec updates
* VFS-914, remove uncompatible tests
* VFS-914, remove uncompatible tests
* some bugfixes
* VFS-914, move common DNS modules to ctool
* VFS-914, move common DNS modules to ctool
* updating remote_location during read/write/truncate
* protocol update
* VFS-920 C++/Erlang interface.
* VFS-914, implement logic in dns query handler
* VFS-914, implement logic in dns query handler
* protocol update
* protocol update
* VFS-919 Modules monitoring.
* typo fix
* add block synchronization mechanism to fslogic
* VFS-914, fix gen_dev.args
* VFS-914, fix gen_dev.args
* update protocol
* VFS-914, generic DNS server
* VFS-896 Take proxy type as oneproxy argument
* add remote_location to each regular file
* remote location module - new data structure and basic api for sync purposes
* VFS-914, generic DNS server
* VFS-896 Allow binding to separate NICs.
* VFS-896 Improve request handling - timeouts.
* VFS-896 Redesign communication layer of the Gateway module.
* So many supervisors.
* VFS-896 Polish presentation info.
* VFS-896 Implement receiving message back from remote location.
* VFS-896 Implement message sending through gateways.
* init dbsync
* init changes for dbsync


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

* release notes update
* deps update
* VFS-897 Use effective user privileges on page_space.
* VFS-897 Using effective user privileges.
* typo fix + dependency to rrdtool
* bigcouchdb update
* db ssl linking fix
* VFS-910 Change layout.
* deb package build
* VFS-910 Fix layout.
* VFS-910 Minor layout change.
* VFS-910 Change download oneclient page.
* VFS-894, specs updates
* VFS-894, fix typo causing error undef
* onepanel update
* onepanel update
* VFS-888 Fix compilation errors.
* VFS-899 Add breadcrumbs.
* VFS-888 Add documentation and comments to some functions.
* VFS-894, massive specs update
* VFS-894, massive specs update
* VFS-894, change how perms editor behaves on clicks
* VFS-907 Update onepanel ref.
* VFS-894, support for groups in acls
* VFS-894, move whole perms editor to another module
* VFS-888 Fix CT tests.
* VFS-894, move some code to pfm_perms
* apply recommended changes (in doc and returned values)
* naming and doc improvements
* disable directory read permission checking
* change log from error to debug
* handling acl errors + some bugfixes
* additional group synchronization
* add new view
* fix wrong returned size
* check read permission on dirs
* group name to id mapping
* group sync
* VFS-888 Fix unit tests.
* VFS-888 Fix compilation after merge.
* binary conversion bugfix
* clear test db
* VFS-886, recommended fixes
* VFS-886, recommended fixes
* VFS-886, merge with develop
* group permission checking
* ctool update
* VFS-895 Add RPM package install files progress indicator.
* VFS-895 Update refs.
* update onepanel
* update ctool
* VFS-886, fix arrow icon
* VFS-886, update alerts
* VFS-886, update alerts
* VFS-886, fix identifier in acls
* VFS-886, fix identifier in acls
* VFS-886, fix onepanel ref
* VFS-886, fix bug when setting ACL to main spaces dir
* VFS-886, revert changes
* VFS-886, fix some css
* VFS-886, revert changes
* VFS-886, update ctool ref
* VFS-886, add dropdown for choosing Identifiers
* VFS-888 Document added functions.
* VFS-886, newest changes
* VFS-888 Map files to blocks.
* VFS-886, move some css to static file
* reset acl when changing file mode
* trim_spaces moved to ctool, doc update
* Include krb and ltdl dylibs in release
* delete write permission check during set_acl cdmi request
* VFS-886, move some css to static file
* delete read permission check during get_acl request
* increase state number due to fslogic cache registering
* VFS-886, add posix and acl tabs for perms
* change test cache location from fslogic to control_panel
* VFS-886, add radio buttons
* VFS-881 Update onepanel ref.
* VFS-881 Minor GUI web pages refactoring.
* VFS-881 Add groups management.
* VFS-881 Add space privileges management page.
* VFS-880 special characters in cdmi, + some minor fixes
* VFS-881 Using privileges to enable/disable user actions.
* revert deleted view
* VFS-886, newest changes
* merge similar views
* VFS-867 Fix init scripts return codes.
* VFS-867 Rename database in init script.
* VFS-867 Add some comments.
* test fixes after merge
* VFS-867 Fix database release addition.
* VFS-867 Update onepanel ref.
* VFS-867 Update onepanel ref.
* VFS-867 Set RPM package version.
* VFS-886, merge with VFS-858
* VFS-864, fix user gruid not being set in developer mode
* VFS-886, newest changes
* VFS-886, modify chmod panel to include ACLs
* vcn_utils -> utils
* doc update
* move eunit test from veil_modules to oneprovider_modules
* reformat code
* veil_document -> db_document rename
* proto update
* merge errors fixes
* onepanel and bigcouchdb merge update
* delete accidetially merged veilprotocol
* delete accidetially merged veilclient&veilhelpers
* merge
* VFS-694 Remove additional letter from OPENSSL_INCLUDE_DIR var.
* update capabilities
* VFS-694 Add a missing letter to GLOBUS_INCLUDE_DIRS var.
* VFS-694 Fully rely on pkg-config for Globus.
* copy files and directories
* proto update
* VFS-864 Fix unit test.
* VFS-864 Remove dn from gui web pages.
* VFS-864 Minor refactoring.
* VFS-864 Remove unnecessary dn and GR auth usages.
* VFS-864 Fix all ct tests.
* cdmi_test_SUITE:file_exists now uses logical_files_manager:exists
* VFS-864 Fix user removal in ct tests.
* VFS-864 fix env getter in fslogic:getattr
* VFS-864 Fix some fslogic ct tests.
* VFS-864 Fix all unit tests.
* VFS-864 Fix some unit tests.
* VFS-864 fix user's login handling in fslogic_test
* VFS-864 Fix one fslogic ct test.
* VFS-864 Update ctool ref.
* VFS-837 Fix cdmi ct tests.
* VFS-888 Add file_block DAO record and move file_location into separate documents.
* doc update
* user names in ACLs
* VFS-864 Make ct tests running.
* VFS-837 Update ctool ref.
* VFS-837 Fix login validation.
* VFS-837 Final renaming.
* merge error fix
* VFS-886, merge with develop
* storage_file_manager code reformat after merge
* VFS-837 Fix naming errors after merge.
* VFS-883 use storage user id cache
* VFS-883 improve storage ids cache update
* VFS-837 Make onedata start with an uppercase letter.
* VFS-883 fix ets:lookup match
* VFS-827 Fix provider details expansion.
* VFS-883 cleanup caches before reloading
* VFS-883 add storage user/group ids cache
* VFS-837 Update onepanel ref.
* update onepanel
* VFS-870, get rid of logotype footer
* VFS-870, fix reference to icomoon icons
* VFS-870, remove unused code from file_manager.js
* VFS-870, move icomoon font to ctool
* move definitions to header
* move definitions to header
* permissions cache improvements + some more tests
* VFS-870, newest changes
* VFS-837 Minor formatting changes in init script.
* VFS-837 Rename libs directory.
* VFS-837 Fix application start.
* VFS-837 Fix docs generation.
* VFS-837 Fix generation error.
* VFS-837 Update refs.
* generate message id
* permissions cache + some tests
* VFS-837 Change form 'veil' to 'onedata'.
* typo fix
* VFS-789 update veilhelpers:exec docs
* VFS-789 remove gpv_nif spec
* VFS-789 fix cert confirmation
* virtual acl fix
* VFS-789 add missing doc
* VFS-789 fix typo
* VFS-789 fix message format
* VFS-789 improve code style
* VFS-789 improve fixing storage file owner
* VFS-789 Fix some small style-related things.
* VFS-789 Fix some small style-related things.
* error forbidden test
* VFS-870, add new icons
* VFS-789: remove some newlines
* VFS-789 use nullptr instead 0
* VFS-789: moar std::
* checking perms in cdmi
* checking acl perms in storge_files_manager
* VFS-789 moar std::
* VFS-789 code style improvements
* VFs-859 Fix rpm creation when ONEPANEL_MULTICAST_ADDRESS variable is set.
* VFS-789 remove uneeded get_env
* VFS-789 fix user's ctx
* VFs-859 Fix missing 's'.
* VFS-866 add support for users' logins
* VFS-789 add license info
* VFS-789 add some comments to oneproxy driver
* VFS-789 add missing log
* VFs-859 Spaces and tokens web pages refactoring.
* VFS-676 Update GRPCA.
* VFS-789 log level and oneproxy names defs
* VFS-789 compile fix
* VFS-789: revert tests
* VFS-789 add some docs
* VFS-789 add some docs
* VFS-859 Change page tokens.
* VFS-679 Fix user_logic_tests.
* chmod file to 0 when setting acl, update file_meta fixes
* VFS-679 Communicate with control_panel through gen_server.
* VFS-841, minor fix
* acl check permission unit test
* VFS-841, fix column switches not displaying correctly
* cdmi_acl write
* cdmi_acl read
* VFS-679 Lookup control_panel PID before message delivery.
* test_fix
* revert debug logs
* generating virtual acl, and acl evaluation
* VFS-789 disable write mock
* VFS-841, fix error when moving files to /spaces dir
* VFS-841, update ctool ref
* VFS-841, move some code to js file
* VFS-841, move some code to js file
* VFS-841, add specs do js files
* revert event changes
* revert event changes
* add missing lin
* VFS-789: fix rest authentication
* VFS-679 GUI Session related tweaks and fixes.
* VFS-679 Remove race conditions and fix outstanding problems.
* acl manipulation + get child count callback added to fslogic
* VFS-841, add chmod submenu in file manager
* proto update
* VFS-789: update authorization method
* VFS-679 Update helpers.
* proto update
* VFS-757 Update onepanel ref.
* VFS-855, update bootbox.js refs
* VFS-841, newest changes
* VFS-855, update ref to fileupload.js
* VFS-855, update bootbox.js ref
* VFS-841, update bootbox.j ref
* VFS-679 Refresh access tokens periodically while user is present.
* VFS-789: bundle oneproxy
* VFS-789: add oneproxy src
* VFS-855, use macros rather than hardcoded app name
* VFS-855, bump onepanel
* VFS-841, add chmod popup
* VFS-679 Refresh access token.
* non-cdmi partial upload
* revert valuerange create
* create file with valuerange
* partial upload + xattrs error fix
* VFS-807, update ctool ref
* VFS-807, update ctool ref
* childrenrange support
* disable test verbosity
* get container by range
* error fix
* handlig logical_files_manager forbidden error
* mv support
* VFS-807, move static files to ctool
* additional merge changes
* error names taken from define
* crash when something unexpected happens in cdmi_metadata
* Fix white spaces.
* VFS-793 Remove infinite logging loop.
* VFS-855, change buttons to link to make them work without websocket
* VFS-793 Update onepanel ref.
* VFS-793 Apply recommended changes.
* VFS-793 Remove unneeded log replies.
* checking body correctness
* VFS-855, update docs
* VFS-679 Update ctool.
* VFS-855, update docs
* VFS-855, revert unwanted changes
* VFS-855, revert unwanted changes
* VFS-855, add download_oneclient page
* VFS-793 Update onepanel ref.
* recommended changes
* VFS-793 Move functions using protocol from onepanel to veilcluster.
* error handling
* VFS-679 Handle new OpenID 'emails' content.
* VFS-855, fix ensure unique filename to work with special chars
* VFS-855, fix ensure unique filename to work with special chars
* VFS-855, fix ensure unique filename to work with special chars
* VFS-855, fix ensure unique filename to work with special chars
* VFS-855, fix ensure unique filename to work with special chars
* VFS-847, fix ls not woring prop-erly
* VFS-855, fix ls not working properly
* VFS-855, disable direct login for production
* VFS-847, fix privacy policy cookie
* VFS-847, remove redirect after login mechanism
* VFS-847, remove redirect after login mechanism
* VFS-847, remove redirect after login mechanism
* VFS-855, udpate ctool ref
* Refactor cdmi_metadata module.
* VFS-834: fix normalization of user's name in user_logic:create_partial_user
* VFs-793 Minor fixes.
* Remove unused variable in cdmi_object.
* VFS-833, add link for client download
* Update onepanel ref.
* VFS-833, add link for client download
* VFS-793 Remove pages moved to onepanel.
* out of range read tryfix
* decoder name fix
* add xattrs to get_answer_decoder_and_type
* Add selective metadata update.
* doc update
* merge error fix
* ctool update
* proto update
* list dir refactoring
* set context with token
* VFS-834: fix gruid type in handshake ack
* VFS-834: fix compile
* Fix empty metadata case.
* VFS-834: fix spec in fslogic_remote, improve cache in ws_handler
* ctool update
* send access token hash to user
* Add metadata to containers.
* VFS-834: fix storage_files_maneger:create VFS-834: improve get_user_root
* Improve comments, fix specs and simplify internal functions in cdmi_metradata module.
* Merge remote-tracking branch 'remotes/origin/feature/VFS-830-dokladne-przejrzenie-dokumentacji' into feature/VFS-801-wsparcie-dla-metadanych-cdmi-user
* proto update
* token handling improvements
* w/e
* VFS-828 Better TOKEN caching.
* VFS-828 Check for invalid fuse id, as in better times.
* VFS-829: revert helpers build type
* VFS-84, update veilproto
* VFS-84, update submodules versions
* revert ctool changes
* Minor correction after merge
* VFS-834: fix fslogic_test_SUITE
* remove debug logs
* disable cert checking when generating token
* VFS-834: fix compile and unit tests
* SubmodulesUpdate
* SubmodulesUpdate
* VFS-834: add docs
* SubmodulesUpdate
* SubmodulesUpdate
* debug ctool
* debug ctool
* Eunit tests correction after merge
* additional guard expression
* VFS-834: rename access_token in fslogic_context
* VFS-828 Backport fixes from VFS-834
* Bugs correction after merge
* Bugs correction after merge
* Bugs correction after merge
* Resolve conflicts after merge
* Resolve conflicts after merge
* add stacktrace to log
* VFS-834: fix get_file_children ctx
* VFS-834: code cleanup
* additional range write tests
* Small fixes.
* undef funcion fix
* VFS-828 Allow user authentication through HTTP headers.
* Getting and setting user metadata for CDMI.
* VFS-84, fix spaces not displaying as space folder
* Add user matadata to file attrs. (cherry picked from commit 312049f)
* token support
* token support test commit
* VFS-828 Update client, helpers and protocol refs.
* handling bad base64 strings
* VFS-829: fix typo
* VFS-834: fix perms checking
* handling 401 unauthorized
* VFS-829: hotfix for interprovider rename
* VFS-834: initial impl
* additional tests
* return moved_permanently when someone has forgotten about '/' in path
* mimetype and valuetransferencoding support
* VFS-84, fix spec
* VFS-84, minor fix
* xattrs additional tests and minor fix
* VFS-829: fix some docs
* VFS-829: fix some docs
* basic xattrs support
* VFS-829: revert ++
* proto update
* VFS-84, recommended fixes
* VFS-829: something
* proto update
* VFS-829: add some missing docs
* VFS-829: add some missing docs
* VFS-84, update ctool ref
* VFS-829: add some docs
* VFS-829: add some docs
* return container names with '/' at the end
* VFS-829: add some docs
* VFS-829: fix compile
* VFS-829: add some docs
* stop ignoring cowboy_req return value
* use '_' in some cases
* VFS-829: really move rename impl to seperate module..
* VFS-829: move rename impl to seperate module
* VFS-829: improve error recovery while moving files betwean spaces
* VFS-829: remove some redundant checks
* VFS-84, merge VFS-813
* remove debug logs
* VFS-84, dont fail if there is no provider id
* VFS-84, dont fail if there is no provider id
* VFS-810, add referer to login redirection
* valuerange update
* VFS-829: fix maunual test
* VFS-829: fix fslogic_test_SUITE
* VFS-829: fix compile for ct tests
* VFS-829: change log level and remove some debug logs
* VFS-829: fix some typos
* VFS-829: improve error handling
* read value by Range
* VFS-829: impl for interspace rename
* VFS-84, update ctool ref
* VFS-84, fix typo
* VFS-84, update ctool, dosc update
* VFS-829: fix compile
* VFS-829: fix compile
* VFS-829: initial impl of remote file rename
* VFS-84, fix space dir not displaying correctly in GUI
* VFS-84, tests fix
* VFS-84, tests fix
* VFS-84, tests fix
* VFS-84, tests fix
* VFS-84, add tests for new perms handler in fslogic
* docs
* Fix cowboy cookie error on malformed cookies
* Fix cowboy cookie error on malformed cookies
* VFS-820, add file perms checking
* VFS-84, updat veilproto version
* VFS-84, updat veilproto version
* VFS-84, updat veilproto version
* VFS-84, turn off verbosity in tests
* VFS-84, turn off verbosity in tests
* VFS-84, turn off verbosity in tests
* VFS-84, turn off verbosity in tests
* VFS-84, fix missing funs in veil cowboy bridge
* VFS-84, fix missing funs in veil cowboy bridge
* VFS-84, fix cdmi tests
* VFS-829: make gen_dev tolarate hostname with '-' character and allow to use custom output directory
* 84, turn off verbosity in tests
* 84, tests fix
* final touches
* 84, add exception handling during veil_cowboy_bidge apply
* 84, add exception handling during veil_cowboy_bidge apply
* return root container when pointing to default space
* VFS-84, start meck in tests
* VFS-84, add spaces mock for logging in without GR
* VFS-84, revert changes
* VFS-84, remove io:formats
* Remove unnecessary logging.
* Fix time-dependent rrderlang unit tests.
* VFS-84, minor fix
* VFS-84, fix submodules versions
* VFS-84, static file handler works on veil cowboy bridge
* improve ct compile
* ReleaseNotes updated
* Submodules updated
* VFS-787: fix logical_files_manager:create behaviour in case of race
* VFS-787: fix cdmi_test_SUITE
* improve provider's connection management
* VFS-781 Ensure binary secret and uid.
* Update helpers' and client's refs.
* update protocol
* VFS-781 Allow user to be authenticated by a token hash.
* fix typo in cdmi_test_SUITE
* Merge remote-tracking branch 'origin/develop' into feature/VFS-787_VFS-755-dostep-do-zdalnych-plikow-z-gui-
* VFS-787: remove debug
* VFS-84, dosc
* VFS-84, dosc
* VFS-84, minor fixes
* VFS-787: remove debug
* using contact_fslogic/1 instead of contact_fslogic/2
* update ctool
* VFS-787: fix typo
* VFS-787: update dao
* update onepanel
* Added todo for cdmi_size for directories.
* Move default_storage_system_metadata definition to new header file.
* VFS-787: fix fslogic_test_SUITE
* VFS-798 Add +x to onepanel_setup script.
* VFS-84, minor fixes
* VFS-798 Update OnePanel ref.
* add get_file_uuid to fslogic funcionality
* VFS-787: fix some fslogic_test_SUITE
* VFS-84, docs
* VFS-84, docs
* VFS-84, docs
* VFS-84, sessions clearing, docs
* revert eunit removal
* VFS-84, add delegation swithing
* protocol update
* VFS-798 Update DAO ref.
* doc update
* Revert variable names in get_file_test,
* Revert variable names in get_file_test,
* Simplify metadata_with_prefix function using binary:longest_common_prefix.
* Extract metadata test case.
* VFS-787: fix some fslogic_test_SUITE cases
* extract definitions to headers + some more documentation
* VFS-766 Update OnePanel ref.
* VFS-84, scalable control_panel, session memory based on DB
* ignore unknown values in object and capability requests
* VFS-787: fix rest_test_SUITE
* VFS-787: fix rest_test_SUITE
* VFS-766 Update onepanel ref
* VFS-787: fix typo
* VFS-787: fix typo
* VFS-787: fix typo
* capabilities tests + minor improvements
* VFS-766 Fix path in setup scripts.
* VFS-798 Update OnePanel ref.
* VFS-787 Add additional log.
* cdmi_capabilities support, objectid tests, some fixes
* VFS-787: fix user management in fslogic_test_SUITE
* Add function docs.
* VFS-787: fix remote_files_manager_test_SUITE
* Add selective metadata read.
* VFS-787: fix some docs
* VFS-787: fix crash_test_SUITE
* VFS-787: fix ccm_and_dispatcher_test_SUITE
* VFS-787 Add additional log.
* VFS-787: fix nodes_management_test_SUITE
* Refactor prepare_metadata so it will be possible to filter metadata.
* Parsing metadata in URL (cherry picked from commit e6e42b5)
* VFS-743, Protecting atoms table from overload
* objectid support with basic test
* VFS-743, Input messages checking tests update
* VFS-787: fix eunit tests
* VFS-743, Input messages checking upgrade
* VFS-743, Input messages checking upgrade
* VFS-787: moar docs
* VFS-787: moar docs
* Simplify test.
* VFS-787: fix compiel
* VFS-787: fix merge
* update helpers
* VFS-787: docs for ws_handler
* VFS-787: docs for ws_handler
* compilation fix, some old todo removed
* fix argument count in docs
* VFS-787: docs for provider_proxy_con
* VFS-787: remove , "c_src/veilhelpers_nif/communicator.cc"
* VFS-787: add some docs
* VFS-787: add some docs
* VFS-787 Fix change Space name function.
* VFS-787: fix OS group ctx
* VFS-787: support for global default spaces
* VFS-787 Remove unnecessary function.
* VFS-787 Default user's Space.
* remove unnecessary changes
* remove unnecessary changes
* Add metadata for containers.
* Add cdmi storage system metadata to objects' get and put response.
* support version 1.0.1, fix cdmi file get errors
* selective file update, better error handling
* VFS-787 Default user's Space.
* VFS-787: fix something
* VFS-787 Update file description.
* VFS-787: remove some unneeded code
* VFS-787: fix storage space's initialization
* VFS-787: fix ctool integration
* update file first attempt
* VFS-787: improve inter-provider ClusterProxy
* VFS-754: add cluster proxy getattr
* VFS-787: add ClusterProxy:getattr
* VFS-787: update protocol
* streaming files with cdmi content type
* VFS-787: initial impl of remote-gui ops
* VFS-755 Add Spaces synchronization.
* VFS-755 Fix compilation errors.
* VFS-755 Update ctool ref.
* VFS-755 Refactoring.
* VFS-755 Refactoring.
* VFS-755 Using ctool to verify login.
* VFS-755 Fix execution errors.
* VFS-755 Fix compilation errors.
* VFS-755 Add Global REST API client.
* VFS-755 Fix page style.
* handle non-cdmi container create
* VFS-732, update ctool ref
* VFS-732, update ctool ref
* VFS-732, fix logout not working
* remove duplicated body entities checking
* VFS-732, turn off verbose mode in rest test
* VFS-732, adjust tests to new cowboy HTTP return codes
* wrong 'method' value format fix
* validating req format
* VFS-732, revert unwanted changes
* VFS-732, update onepanel ref
* VFS-732, merge with release
* VFS-732, update ref to ctool
* checking cdmi version
* VFS-732, update ctool ref
* VFS-803, file managers caches clearing update
* VFS-732, fix record name clash in wf/ibrowse
* VFS-732, fix record name clash in wf/ibrowse
* VFS-732, update n2o
* VFS-803, file managers caches clearing
* VFS-779, fix errors in pasting from clipboard
* VFS-732, add login mechanism for development
* VFS-732, add login mechanism for development
* VFS-732, add login mechanism for development
* VFS-732, fix upload not working on cowboy 1.0
* VFS-732, fix upload not working on cowboy 1.0
* VFS-732, fix upload not working on cowboy 1.0
* VFS-732, fix privacy_policy_url macro usage
* VFS-766 Apply recommended changes.
* VFS-766 Update onepanel submodule.
* VFS-766 Using _prefix in veil.spec.
* VFS-766 Update onepanel submodule.
* VFS-766 Fix minor bug.
* VFS-766 Fix rebar tuple in application env.
* VFS-766 Update onepanel submodule.
* VFS-766 Using multicast address env variable.
* VFS-732, update ctool ref
* VFS-766 Update onepanel submodule.
* VFS-779, adjust tests code
* VFS-779, adjust tests code
* VFS-779, adjust tests code
* VFS-779, revert cowboy to 0.9.0
* VFS-779, change session TTL and openid redirection method to GET
* VFS-766 Fix node types names.
* VFS-766 Fix paths in setup scripts.
* missing parameter added
* VFS-754: hack-support for osx
* VFS-754: protocol update
* client update
* fix tests
* ReleaseNotes and veilclient update
* fix tests
* add nofollow flag to helpers::open
* revert remove uneeded user ctx setup
* remove uneeded user ctx setup
* Apply recommended changes.
* Remove unnecessary logs.
* Fix CSRF logout.
* client update
* cdmi_handler refactoring, new layer introduced
* finding ca in certs dir
* allow to have different certificates in gui and dispatcher
* cdmi handler name changed + small refactoring
* Add html encoding.
* VFS-754: fix providers endpoint urn
* fix user effective uid/gid setup & remove all secondary groups
* VFS-754: fix spaces perms
* Update onepanel ref.
* VFS-755 Change CA file path.
* VFS-755 Beautify.
* VFS-755 Add page for tokens.
* VFS-754: fix providers endpoint urn
* VFS-754: add support for push messages
* VFS-754: add support for push messages
* VFS-754: add support for push messages
* VFS-754: add support for push messages
* is_group_dir refactoring
* code reformat
* VFS-754: update onepanel
* VFS-754: improve proxy communication
* tests documentation
* VFS-755 Change page header.
* VFS-755 Beautify popup dialogs.
* VFS-754: fix space name sync
* VFS-754: fix space name sync
* submodules update
* submodules update
* Release notes created
* VFS-754: fix space name sync
* VFS-749: remove undefined method invocation
* VFS-755 Changing Space name allowed.
* VFS-755 Add Space invitation tokens.
* doc update
* doc update
* cdmi version header support
* VFS-755 Page space added.
* VFS-754: improve remote error handling
* VFS-754: improve remote error handling
* streaming get_noncdmi_file
* VFS-754: support for remote_file_manager
* client update
* client update to release
* streaming file download
* VFS-755 Page spaces done.
* client update
* handle noncdmi file PUT + test
* VFS-754: support for remote_file_manager
* VFS-754: full user ctx setup
* non-cdmi object creation
* client updated
* VFS-754: full user ctx setup
* client updated
* VFS-754: full user ctx setup
* VFS-754: full usert ctx setup withing fslogic for remote requests
* VFS-754: compile fix
* VFS-754: update ctool
* VFS-754: user authentication impl
* VFS-754: user authentication impl
* VFS-755 Add popup dialogs.
* prepare_object_ans/prepare_container_ans refactoring
* Fix monitoring issue.
* selective read + tests
* VFS-754: spaceId/binary
* VFS-754: spaceId/binary
* VFS-754: spaceId/binary
* read file with basic test + some todo implemented
* VFS-755 Change space management page.
* VFS-754: spaceId/binary
* additional create file tests
* VFS-754: move space info record definition
* create file with base64 content test
* VFS-754: impl first working pull reroute
* VFS-754: impl first working pull reroute
* VFS-754: impl first working pull reroute
* VFS-754: update protocol
* VFS-754: impl first working pull reroute
* create file + basic test
* VFS-754: fix certs handling
* VFS-754: compile fix
* VFS-754: update deps
* VFS-754: provider_proxy impl
* VFS-754: provider_proxy impl
* VFS-754: provider_proxy impl
* VFS-749: fix args for helper getter
* VFS-749: protocol update
* VFS-749: protocol update
* VFS-749: compile fix
* VFS-749: support for forcing ClusterProxy helper
* VFS-749: update deps
* sync spaces with GR
* sync spaces with GR
* some minor fixes
* some minor fixes
* create dir + tests
* VFS-754: update protocol
* some refactoring
* delete_file_test + some minor fix in cdmi handler
* VFS-746 Fix Global Registry hostname format.
* VFS-746 Apply recommended changes.
* header doc update
* dir/file remove support + some tests
* some minor fixes
* VFS-746, fix tests
* VFS-746, add html_encode to possible XSS injection point
* VFS-746, fix tests
* VFS-746, fix typo
* VFS-746, fix typo
* VFS-746, fix tests
* VFS-746, update ctool
* VFS-746, minor fixes
* VFS-746, minor fixes
* VFS-746, remov edevel funciotns
* container selective get handling
* additional tests
* list dir extended + refactored
* VFS-746, minor fix
* VFS-746, code handling redirects from GR
* basic list dir cdmi operation with tests
* VFS-748, merge release to develop
* VFS-748, use POST redirect in OpenID
* VFS-748, use POST redirect in OpenID
* VFS-748, use POST redirect in OpenID
* VFS-748, use POST redirect in OpenID
* VFS-748, use POST redirect in OpenID
* update deps
* Update client and helpers refs.
* Update client and helpers refs.
* add rename_file support
* Test timeout update
* add rename_file support
* Sub proc management update
* helpers and client update
* VFS-709, Eunit tests update.
* VFS-709, Eunit tests update.
* helpers and client up
* VFS-709, Eunit tests update.
* helpers up
* update ctool version
* update ctool dependency
* fix undeterministic behaviour in logging tests
* fix undeterministic behaviour in logging tests
* fix undeterministic behaviour in logging tests
* VFS-738 Fix log format.
* add missing export
* compile fix
* compile fix
* update protocol
* test
* compile fix
* compile fix
* helpers up
* VFS-719 Update onepanel submodule.
* compile fix
* VFS-738 Fix test.
* VFS-738 Fix atom limit overflow.
* compile fix
* compile fix
* compile fix
* revert submodules to proper version
* VFS-443, fix tests
* update helpers
* VFS-443, fix tests
* test impl
* VFS-443, fix tests
* client up
* client up
* heleper up
* VFS-443, remove animation time on auto scroll on logs pages
* VFS-443, fix typo
* merge VFS-443
* VFS-443, fix CSS on page_cliet_logs
* VFS-711 Update veilhelpers_nif to work with current VeilHelpers.
* VFS-443, minor fix
* update client (some merge errors fixed)
* update client and helpers with lepsze-zarzadzanie-statycznymi-danymi
* minor fixes
* VFs-443, remove badmatch liability
* mochiweb version
* fix polish chars handloing in REST
* fix polish chars handloing in REST
* fix polish chars handloing in REST
* revert submodules versions
* fix polish chars display in team names
* revert submodules version
* tests fix
* test impl
* test impl
* test fix
* fix polish chars handling in teams xml
* client update
* test impl
* VFS-443, fix problems with polish chars
* client update to latest
* update proto, client, helpers
* merge VFS-443
* merge VFS-443
* VFS-443, tests fix
* downgrade client
* test impl
* test impl
* VFS-443, tests fix
* VFS-719 Update onepanel submodule.
* VFS-719 Apply recommended changes.
* VFS-443, set submodules to proper versions
* VFS-443, set submodules to proper versions
* VFS-443, revert changes
* VFS-443, revert changes
* VFS-443, install trace files for clients
* VFS-443, working interception of client logs
* client update
* VFS-443, newest changes
* update client, helpers, protocol
* VFS-719 Update onepanel submodule.
* VFS-719 Fix veil.spec.
* VFS-719 Update onepanel submodule and veil.spec.
* client and helpers update
* proto update
* test fix
* helpers and proto update
* additional export
* new file mode setting
* add s_bit to default directory perms
* client update to release
* test adjustment to new default perms
* VFS-719 Fix rpm build
* change default group files permission at storage
* VFS-719 Update onepanel submodule.
* test adjustment
* set sticky bit in group dirs
* remove sticky bit checking, always check owner
* VFS-719 Fix Makefile.
* VFS-719 Onepanel submodule added.
* VFS-719 Change location of Onepanel release.
* remove sticky bit from groups dir
* VFS-719 Change script header.
* VFS-719 Move onepanel_setup script to VeilCluster files.
* VFS-719 Adding Onepanel to RPM.
* VFS-443, design of client management popup in page_client_logs
* client update
* client updated with logs
* client set to integration-2 with release protocol
* set client to old one
* client update
* proto + helpers update to release
* update protocol and helpers to develop
* VFS-443, fixes in perms checking
* VFS-443, fixes in perms checking
* client update
* client set to newer one (nearly develop)
* remove unnecessary mocks
* doc update
* handling UNDEFINED_MODE in check_file_perms
* recommended changes v2
* recommended changes
* client update
* VFS-443, initial commit
* client update
* client update
* client update
* VFs-443, minor change
* VFS-443, revert submodules to proper versions
* rename_file permission checking change, doc update
* VFS-443, add permissions checking during file download and upload
* VFS-716: update deps
* VFS-716: revert some invalid changes
* client update to release
* client update
* VFS-443, revert submodules to proper versions
* VFS-443, revert unwanted changes
* VFS-443, use erlang ssl instead of curl
* VFS-443, use erlang ssl instead of curl
* VFS-443, update gui_session_handler
* VFS-443, update gui_session_handler
* ct fix
* VFS-716: merge veilcluster/release -> veilcluster/develop
* eunit test fix
* export for eunits
* tests fix
* EPERM -> EACCES error code change
* client update
* VFS-443, update gui_session_handler
* VFS-443, update gui_session_handler
* compilation fix
* save_file_descriptor multiple return values fix
* VFS-443, remove changes concerning central logger
* VFS-443, remove changes concerning central logger
* changing perms only if they're different from current ones
* VFS-443, fix problems with desynchronization of selected elements
* client update
* enable all ct tests
* VFS-443, newest changes
* client update
* enable sticky bit on team dirs
* client update
* VFS-443, newest changes
* client update
* client update
* client update
* client update
* client update
* client update
* VFS-443, adjust tests
* client update
* enable debug logs on fslogic and disable all other tests
* client update
* client update
* VFS-443, adjust tests
* client update
* checking write permission in parent dir, during file removal
* VFS-443, adjust tests
* VFS-443, adjust tests
* client update
* VFS-443, adjust tests
* VFS-443, fix problems with unicode and utf8 encoding
* client update
* client update
* client update
* VFS-443, cookie policy system
* client update
* exclude dirs from storage chmod
* client update
* VFS-443, cookie policy system
* client update
* wrong function call fix
* client update
* VFS-443, cookie policy system
* client update
* client update
* client update
* client update
* client update
* client update
* client update
* VFS-443, fix behaviour of download handler during errors
* client update
* VFs-443, newest changes
* client update
* client update
* client update
* client update
* client update
* client update
* update
* client update
* compilation fix
* client update
* all changes
* VFS-496: update deps
* fix exports
* add missing header
* add missing header
* add missing export
* fix merge release-1.0
* VFS-174, Minor logs update.
* fix merge
* VFS-174, White spaces update
* VFS-174, Minor logs update.
* VFS-174, White spaces update
* VFS-174, White spaces update
* fic merge
* VFS-174, Corrections after merge
* VFS-174, Logs updated
* Add myself to the team member list.
* VFS-714, fix error in binary to unicode conversion
* VFS-714, fix error in binary t ounicode conversion
* VFS-443, merge with release
* VFS-608, minor fix
* VFS-608, update submodules to develop
* VFS-608, merge release to develop
* VFS-608, merge release to develop
* VFS-608, merge release to develop
* VFS-608, merge release to develop
* VFS-608, merge release to develop
* VFS-608, merge release to develop
* VFS-608, merge release to develop
* client update
* VFS-714, initial commit
* default open mode message changed
* wrong set_context usage fix
* test commit
* VFS-608, update veilclient version
* VFS-608, update veilclient version
* VFS-443, split client and cluster logs page into two
* helpers update
* helpers update
* proto update
* VFS-443, revert veilclient and helpers version change
* VFS-443, revert veilclient and helpers version change
* VFS-496: compile fix
* VFS-174, Logs updated
* VFS-496: remove designs declarations
* VFS-496: add some specs
* comment out some code
* VFS-174, Logs updated
* VFS-496: rebase onto release-1.0: deps
* VFS-496: rebase onto release-1.0
* VFS-496: rebase onto release-1.0
* VFS-496: rebase onto release-1.0
* test fix
* VFS-608, revert veilvlient and helpers version change
* temporary removed perm test
* VFS-174, Logs updated
* VFS-496: fix module reload
* chmod working with cluster proxy
* VFS-174, Logs updated
* VFS-443, minor fix
* VFS-443, merge changes to page logs from old branch
* VFS-608, minor fix
* VFS-443, initial commit
* VFS-608, fix log message when cert is declined
* VFs-443, initial commit
* VFS-608, revert unwanted changes
* VFS-608, revert unwanted changes
* VFS-608, report failed uploads only once
* VFS-608, get rid of user doc caching in session memory
* VFS-711 Update veilhelpers_nif to work with current VeilHelpers.
* VFS-711 Update helpers' ref.
* VFS-608, fix duplicate view name in db
* VFS-711 Update client and helper refs.
* VFS-608, merge with release-1.0
* VFS-711 Update bigcouchdb and veilclient refs to current release.
* VFS-711 Add -std=c++11 flag for helpers' build.
* VFS-608, add redirect handler for addresses starting with www
* VFS-711 Update helpers' ref to current release version.
* VFS-608, add redirect handler for addresses starting with www
* proto update
* VFS-174, Logs updated
* VFS-608, adjust tests
* VFS-608, adjust tests
* VFS-608, implement cert confirmation
* VFS-608, implement cert confirmation
* VFS-608, update veilproto version
* VFS-608, unverified verts handshake
* VFS-608, unverified verts handshake
* VFS-608, checkout newer veilproto ver
* VFS-608, use login as user's DN, add email verification
* helpers update
* helpers update
* helpers update
* helpers update
* VFS-608, update exports on page modules
* helpers update
* VFS-565: remove logger.hrl from stress tests
* VFS-565: remove logger.hrl from stress tests
* VFS-608, update exports on page modules
* helpers update
* VFS-608, update page_manage_acc
* VFS-608, add unverified_dn_list field to user doc
* VFS-658, update ctool version
* VFS-658, update ctool version
* VFS-658, update ctool version
* VFS-658, update ctool version
* VFS-658, update ctool version
* VFS-658, revert unwanted changes
* VFS-658, move get_cookie_ttl function to session_logic_behaviour
* VFS-174, Logs updated
* VFS-658, minor fix
* VFS-658, minor fix
* VFS-658, minor fix
* VFS-658, minor fix
* VFS-658, minor fix
* VFS-658, move logger tests out to ctool
* VFS-658, move logger tests out to ctool
* VFS-658, move logger tests out to ctool
* VFS-658, minor fixes
* VFS-658, minor fixes
* VFS-658, minor fixes
* VFS-658, minor fixes
* VFS-658, minor fixes
* VFS-658, minor fixes
* remove old st_utils.erl
* VFS-658, minor fixes
* VFS-658, minor fixes
* VFS-658, extract gui utils modules to ctool
* VFS-658, extract gui utils modules to ctool
* VFS-658, move some deps to ctool
* VFS-658, move some deps to ctool
* VFS-658, remove ranch from deps
* VFS-174, eunit tests update
* VFS-174, eunit tests update
* VFS-174, eunit tests update
* VFS-174, eunit tests update
* VFS-174, eunit tests update
* VFS-174, eunit tests update
* VFS-174, logging test
* VFS-174, include logging hrl where needed
* VFS-174, include logging hrl where needed
* VFS-174, include logging hrl where needed
* VFS-174, include logging hrl where needed
* fix unreachable code
* new dao commit tag
* VFS-174, lager:* changed to new macros
* fix unreachable code
* VFS-692 Move st_utils.
* VFS-692 Add logs.
* VFS-565: add sample fprof config
* VFS-692 Change test duration.
* VFS-692 Fix exports.
* VFS-692 Change limit test config. Add common stress tests functions module.
* VFS-565: update fprof runner
* VFS-692 Change arguments parsing in load_runner.escript.
* VFS-692 Return initial tests durations.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations.
* VFS-565: fprof test
* VFS-565: fprof test
* VFS-565: fprof test
* VFS-692 Change tests durations.
* VFS-565: fprof test
* VFS-565: fprof test
* VFS-565: fprof test
* VFS-565: include logger and gsi_handler
* VFS-692 Add setup to limit test.
* VFS-692 Remove logs.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations and add logs.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations and update basho_bench ref.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations.
* VFS-692 Fix load logging mechanism and update basho_bench.
* VFS-692 Fix load logging mechanism.
* VFS-692 Fix load logging mechanism.
* VFS-565: compile basho_bench without veilcluster source
* VFS-692 Add logs.
* VFS-692 Validating column names in RRD.
* VFS-692 Validating column names in RRD.
* VFS-692 Change basho_bench dependency.
* VFS-692 Change test duration.
* VFS-692 Remove logs.
* VFS-692 Add logs.
* VFS-692 Add logs.
* VFS-692 Change tests durations.
* VFS-692 Fix compilation error.
* VFS-692 Fix compilation error.
* VFS-692 Fix compilation error.
* VFS-692 Fix load logging during stress test.
* VFS-692 Fix load logging during stress test.
* VFS-692 Refactor stress tests code.
* VFS-692 Fix net_adm:ping function error.
* VFS-692 Change tests durations and run limit test.
* VFS-692 Change tests durations.
* VFS-692 Fix dd stress test.
* VFS-692 Make directory with parents.
* VFS-692 Clear storage before addition.
* VFS-692 Insert storage before user creation.
* VFS-692 Change cluster hostname.
* VFS-692 Change cluster hostname.
* VFS-692 Add additional logging.
* VFS-692 Fix compilation error.
* VFS-692 Add additional logs.
* VFS-692 Fix compilation error.
* VFS-692 Change tests durations and add logs.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations.
* VFS-692 Change test duration.
* include guards + unnecessary code removal
* VFS-692 Fix exports.
* VFS-692 Change limit test config. Add common stress tests functions module.
* dao_driver -> dao_external
* VFS-692 Change arguments parsing in load_runner.escript.
* VFS-692 Return initial tests durations.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations.
* same as bugfix/VFS-682-cleanup-po-deinstalacji-rpma but created from release branch
* dao_driver:sequential_synch_call
* VFS-692 Change tests durations.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations.
* VFS-692 Add setup to limit test.
* VFS-692 Remove logs.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations and add logs.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations and update basho_bench ref.
* VFS-692 Change tests durations.
* some_record definition extracted to common.hrl
* VFS-596, mionr fix
* VFS-596, fix typo in rebar.config
* VFS-692 Change tests durations.
* VFS-692 Fix load logging mechanism and update basho_bench.
* VFS-692 Fix load logging mechanism.
* VFS-692 Fix load logging mechanism.
* merge branch develop
* VFS-692 Validating column names in RRD.
* VFS-692 Validating column names in RRD.
* cleanup after rpm deinstallation v2
* VFS-692 Change basho_bench dependency.
* cleanup after rpm deinstallation
* revert client change
* client update
* VFS-644 Change veil dependencies.
* VFS-692 Change test duration.
* VFS-692 Remove logs.
* VFS-644 Add authorization to curl opts for installation setup.
* VFS-692 Add logs.
* VFS-692 Add logs.
* VFS-692 Change tests durations.
* VFS-596, update specs
* VFS-596, update specs
* VFS-692 Fix compilation error.
* VFS-692 Fix compilation error.
* VFS-692 Fix compilation error.
* VFS-596, fix typo
* VFS-596, fix typo
* VFS-596, fix typo
* VFS-596, add periodical epired sessions clearing
* externalize some common methods to gui_utils
* externalize some common methods to gui_utils
* VFS-692 Fix load logging during stress test.
* VFS-692 Fix load logging during stress test.
* VFS-692 Refactor stress tests code.
* VFS-692 Fix net_adm:ping function error.
* VFS-692 Change tests durations and run limit test.
* VFS-596, update specs
* VFS-cookies-hotfix, remove fpermissive flag
* VFS-cookies-hotfix, remove debug
* VFS-596, implement clearing expired sessions
* VFS-692 Change tests durations.
* VFS-cookies-hotfix, update certs to have longer private keys
* VFS-596, move some code to session_logic
* VFS-cookies-hotfix, test debug
* VFS-596, code refactoring
* VFS-692 Fix dd stress test.
* VFS-692 Make directory with parents.
* VFS-596, specs updates
* VFS-cookies-hotfix, adjust cookies in gen_dev and CT to new bamboo naming convention
* VFS-596, newest changes
* dao_tests split into dao_driver, dao_worker, and dao_records (from dao project)
* VFS-692 Clear storage before addition.
* VFS-692 Insert storage before user creation.
* some setup functions moved to dao_setup
* VFS-692 Change cluster hostname.
* VFS-692 Change cluster hostname.
* apply all changes again
* revert all
* VFS-692 Add additional logging.
* VFS-hostname-in-cookie, all cookies (db, cluster, cluster during CT) all now est to node hostname
* verbosity turned off
* dao.erl -> dao_worker.erl, eunit
* VFS-596, fix session error dialog on login page
* dao.erl -> dao_worker.erl, test commit
* VFS-692 Fix compilation error.
* VFS-692 Add additional logs.
* VFS-692 Fix compilation error.
* VFS-692 Change tests durations and add logs.
* VFS-692 Change tests durations.
* VFS-692 Change tests durations.
* dao.erl -> dao_worker.erl
* VFS-692 Change tests durations.
* VFS-644 Apply recommended changes.
* dao_records extraction
* VFS-583, revert unwanted changes
* VFS-583, fix test not starting because of cowboy
* VFS-583, fix test not starting because of cowboy
* VFS-644 Fix minor bug.
* VFS-644 Update bigcouch submodule.
* VFS-644 Automatic vfs_storage.info file creation.
* fix typo
* fix typo
* fix typo
* VFS-583, small fixes
* VFS-583, small fixes
* VFS-583, fix tests
* VFS-583, fix tests
* VFS-583, resolve some security issues with GUI
* VFS-583, resolve some security issues with GUI
* rebase origin/release-1.0
* rebase origin/release-1.0
* rebase origin/release-1.0
* rebase origin/release-1.0
* rebase origin/release-1.0
* rebase origin/release-1.0
* rebase origin/release-1.0
* VFS-583, protection against XSS
* VFS-558: fix test
* VFS-558: fix test
* VFS-558: update certs
* VFS-558: update certs
* VFS-558: update certs
* VFS-558: update client certs
* VFS-558: update client certs
* VFS-558: revert wss.erl
* VFS-558: update wss connect opts
* VFS-558: ct debug
* VFS-558: ct debug
* VFS-558: ct debug
* VFS-558: ct debug
* VFS-553: use all ciphers
* VFS-558: remove des-crc cipher
* VFS-531 Fix rrderlang tests.
* VFS-531 Apply recommended changes.
* VFS-531 Move rrderlang timeout to environment variables.
* rebase origin/release-1.0
* VFS-531 Fix test description.
* VFS-531 RRDErlang unit tests added.
* VFS-531 Add 'fetch' handle to gen_server.
* VFS-531 Move rrderlang to veilcluster.
* VFS-531 Checking whether chart is not already available on page.
* VFS-531 Resize RRA. Fix time range.
* VFS-531 Fix time range.
* VFS-531 Fix memory leak.
* test_utils header and guard
* ctool update
* ctool includes directory update
* db cleanup added
* cleaning test directories
* cleaning test directories
* vcn_utils moved to ctool
* nodes_manager rename to test_utils, crash test fixed
* start_asserions removed
* get_db_node refactoring
* addidional functions moved to ctool
* addidional delete
* example suite refactoring, cleanup in nodes_manager
* add missing args
* using ctool start/stop_app_on_nodes
* date update
* some minor error fixes
* delete surplus assertions, fix crash_test init
* using get_db_node from ctool
* using additional functions from ctool
* start test node function taken from ctool
* start/stop deps for tester node taken from ctool
* INIT_DIST_TEST macro taken from ctool
* doc update
* assertion macros taken from ctool header
* rest routing fixed, debug changes reverted
* debug log
* rest test verbosity enabled
* event(init) function added to page_connection_check
* * test fix * gui route to check_connection_page taken from header file
* name changed from 'test' to 'connection_check', some terms moved to new header file
* ct tests
* missing header fix
* test callbacks for gui and rest added
* VFS-531 Merge monitoring functionality.
* debug logs
* VFS-583, some code refactoring
* VFS-582, revert gen_dev.args modification
* VFS-583, some code refactoring
* VFS-583, get rid of more wf calls
* VFS-583, get rid of more wf calls
* VFS-583, get rid of more wf calls
* VFS-583, get rid of more wf calls
* VFS-583, get rid of more wf calls
* VFS-583, get rid of more wf calls
* eunit test delete reverted
* dao_tests fix applied again
* VFS-583, introduce gui_ctx
* VFS-583, more wf call extractions to gui_jq
* VFS-583, more wf call extractions to gui_jq
* test fix revert
* error logs improvement
* VFS-583, more wf call extractions to gui_jq
* VFS-583, more wf call extractions to gui_jq
* dao_tests fix
* VFS-583, more wf call extractions to gui_jq
* common prefixes moved to dao/include/common.hrl
* VFS-583, change all &#35;jqeury events to gui_jq calls
* VFS-583, move some common jq functions to gui_jq
* VFS-583, move some common jq functions to gui_jq
* VFS-583, move some common jq functions to gui_jq
* VFS-583, move DOM update funcitons to gui_jq
* VFS-583, extract conversion functions for gui_utils to gui_convert
* VFS-583, revert unwanted changes
* VFS-583, revert unwanted changes
* VFS-583, revert unwanted changes
* VFS-583, revert unwanted changes
* VFS-583, revert unwanted changes
* VFS-583, revert unwanted changes
* VFS-583, revert unwanted changes
* VFS-583, readd rrderlang to reltool
* VFS-583, readd rrderlang to reltool
* VFS-583, readd rrderlang to reltool
* VFS-583, remove mimetypes deps, dump n2o cowboy and ranch, fix page codes to work
* VFS-531 Merge more monitoring functionality.
* VFS-531 Merge more monitoring functionality.
* VFS-531 Merge monitoring functionality.
* temporary removed dao eunit tests
* dao_helper, dao_hosts and dao_json moved to dao project
* VFS-531 Fix rrderlang tests.
* VFS-531 Apply recommended changes.
* VFS-531 Move rrderlang timeout to environment variables.
* VFS-531 Apply recommended changes.
* VFS-531 Fix test description.
* VFS-531 RRDErlang unit tests added.
* VFS-531 Add 'fetch' handle to gen_server.
* VFS-531 Move rrderlang to veilcluster.
* VFS-531 Checking whether chart is not already available on page.
* VFS-531 Resize RRA. Fix time range.
* VFS-531 Fix time range.
* VFS-531 Fix memory leak.
* fix unreachable code
* fix compile
* test_utils header and guard
* ctool update
* VFS-607, use library uri parsing rather than handmade
* fix warnings [unused variable]
* ctool includes directory update
* remove debug
* VFS-496: update views file names
* VFS-496: update views file names
* VFS-496: update views file names
* VFS-496: update views file names
* db cleanup added
* cleaning test directories
* cleaning test directories
* VFS-496: add apply_from_binary
* vcn_utils moved to ctool
* nodes_manager rename to test_utils, crash test fixed
* start_asserions removed
* get_db_node refactoring
* addidional functions moved to ctool
* addidional delete
* VFS-496: refactor db structure management
* example suite refactoring, cleanup in nodes_manager
* add missing args
* using ctool start/stop_app_on_nodes
* VFS-563: compile fix
* VFS-563: add some docs
* VFS-563: change logs level
* date update
* some minor error fixes
* delete surplus assertions, fix crash_test init
* using get_db_node from ctool
* using additional functions from ctool
* VFS-607, remove old commented-out code
* VFS-563: check skipped tests
* VFS-607, adjust tests
* VFS-607, adjust tests
* start test node function taken from ctool
* start/stop deps for tester node taken from ctool
* INIT_DIST_TEST macro taken from ctool
* doc update
* VFS-594: revert intellij config
* VFS-594: revert intellij config
* VFS-594: update tests
* VFS-594: update docs
* VFS-607, introduce api for secure http requests
* VFS-594: fix
* assertion macros taken from ctool header
* rest routing fixed, debug changes reverted
* VFS-594: add debug info
* VFS-594: add debug info
* debug log
* rest test verbosity enabled
* event(init) function added to page_connection_check
* * test fix * gui route to check_connection_page taken from header file
* VFS-594: fix ct tests
* name changed from 'test' to 'connection_check', some terms moved to new header file
* VFS-594: disable unneeded tests
* ct tests
* VFS-594: remove check_perms form remote files manger
* missing header fix
* VFS-594: dont allow for unknown user access
* VFS-594: fix some veilhelpers args
* VFS-594: fix unit tests
* VFS-594: fix compile
* VFS-594: fix compile
* VFS-594: fix compile
* test callbacks for gui and rest added
* VFS-594: fix compile
* VFS-594: set fs user ctx for remote fs ops
* VFS-594: fix compile
* VFS-594: add user ctx to veilhelpers calls
* VFS-531 Merge monitoring functionality.
* VFS-607, adjust tests to new coe
* VFS-607, fix typo
* VFS-607, introduce security fixes to openid
* VFS-553: add missing docs
* VFS-553: add missing docs
* VFS-553: fix eunit tests
* VFS-553: fix eunit tests
* VFS-553: some new eunit tests
* VFS-553: fslogic_objects_tests
* VFS-553: fix storage id
* VFS-553: revert log level
* VFS-553: revert test debug mode
* Fix for read_event for stats
* VFS-553: move API functions
* VFS-553: fslogic_req_special docs
* VFS-553: fslogic_req_regular docs
* VFS-553: fslogic_req_generic docs
* VFS-553: fix compile
* VFS-553: fslogic_path docs
* VFS-553: fslogic_objects docs
* VFS-553: fslogic includes cleanup
* VFS-553: log cleanup
* Fix guy events for stats
* VFS-553: includes cleanup
* VFS-553: fslogic_file docs
* VFS-553: change get_by_uuid methods error handling
* VFS-553: add some fslogic_file docs
* VFS-553: fix eunit tests
* VFS-457, minor update
* VFS-553: fix n2o megre
* VFS-553: fix unit tests
* VFs-457 Fix os_mon dependency.
* VFS-457 Test for IO storage events added.
* VFS-553: add fslogic_file_tests template
* VFS-553: fslogic_errors tests
* VFS-553: fslogic_error docs
* VFS-553: fslogic_context tests
* VFS-553: fslogic_context docs
* VFS-553: fix compile
* VFS-553: fix compile
* VFS-553: fix compile
* VFS-553: use getter/setter for user_dn
* VFS-553: fix merge VFS-498
* VFS-553: fix merge VFS-498
* VFS-457 Move storage test file prefix to define.
* VFS-553: add unit test files
* VFS-256, fix typo
* VFS-256, add some comments for clarity
* VFS-457 Another default.yml fix.
* VFs-256, fix version o client and helpers in submodules
* VFS-256, add some comments for clarity
* VFS-256, revert unwanted changes
* VFS-256, introduce suggested fixes, more specs
* VFS-457 Fix default.yml.
* rule_definitions fix
* Initialization of cluster_rengine changed
* VFS-457 Apply recommended changes.
* Build fix
* getSubProcs available only in tests
* worker_host register_sub_proc handler merge subproclists simple test for this added in cluster_rengine_test_SUITE
* VFS-256, fix typo
* VFS-256, fix typo
* VFS-256, introduce suggested fixes
* VFS-489 Add io event data validation.
* VFS-489 Apply recommended changes.
* VFS-256, merge master
* VFS-516, merge with master
* VFS-457 Apply recommended changes.
* VFS-558: update wss connect opts
* VFS-471: improve update_meta_attr behaviour
* VFS-471: improve update_meta_attr behaviour
* VFS-471: improve update_meta_attr behaviour
* VFS-471: improve update_meta_attr behaviour
* VFS-553: ensure sync meta update
* VFS-553: fix assert_group_access
* VFS-553: fix typo
* VFS-553: fix typo
* VFS-553: fix tests VFS-553: update meck
* Verbose mode disabled in cluster_rengine_test
* VFS-553: add root user
* Initialization code in cluster_rengine_test fixed
* VFS-553: revert docs
* VFS-553: init refactor fslogic_utils
* VFS-553: move list_all_users to dao
* VFS-553: compile fix
* VFS-553: refactor get_new_file_location
* VFS-553: improve error handler
* VFS-553: refactor create_file_ack
* VFS-553: refactor renew_file_location
* VFS-553: refactor file_not_used
* VFS-553: refactor get_file_location
* VFS-553: refactor create_link
* VFS-553: refactor get_file_children
* VFS-553: refactor create_dir
* VFS-553: bug fixes
* VFS-553: init refactor rename_file
* VFS-553: refactor delete_file
* VFS-553: add fslogic_meta module
* VFS-553: some bug fixes and module rename
* VFS-553: refactor get_link
* VFS-553: refactor get_statfs
* VFS-553: refactor get_file_attr
* VFS-553: add fslogic_objects:save_file
* VFS-553: refactor change_file_perms
* VFS-553: fix ct test code path
* VFS-553: refactor change_file_group
* VFS-553: refactor change_file_owner
* VFS-553: refactor update_times
* VFS-553: fix all bugs to make CT tests pass
* VFS-553: minor fixes
* VFS-553: fix fslogic unit tests
* VFS-553: initial module reorganization
* VFS-457 Unregister default rules in cluster manager for test purposes.
* update deps
* VFS-489 Saving pair of storage_id and storage_helper_info in client session.
* fix CA cert key for ets store
* VFS-489 Delete unnecessary view.
* VFS-489 Saving client storage info in session.
* VFS-256, comment update and uncommentable define for nodes_manager verbosity
* VFS-489 Update veilhelpers.
* VFS-553: update deps
* VFS-256, fix typo
* chaneg submodules version
* display none when no teams are defined
* incorporate fixes to openid_utils
* incorporate fixes to openid_utils
* prevent n2o application from starting with cluster
* VFS-256, update in tests:
* VFS-256, update in tests:
* VFS-256, update in tests:
* VFS-256, merge submodules
* minor fix
* VFS-256, add compilation option for verbosity on starting apps on nodes during tests
* bigcouch update - delayed_commit
* VFS-256, remove unused apps from .app.src
* remove n2o from reltool for tests
* remove n2o from reltool for tests
* remove n2o from reltool for tests
* VFS-457 Apply recommended changes.
* VFS-256, remove unused app from nodes_manager
* VFS-256, move gui env configuration to control_panel.erl rather than sys.config
* VFS-256, remove debug
* VFS-256, fix problems with no session when downloading user files
* VFS-256, minor fixes
* VFS-532: fix typo
* VFS-489 Merge master branch.
* VFS-532: fix test CA cert
* VFS-532: strip self signed certs
* VFS-532: fix getting certificate extensions
* VFS-489 Temporary logging.
* VFS-532: use CA certs with SSL listeners VFS-532: add test sub CA certificate
* VFS-489 Refactoring.
* Veilclient updated
* VFS-489 Direct IO storage autodetection.
* Veilclient updated
* VFS-482, sub_proc update
* Veilclient updated
* Debug logs added
* VFS-256, change repo structure
* VFS-256, fix problems with logging
* VFS-489 Testing client read and write permissions on storage.
* VFS-489 Creating storage test file.
* veilclient updated
* df -h fix
* review notes applied
* Code for manual tests added
* typo fix
* Some of review notes added
* unnecessary logs deleted
* Truncate event for logical_files_manager added
* quota_check_freq changed in config
* various fixes and refactoring in cluster_rengine and related
* VFS-516, Acks from clients routing
* refactoring
* VFS-493 Add write permissions for CRL owner.
* typo fix
* setting ulimits for all starting nodes
* debug logs removed
* Refactoring
* VFS-457 Fix minor bug.
* VFS-457 Verbose tests.
* VFS-457 Verbose tests.
* VFS-457 Verbose tests.
* VFS-457 Set application variables.
* VFS-457 Distributed test added.
* VFS-457 Unit tests added.
* changes related to generic event message addition
* Generic event message added
* VFS-493 Zle uprawnienia dla plikow crl
* VFS-457 Redirect control panel port in cluster rengine distributed test.
* VFS-457 Fix port redirection in ct tests.
* VFS-457 Integration with cluster_rengine.
* VFS-514, minor update
* VFS-514, new cache template tests added
* VFS-514, new cache template updated
* VFS-514, new cache template added
* VFS-457 Verbose tests.
* dodane logi
* todos
* enable dd_write and multiple_ping_v2
* temporary disable dd_write and multiple_ping_v2
* fix situation when db or veil is not configured on host
* ct fix due to change in session cleanup
* -comment about err codes -"/opt/veil/" define in init.d script
* code cleanup
* bigcouch update
* disable init.d messages in installator setup
* some minor fixes
* status, restart, and force-reload implemented, adequate error codes and messages added
* fix debug marcos change default log level to info
* dodane logi do debugu
* [VFS-482] node_manager fix
* [VFS-482] redo unintentional change
* [VFS-482] all review notes applied
* VFS-506, adjust control_panel ports in multiple nodes test suites
* [VFS-482] Some of review notes applied
* [VFS-482] Some of review notes applied
* [VFS-482] Some of review notes applied
* [VFS-482] bamboo test fix
* [VFS-481] unneccessary view reload on rm_event fixed
* VFS-506, resolve port clash in crash test suite
* [VFS-482] production-ready values in default.yml
* [VFS-482] veilclient and veilprotocol updated
* [VFS-482] Full quota functionality before merge with master
* before merge
* VFS-506, get redirector port from config.yml
* VFS-506, add convenience function to change user role
* VFS-506, add convenience function to change user role
* VFS-506, add port 80 to required ports
* [VFS-482] general refactoring in cluster_rengine
* VFS-498, fix typo
* VFS-498, fix typo
* VFS-498, fix typo
* VFS-498, add cleanup after redirect listener
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-498, add redirect handler for redirection from http to https
* VFS-506, export new funs
* VFS-506, export new funs
* VFS-506, export new funs
* VFS-506, export new funs
* ping duration -> 240
* code cleanup
* VFS-517 Move 'LICENSE.txt' file to release files.
* VFS-506, add role field to user doc
* fix typo
* set autorefresh timer in file manager page to 1 second
* ping duration 60min
* temporary disable quiet ssh option, so we can obtain error message when connection can't be established
* quiet option added to ssh
* deleting session when its last connection is deleted
* fix connection not being closed during download error
* -hosts args changed to contain only info about host (user is always root), comment in bash script added
*  test fix
* fix connection not being closed during download error
* fix connection not being closed during download error
* docs small fix
* VFS-503, prevent from recursive deletion of groups dir
* VFS-503, prevent from recursive deletion of groups dir
* VFS-503, prevent from recursive deletion of groups dir
* VFS-503, prevent from recursive deletion of groups dir
* VFS-502, prevent from recursive deletion of groups dir
* full script
* veilclient updated
* Fixes
* [VFS-482] GUI is event producer
* Basic scenario works
* [VFS-482] ordinary ets instead of simple cache used in cluster_rengin
* session clearing bursts size: 1000
* Before merge with VFS-483
* ping test duration -> 30min, session clearing in bursts
* ping test duration -> 4h
* usr and grp management added
* script name changed to addusers
* removing deleted session fix , unwanted log removed
* broken pid temp fix 2
* [VFS-482] first simple version
* [VFS-482] commit before merge with VFS-441
* [VFS-441] veilprotocol and veilclient updated
* [VFS-483] some of review notes corrected
* VFS-256, introduce comet supervisor to terminate comet processes
* VFS-500, ensure no deadlock occurs in page_file_manager sync process
* Fixes for presentation
* various fixes
* VFS-500, minor update
* VFS-500, check_utf update
* VFS-500, unnecessary logs deleted
* VFS-500, functions description updated
* VFS-500, tests on
* ping duration ->1min, some logs added, broken pid temp fix
* VFS-500, shares updated
* [VFS-441] unit tests added
* ping duration ->2min
* interesting logs marked with &#35;&#35;, ct tests disabled
* VFS-500, ls update
* remotecall method changed to return error code in addition to result
* VFS-500, logs updated
* VFS-500, logs updated
* VFS-256, fix file download and upload to wor under n2o
* VFS-500, logical file manager can translate utf to proper format
* pid parsing error fix
* VFS-500, storage id generation updated
* [VFS-441] refactoring
* session expire time -> 10min
* command fix
* remotecall script: * -debug and -silent options implemented * added ability to use key pool when connecting
* remotecall generic script
* [VFS-441] refactoring
* VFS-500: revert filename utf-8 hack
* VFS-500: revert filename utf-8 hack
* [DAO] proper UTF-8 handling
* whitespace fix
* debug log change, session expiration time set to 10min
* Revert changes
* VFS-500, tests stoped
* VFS-500, logs added
* VFS-478 Cleanup gpv_nif.c
* VFS-478 Dopracowanie gpv_nif.
* bigcouch update (process limit raised)
* ports and process limit increased
* [VFS-441] unnecessary sleeps removed
* debug log
* [VFS-483] small fixes
* [VFS-483] functional version of send to all users fuses task for review
* 10 min
* bigcouch update(log lvl debug)
* 3h duration
* bigcouch update
* VFS-256, adjust openid tests to the new code
* VFS-256, rename some dirs in repo
* VFS-256, implement file downloading functionalities
* enable only ping + added debug log
* VFS-256, file manager page fully funcitonal except upload
* VFS-492, initial integration of file_manager page
* removing connection with no session_id fix
* VFS-457 Change gen_server timeout in modules_start_and_ping_test for ccm_and_dispatcher_test_SUITE.
* VFS-457 Change heart beat to 1 sec for some distributed tests.
* VFS-457 Return previous timeouts for distributed tests.
* useradd escript mock
* VFS-457 Change sleep time in nodes_manager:wait_for_cluster_cast function.
* VFS-457 Change timetrap interval.
* -Group dirs rollback removed -eunit fix -some additional minor changes
* ct tests hot fix, minor update
* [VFS-483] first version (does not work)
* ct tests hot fix
* fix to avoid unwanted metafiles in db after unsuccessful login attempt
* VFS-492, fixed team formatting for special chars
* VFS-492, fixed team formatting for special chars
* VFS-457 Update rrderlang application. (Error with 'NaN')
* ct test fix, gui error reporting added
* VFS-492, fixed team formatting for special chars
* VFS-492, fixed team formatting for special chars
* VFS-457 Verbose tests.
* ct test fix
* VFS-457 Verbose tests.
* VFS-457 Uncomment check_init assertion.
* VFS-457 Comment check_init assertion.
* eunit fix
* VFS-457 Change 'try-catch' logic in cluster manager in function 'calculate node load'.
* VFS-457 Add additional logging messages to cluster manager in map_node_stats_to_load function.
* VFS-457 Uncomment assertion in nodes_manager.
* Cleaning up created dirs and db files when user creation fails
* fix tests
* fix tests
* add polish grid CA
* add polish grid CA
* fix user dir chown
* VFS-256, all pages but file_manager adapted to n2o
* VFS-457 Lengthen 'wait_for_cluster_cast' and 'wait_for_cluster_init' timeouts
* VFS-457 Update distributed tests. Start/stop rrderlang application. Lengthen check load status interval.
* VFS-457 Change cluster initialization timeout for distributed tests.
* [VFS-441] veilclient updated (for integration tests)
* [VFS-441] refactoring
* VFS-455 Use store instead of stack for CLR storage.
* VFS-446, docs update
* VFS-446, minor update
* VFS-446, minor update
* VFS-446, minor update
* VFS-446, minor update after merge
* VFS-446, minor update
* VFS-461, Update of users' cache clearing
* VFS-457 Sending 'log_load' message from main process.
* VFS-455 Add crl_update_interval config option.
* VFS-421, Minor update
* VFS-455 Add support for certificate revocation lists.
* Add vcn_utils module for shared utility functions.
* [VFS-441] clients blocked when quota exceede
* VFS-457 Apply recommended changes.
* VFS-421, Minor update
* VFS-421, Refactoring of code
* [VFS-441] disabling client writes
* VFS-448: fix is_proxy_certificate/1 for legacy certs
* VFS-448: fix redSequence in is_proxy_certificate/1
* VFS-448: Add support for not RFC compliant proxy certs
* VFS-457 Merge master branch.
* VFS-457 Add RRD database to collect cluster statistical data.
* VFS-256, logs page functional
* [VFS-441] veilclient pull request
* VFS-461, users cache clearing update
* VFS-446, tests updated
* VFS-446, dispatcher asynch mode added
* tests configurations set to default values
* removing deps finally deleted
* removing deps temporary inserted again
* reverted removing deps
* reverted state change
* debug logs reverted
* basho_bench i websocket_client updated to master's revision
* 5min
* 60 min
* VFS-447, lager async_threshold configuration
* some debug logs
* ping duration -> 20min
* [VFS-441] client get event producer config on startup + refactoring
* ping duration -> 180min
* ping duration -> 30min
* removed deps cleanup
* ping duration -> 2min
* all turned off except ping (duration 20min)
* multiple_ping, limit_with_socket duration -> 2min
* turned off limit, limit_with_pb, limit_with_pb2
* testing previously unused tests
* VFS-256, manage_account works under n2o
* basho update(generate points in main graph)
* ping duration - 5min
* Merge branch 'master' into VFS-426-upgrade-srodowiska-do-stress-testow
* VFS-256, migrate several pages to n2o
* ping duration -> 180min, ping v2 disabled
* ping turned off, ping v2 turned on for 10 min duration
* VFS-256, logging in works on n2o
* VFS-256, n2o starts with cluster
* ping duration -> 120min
* ping duration -> 10min websocket client update(socket closing and handling handshake timeout)
* multiple ping v2 duration -> 120min
* changed websocket_terminate to include all close types
* multiple ping v2 turned off
* websocket client update (increased timeout of handshake), wss terminate socket log removed
* VFS-256, remove nitrogen files
* websocket client update (logs removed), wss logs removed
* ping, multiple ping v2  - duration 60min
* websocket_terminate fix 3
* websocket_terminate fix 2
* websocket_terminate fix
* test change in wss 3
* test change in wss 2
* test change in wss
* basho update ( graph fix 3)
* test change
* basho update (graphs another fix)
* client update (more logs)
* client update (log added, mask_payload new clause removed)
* basho update (graphs fix 2)
* basho update (graphs fix)
* sleep revert, time -> 2min, websocket_client update (new clause for mask_payload)
* test sleep
* dd - 0min, ping_v2 - 0min, ping - 10min
* times changed to 10min
* loop log removed form ping test, wrong error reason format fixed
* minor change in returned error reason
* loop log removed from dd test (v2)
* loop log removed from dd test
* duration: 60 basho update (cleaning up) ping_v2 driver cleanup
* VFS-446, high load test updated
* [VFS-441] protocol buffer messages for events improved
* VFS-446, high load test added
* [VFS-441] Tests fixes
* duration: 120
* websocket_client update
* duration 60
* sleep
* VFS-444 Merge load logs into summary log and plot summary graph.
* basho update(error reporting removed)
* comment in storage.cfg
* default storage config file + ability to handle it added to gen_dev script
* VFS-445 Delete unnecessary views from db.
* VFS-444 Revert unnecessary changes.
* Revert "VFS-444 Translate TODO information from polish to english."
* [VFS-441] presentation
* VFS-444 Minor change in Makefile.
* VFS-444 Clean deps for basho_bench.
* VFS-444 Translate TODO information from polish to english.
* Quick test fix
* VFS-444 Update basho bench repository tag.
* VFS-444 Change arguments for insert storage function in user_file_size_test for fslogic.
* concurent 30
* [VFS-441] merge problems
* Minor change in node_manager.
* VFS-444 Minor change in load_runner.escript.
* VFS-444 Apply required changes.
* [VFS-441] fixes
* basho update (unwanted logs removed)
* duration 120min, concurent 10
* VFS-444 Change load log fields names.
* VFS-444 Add header row and time column to load log.
* duration 180min
* basho update (loop log fix 4)
* basho update (loop log fix 3)
* basho update (loop log fix 2)
* basho update (loop log fix)
* loop log removed, basho update (loop log added)
* VFS-442 Modify load_runner.escript, so that now it can be run on worker node too.
* duration 2 min
* concurent 40
* basho update (log fix)
* basho update (number format changed)
* VFS-444 Modify stress tests durations.
* duration = 5min, concurent = 30
* basho update (cast -> call, some logs removed)
* basho update (remove latency stats generation)
* VFS-442 Add load_runner.escript and communicate with nodes.
* duration: 120min, basho update(cast, new log about window, some logs removed)
* VFS-442 Fix wrong code removal.
* VFS-442 Remove unnecessary db and storage cleaning code.
* VFS-442 Merge master branch.
* VFS-442 Logging current nodes load.
* VFS-428, change the way nitrogen_handler terminates in case of an error
* VFS-286, eunit tests update
* VFS-442 Minor update in fslogic distributed test.
* VFS-286, tests update
* VFS-442 Update functions in pull request.
* VFS-286, logs update
* VFS-286, logs update
* [VFS-441] little refactoring
* [VFS-413] simple manual test works
* duration: 180min, concurent 40
* duration: 30min
* report_interval 10s, concurent threads 100, error reporting: cast
* VFS-286, logs update
* VFS-444 Initial commit.
* VFS-286, logs update
* VFS-286, logs update
* VFS-442 Update fslogic distributed test.
* VFS-442 Minor update in distributed test.
* report_interval changed to 30s ( previous commit was only updating basho, my mistake)
* VFS-286, revision translation update
* VFS-286, logs update
* report_interval changed to 2s
* VFS-442 Update veilclient.
* VFS-442 Update veilhelpers and veilprotocol.
* VFS-442 Change getstatfs handle after adding answer filed to StatFSInfo message.
* report_interval changed to 2s
* basho_bench update
* VFS-442 Add periodical update of user files size view.
* basho_bench update
* basho_bench update
* basho_bench update
* VFS-286, logs update
* basho_bench update
* basho_bench update
* VFS-442 Udpate veilhelpers.
* VFS-442 Update veilprotocol.
* basho_bench update
* one small fix + try clause to prevent unexpected crash
* test duration increase
* basho_bench update
* docs directory cleanup
* lager version upgrade
* cleaning deps before building basho_bench
*  revert lager version update
* lager version update
* reverted random git tag
* basho_bench git tag changed to random for test purpose
* test duration changed
* basho update
* basho update
* VFS-286, tests update
* VFS-286, tests update
* basho update
* VFS-286, test db added
* ping_v2 config change
* VFS-286, preprocessor commands update
* VFS-286, ct tester update
* VFS-286, preprocessor commands update
* tmp basho_bench revert
* multiple_ping_v2 config chanage
* multiple_ping_v2 config chanage
* multiple_ping_v2 config chanage
* multiple_ping_v2 config chanage
* multiple_ping_v2 config chanage
* basho bench download from branch
* test time changed
* VFS-442 Update distributed tests for user files size and chown.
* VFS-442 Update user_logic unit test.
* VFS-442 Delete globus from documentation.
* VFS-442 Update veilprotocol.
* VFS-442 Getting user files size functions added.
* VFS-442 Update veilprotocol.
* VFS-442 Fix bug in dao_users.
* VFS-442 Adding quota document to database.
* [VFS-413] cluster_rengine uses subproc cache instead of adhoc solution
* [VFS-413] test refactoring
* [VFS-413] Tests corrected
* test time changed
* Nodes -> Erlang nodes
* - /mnt/veil -> /mnt/vfs - his files -> its files (suggested in jira comment to VFS-416) - returning to main menu after db start reverted
* setup actualization: -added default paths for storage configuration -some messages changed to be more explanatory -setup returns to main menu after starting db -interaction_get_string changed to provide default value when no input is given
* [VFS-413] fix
* Readme update
* [VFS-413] test fix
* [VFS-413] various fix
* update client
* fix integration tests
* update everything
* [VFS-413] tests refactoring
* update everything
* [VFS-413] tests are stable
* VFS-209, minor update
* VFS-209, comment to view added
* node_manager_test eunit fix, remote_files_manager_test refactoring
* reuseaddr flag added to port checking in cluster and installator, test refactoring
* VFS-209, remote_files_manager_test_SUITE update
* test commit with cleaning node_manager
* VFS-209, fslogic tests update
* VFS-406, revert unneeded changes
* VFS-406, make warning about logging dispatch problem global
* VFS-285, final stress test
* VFS-209, minor fslogic update
* VFS-406, prevent central_logger from going into infinite loop during logging failure
* VFS-406, prevent central_logger from going into infinite loop during logging failure
* VFS-285, dd test
* VFS-209, view update
* revert removal of dispatcher port checking
* VFS-406, fix typo
* rest_port in rest suite temporary changed
* VFS-406, fix typo
* VFS-285, dd test
* VFS-209, view update
* VFS-406, wrap log dispatching in a try-catch to identify errors easier
* VFS-285, test
* VFS-285, test
* VFS-285, test
* VFS-285, test
* VFS-209, not regular files creation update
* revert test sleep
* temporary change of control panel port in gui suite
* temporary remove of dispatcher_port checking
* VFS-285, test
* moving things around and some test debug changes
* submodules update
* VFS-209, minor update
* VFS-209, information about file storage status added
* VFS-285, ping driver test
* rest_port now is obtained from default.yml and replaces placeholders in docs files
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* unneeded map functions removed
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, concurency test
* VFS-285, ping driver test
* VFS-285, ping driver test
* typo fix
* error logging change
* VFS-285, ping driver test
* port update
* VFS-285, ping driver test
* login and group name for test user changed, so now system has such user ang group defined and can create dirs for them
* VFS-285, ping driver test
* -listing users now is done in parts, by giving N and Offset
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* dao_users: -list_users function added to dao (returns all users from db) user_logic: -some fixes in create_dirs_at_storage (it creates root dirs for  all users), including not raising error when some directory  already exists fslogic_storage: -insert_storage function now also creates on storage all root dirs  for existing users and groups fslogic_test_SUITE & verbose_distributed_tests.sh: -user_creation_test now also tests if dirs are created even when  storage is added later than users -new test storage added
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, concurency test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, drivers test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-285, ping driver test
* VFS-396, minor update
* create_dirs_at_storage function refactoring so we can call it with one storage as arg (previously it was possible only to create dirs for all storages)
* VFS-388 Add 'deps' dependency for docs and pdf rules in main Makefile.
* VFS-388 Minor changes in Makefiles.
* VFS-396, minor update
* VFS-396, tests update
* VFS-396, chown added to logical files manager
* VFS-402 Minor change in fslogic CT test.
* VFS-396, chown permissions check changed
* VFS-402 CT tests added. Minor changes in verify filename function.
* log update
* changed "initializing" to "out_of_sync"
* node_status now doesn't depend on status of workers
* handling and reporting timeouts during calls to: cmm, node_manager,  request_dispatcher and workers
* error log changed to warning, when heathchecking node during init
* reverting added rest_port env, because it's already included in master
* revert port changing in control_panel_test (it should work ok after merging with recent hotfix)
* VFS-402 Processing single and double dots in file names for remote_files_manager module.
* VFS-402 Processing single and double dots in file names for fslogic module.
* VFS-388 Changes in veilclient.
* VFS-388 Minor change in autodoc Makefile.
* VFS-388 Minor change in Makefile.
* -added "initializing" state (besides "ok" and "error") to node  health report -contains_error function changed to return true if some status  differs from "ok" and "initializing" -some documentation update
* VFS-388 Delete unnecessary xml files, which are now generated by doxygen.
* VFS-388 Changes in structure of README.md, so sphinx doesn't generate warnings.
* VFS-388 Changes in structure of README-DEVELOPER.md, so sphinx doesn't generate warnings.
* VFS-388 Minor change in Makefile.
* VFS-388 Fix in converting 'md' files to 'rst' files using pandoc.
* VFS-388 Remove unnecessary txt files. Converting 'md' files to 'rst' files using pandoc.
* VFS-376, fix rest listener not being stopped during cleanup
* VFS-401 prezentacja
* VFS-401 working basic tests
* [VFS-401] Aggregation works
* [VFS-401] Bugs in processing events corrected
* port numbers changing
* VFS-370, minor test update
* VFS-370, docs update
* closing open socket
* VFS-370, sub proc  cache clearing after error test added
* remove socket closing
* closing socket a bit later
* VFS-370, sub proc automatic cache clearing test added
* nagios test joined with connection test
* VFS-370, sub proc cache clearing test added
* [VFS-401] Server pushes messages to event producers
* VFS-375: update helpers
* info logs changed to debug
* VFS-374: reconfig stress tests
* closing open sockets in ccm_and_dispatcher_test
* VFS-374: reconfig stress tests
* VFS-375: fix compile
* VFS-370, sub proc cache clearing update
* VFS-375: fix compile
* VFS-374: update client
* -complete nagios_handler with healthchecking every node and worker -gui_test extended to gui_and_nagios_test -nagios_timeout value added to env
* VFS-374: update client
* VFS-374: update client
* VFS-375: fix compile
* VFS-374: update client
* VFS-374: update client
* VFS-370, node cache test added
* VFS-374: update client
* VFS-374: optimize connections init
* VFS-374: update client
* VFS-374: update client
* VFS-374: update client
* VFS-370, minor test update
* VFS-370, minor update
* VFS-374: update bigcouch
* VFS-374: update bigcouch
* VFS-356, ct tests minor update
* VFS-356, check o permissions of directories
* VFS-374: update submodules
* VFS-374: add veilclient submodule
* functions needed by nagios to perform healthcheck
* Tests for cluster_rengine improved
* VFS-370, sub processes cache clearing
* First working test for event subscription
* ccm_and_dispatcher_test handshake error code actualized
* VFS-342, revert unwanted changes
* VFS-342, revert unwanted changes
* VFS-342, update veil proto and helpers
* VFS-342, fix merge conflicts
* VFS-342, disable binary files stripping in rpm generation
* VFS-240 Change in pattern match for exist_record.
* VFS-356, ct tests minor update
* VFS-356, minor update
* VFS-356, minor changes after merge with master
* VFS-356, functions desciption update
* pierwsza dzialajaca wersja (testy manualne)
* changed handshake error to more specific (some additional changes to make it work properly)
* VFS-356, ct tests minor update
* VFS-356, ct tests update and umask setting
* VFS-356, ct tests update
* VFS-356, ct tests update
* VFS-356, permissions for new files changed
* VFS-356, eunit tests added
* VFS-304 Fix update time for root.
* VFS-240 exist_file method used in dao_vfs instead of get_file. Changed logging level for get_user error.
* submodules update
* VFS-356, minor corrections
* VFS-356, permissions checking added
* VFS-240 Exist method added to dao modules.
* VFS-359, Timeouts update
* changed handshake error to more specific
* VFS-351 Modification of Makefile, so that now 'make pdf' command generates documentation in pdf format.
* closing open sockets in fslogic suite & remote_files_manager suite
* closing open sockets in fslogic suite
* revert merge master into VFS-350 &#35; Please enter the commit message for your changes. Lines starting &#35; with '&#35;' will be ignored, and an empty message aborts the commit. &#35; On branch VFS-350 &#35; Changes to be committed: &#35;   (use "git reset HEAD <file>..." to unstage) &#35; &#35;	new file:   include/veil_modules/control_panel/openid.hrl &#35;	new file:   src/veil_modules/control_panel/file_transfer_handler.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/actions/dummy.txt &#35;	new file:   src/veil_modules/control_panel/gui_files/elements/bootstrap_button.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/elements/bootstrap_checkbox.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/elements/form.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/elements/veil_upload.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/css/bootstrap-docs.css &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/css/bootstrap-responsive.css &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/css/bootstrap.css &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/css/flat-ui.css &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/css/logs.css &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/css/prettify.css &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/css/style.css &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/fonts/Flat-UI-Icons.dev.svg &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/fonts/Flat-UI-Icons.eot &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/fonts/Flat-UI-Icons.svg &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/fonts/Flat-UI-Icons.ttf &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/fonts/Flat-UI-Icons.woff &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/fonts/icomoon-session.json &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/images/file32.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/images/file64.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/images/folder32.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/images/folder64.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/images/innow-gosp-logo.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/images/plgrid-plus-logo.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/images/spinner.gif &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/images/unia-logo.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/.empty &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/application.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/bootstrap-select.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/bootstrap-switch.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/bootstrap.min.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/flatui-checkbox.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/flatui-radio.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/html5shiv.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/icon-font-ie7.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/jquery-1.8.3.min.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/jquery-ui-1.10.3.custom.min.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/jquery.placeholder.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/jquery.stacktable.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/jquery.tagsinput.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/jquery.ui.touch-punch.min.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/js/veil_upload.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/bert.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/bert.min.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/fileupload/jquery.fileupload.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/fileupload/jquery.iframe-transport.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/fileupload/jquery.ui.widget.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/images/ui-bg_flat_0_aaaaaa_40x100.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/images/ui-bg_flat_75_ffffff_40x100.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/images/ui-bg_glass_55_fbf9ee_1x400.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/images/ui-bg_glass_65_ffffff_1x400.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/images/ui-bg_glass_75_dadada_1x400.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/images/ui-bg_glass_75_e6e6e6_1x400.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/images/ui-bg_glass_95_fef1ec_1x400.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/images/ui-bg_highlight-soft_75_cccccc_1x100.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/images/ui-icons_222222_256x240.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/images/ui-icons_2e83ff_256x240.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/images/ui-icons_454545_256x240.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/images/ui-icons_888888_256x240.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/images/ui-icons_cd0a0a_256x240.png &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery-ui/jquery.ui.all.css &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery.fileupload.min.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery.mobile.css &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery.mobile.icons.css &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery.mobile.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery.mobile.min.css &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery.mobile.min.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery.placeholder.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery.sparkline.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/jquery.sparkline.min.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/livevalidation.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/minify.sh &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/nitrogen.css &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/nitrogen.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/nitrogen.min.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/nitrogen_jqm.js &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/spinner.gif &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/nitrogen/spinner2.gif &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/templates/bare.html &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_static/templates/logs.html &#35;	new file:   src/veil_modules/control_panel/gui_files/gui_utils.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_contact_support.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_error.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_events.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_file_manager.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_index.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_login.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_logout.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_logs.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_manage_account.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_rules_composer.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_rules_simulator.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_rules_viewer.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_shared_files.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_system_state.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/page_validate_login.erl &#35;	new file:   src/veil_modules/control_panel/gui_files/web_404.erl &#35;	new file:   src/veil_modules/control_panel/nitrogen_handler.erl &#35;	new file:   src/veil_modules/control_panel/openid_utils.erl &#35;	new file:   src/veil_modules/control_panel/veil_multipart_bridge.erl &#35;	new file:   test/veil_modules/control_panel/openid_utils_tests.erl &#35;	new file:   test/veil_modules/control_panel/user_logic_tests.erl &#35;	new file:   test_distributed/control_panel_test.spec &#35;	new file:   test_distributed/control_panel_test/control_panel_test_SUITE.erl &#35;
* merge master into VFS-350
* VFS-292: update veilhelpers
* VFS-292: update veilhelpers
* VFS-292: update veilhelpers
* VFS-351 REST documentation updated.
* Pierwszy szkielete
* VFS-378, TODO added
* VFS-376, revert unwanted changes
* VFS-376, turn off verbose test mode
* VFS-376, minor fix in tests
* VFS-376, change REST port in tests
* VFS-376, change REST port to 8443
* VFS-378, minor update after merge
* VFS-378, minor test update
* VFS-378, minor test update
* VFS-378, Rest test update
* VFS-378, rpc call timeouts added
* VFS-378, gen_server calls timeouts update
* VFS-378, Tests update
* VFS-332, update year of creation in new files
* VFS-332, update veilproto to lastest
* VFS-332, checkout to proper veilproto version...
* reverting "its" -> "his" change
* VFS-351 Remove pandoc dependency in veilcluster autodoc.
* VFS-351 Remove pandoc dependency in veilcluster autodoc.
* VFS-351 Minor change in documentation Makefile.
* VFS-351 Superuser privilege for cp command.
* Add temporary .rst files converted from .md, so that now documentation is generated without using pandoc. Moreover generation of pdf is an option for Makefile and disabled by default.
* - ports_in_use now contains list of port env names, instead of concrete values - rest_port env added - checking ports disabled at ccm (due to conflicts with ccm + worker on the same host) - closing open sockets in crash_test_SUITE
* VFS-351 Convert statically .md files to .rst.
* VFS-351 Delete .rst files, try to generate them remotly.
* -ports_in_use env added to config (contains all needed ports except 433 because of problems with ct tests) -checking if ports_in_use are free during veil_cluster_node startup
* VFS-282, minor update
* VFS-282, docs update
* VFS-282, eunit test update
* VFS-282, udpate of cache clearing procedure and cache
* VFS-282, users cache update
* VFS-282, users cache added
* VFS-211 'Links' field added to map function in rest attrs.
* VFS-282, cache update
* VFS-282, cache clearing functions added
* VFS-211 Remove inserted storage.
* VFS-211 Display proper number of links for files.
* VFS-282, dao caches update
* ports_are_free function refactoring
* VFS-282, dao cache update
* checking free ports through socket opening
* BambooCheck, Test script update
* VFS-211 veilprotocol modified.
* VFS-282, ets update
* typo in text
* Checking whether 53, 443, 5555 ports are open
* VFS-282, ets names update
* VFS-341, Files management params update
* VFS-341, Functions description updated
* VFS-345, dns and test update
* storage config default path unified, dao:init_storage() return value fix
* VFS-345, dns update
* VFS-341, nodes manager update
* VFS-343, add '~/' prefix to link names (shared files)
* VFS-341, fslogic test uses verbose mode
* VFS-341, Test update
* VFS-341, Tests update
* Merge pull request &#35;151 in VFS/veilcluster from VFS-352 to master
* VFS-343, make page_shared_files always display full path of shared files
* VFS-341, test update
* VFS-341, cluster manager and test nodes manager update
* VFS-343, pull changes to file_transfer_handler from another branch
* Merge pull request &#35;151 in VFS/veilcluster from VFS-352 to master
* VFS-343, specs update, add logs when config is missing envs
* VFS-343, turn up multipart data size limit to 100GB
* VFS-343, update missing specs in openid_utils
* VFS-343, fix tests that included user creation
* VFS-343, fix tests that included user creation
* VFS-343, bring back standard initialization_time env
* VFS-343, fix user's dir creation process being not compatible with new teams format
* VFS-343, user's teams are now retrieved from OpenID using XML
* *error handling changed to be more specific *spec updated *setup small refactoring *storage backup removed
* getting storage_config_path throught environment variable, some changes in naming
* typo errors
* exiting with 1 exit code when batch installation fails
* -Get storage preferences from user during installation -Insert storage to db during first worker run -Backup existing storage configuration by every worker -Insert backed up storage info to db, during worker startup,  if it's missing
* reverting changes connected with port checking in veil_cluster_node, it needs more work with ct test and will be commited to other branch
* VFS-351 Minio change in veilcluster.py, so that now html tags are skipped in documentation.
* VFS-351 Spec for count_subdirs changed.
* -Port checking added to veil_cluster_node startup -Db hostname configuration moved from bigouch start script to installator
* VFS-240 exist_file function modified.
* VFS-351 Merge master branch.
* VFS-351 Changes in worker_host and dao_users.
* VFS-351 Structure of client files changed.
* VFS-351 Modification of some spec descriptions for erlang modules, so that they are now visible in documentation.
* VFS-351 Clean rule for documentation added in main project Makefile.
* VFS-351 Minor change in edoc spec.
* VFS-351 Minor change in edoc spec.
* VFS-351 Revert changes in code blocks for edoc.
* VFS-351 Temporarily turn off generation of pdf documentation, because pdflatex is not installed on Bamboo machine.
* VFS-351 Ignore python bytecode.
* VFS-351 Fix breathe extension import problem.
* VFS-351 Successful documentation generation in pdf format.
* VFS-240 Methods exist_record and exist_file added.
* VFS-351 Fixed Sphinx warnings.
* VFS-332, update veilproto
* VFS-332, update veilproto
* VFS-332, specs update
* VFS-332, add report_xxx functions, add content to all replies, fix tests
* VFS-351 Documentation updated.
* Bamboo test, test clearing update
* VFS-351 Documentation updated.
* VFS-351 Documentation updated.
* VFS-351 Documentation updated.
* BambooTest - fslogic verbose mode
* VFS-282, minor update
* VFS-292: revert unneeded changes
* VFS-282, docs update
* VFS-282, eunit test update
* VFS-282, udpate of cache clearing procedure and cache
* VFS-282, users cache update
* VFS-282, users cache added
* VFS-211 'Links' field added to map function in rest attrs.
* VFS-292: revert unneeded changes
* VFS-292: update protocol & helpers
* VFS-292: fix dao cache for fuse session
* VFS-282, cache update
* VFS-282, cache clearing functions added
* VFS-211 Remove inserted storage.
* VFS-211 Display proper number of links for files.
* VFS-282, dao caches update
* ports_are_free function refactoring
* VFS-282, dao cache update
* checking free ports through socket opening
* BambooCheck, Test script update
* VFS-211 veilprotocol modified.
* VFS-282, ets update
* typo in text
* Checking whether 53, 443, 5555 ports are open
* VFS-282, ets names update
* VFS-341, Files management params update
* VFS-341, Functions description updated
* VFS-345, dns and test update
* storage config default path unified, dao:init_storage() return value fix
* VFS-345, dns update
* VFS-341, nodes manager update
* VFS-332, change include path in rest_test_SUITE (it was invalid)
* VFS-343, add '~/' prefix to link names (shared files)
* VFS-341, fslogic test uses verbose mode
* VFS-341, Test update
* VFS-341, Tests update
* Merge pull request &#35;151 in VFS/veilcluster from VFS-352 to master
* Merge pull request &#35;147 in VFS/veilcluster from VFS-364 to master
* VFS-343, rename rest_messages to error_code and move up in hierarchy, split definitions of error codes and messages
* VFS-343, make page_shared_files always display full path of shared files
* VFS-341, test update
* VFS-341, cluster manager and test nodes manager update
* VFS-332, turn off verbose mode in rest_test
* VFS-343, pull changes to file_transfer_handler from another branch
* VFS-332, pull changes to file_transfer_handler from another branch to avoid conflicts
* VFS-332, add codes to rest messages
* Merge pull request &#35;151 in VFS/veilcluster from VFS-352 to master
* VFS-343, specs update, add logs when config is missing envs
* VFS-343, turn up multipart data size limit to 100GB
* VFS-343, update missing specs in openid_utils
* VFS-343, fix tests that included user creation
* VFS-343, fix tests that included user creation
* VFS-343, bring back standard initialization_time env
* VFS-343, fix user's dir creation process being not compatible with new teams format
* VFS-343, user's teams are now retrieved from OpenID using XML
* *error handling changed to be more specific *spec updated *setup small refactoring *storage backup removed
* Added checking if ports 55, 443 and 5555 are free.
* VFS-332, reset gen_dev.args
* VFS-332, externalized returned values to hrl file, tests update
* VSF-332, reworked callbacks and returned values in REST modules
* VFS-292: compile fix
* VFS-292: rest ssl renegotiate
* Merge pull request &#35;147 in VFS/veilcluster from VFS-364 to master
* VFS-332, reworked most callback in rest_module_behaviour
* VFS-332, changes in rest_module_behaviour
* VFS-292: update veilhelpers
* VFS-292: update veilhelpers
* VFS-292: update veilhelpers
* VFS-292: link libstdc++.a
* VFS-292: link libstdc++.a
* getting storage_config_path throught environment variable, some changes in naming
* VFS-292: docs
* typo errors
* exiting with 1 exit code when batch installation fails
* -Get storage preferences from user during installation -Insert storage to db during first worker run -Backup existing storage configuration by every worker -Insert backed up storage info to db, during worker startup,  if it's missing
* VFS-292: include openssl into release
* VFS-292: include openssl into release
* Merge pull request &#35;135 in VFS/veilcluster from VFS-146 to master
* Merge pull request &#35;145 in VFS/veilcluster from VFS-278 to master
* Merge pull request &#35;144 in VFS/veilcluster from VFS-313 to master
* Merge pull request &#35;142 in VFS/veilcluster from VFS-255 to master
* Merge pull request &#35;143 in VFS/veilcluster from VFS-327 to master
* Merge pull request &#35;140 in VFS/veilcluster from VFS-285 to master
* VFS-292: fix rpm generation
* Merge pull request &#35;139 in VFS/veilcluster from VFS-295 to master
* VFS-292: fix veilhelpers build dir name
* VFS-292: fix veilhelpers build dir name
* VFS-292: mem leak fix & update veilhelpers
* Merge pull request &#35;138 in VFS/veilcluster from VFS-269 to master
* Merge pull request &#35;136 in VFS/veilcluster from VFS-314 to master
* Merge pull request &#35;133 in VFS/veilcluster from VFS-321 to master
* Merge pull request &#35;134 in VFS/veilcluster from VFS-269 to master
* VFS-316: adjust cluster start wait times
* Merge pull request &#35;132 in VFS/veilcluster from VFS-323 to master
* VFS-292: update helpers
* Merge pull request &#35;131 in VFS/veilcluster from VFS-297 to master
* Merge pull request &#35;130 in VFS/veilcluster from VFS-298 to master
* Merge pull request &#35;127 in VFS/veilcluster from VFS-277 to master
* Merge pull request &#35;129 in VFS/veilcluster from VFS-288 to master
* Merge pull request &#35;128 in VFS/veilcluster from VFS-291 to master
* Merge pull request &#35;125 in VFS/veilcluster from VFS-271 to master
* Merge pull request &#35;123 in VFS/veilcluster from VFS-271 to master
* Merge pull request &#35;121 in VFS/veilcluster from VFS-270 to master
* Merge pull request &#35;120 in VFS/veilcluster from VFS-274 to master
* Merge pull request &#35;119 in VFS/veilcluster from VFS-268 to master
* Merge pull request &#35;116 in VFS/veilcluster from VFS-247-fin to master
* Merge pull request &#35;118 in VFS/veilcluster from VFS-264 to master
* Merge pull request &#35;117 in VFS/veilcluster from VFS-272 to master
* Merge pull request &#35;115 in VFS/veilcluster from VFS-276 to master
* Merge pull request &#35;114 in VFS/veilcluster from VFS-261 to master
* Merge pull request &#35;112 in VFS/veilcluster from VFS-258 to master
* Merge pull request &#35;111 in VFS/veilcluster from VFS-181 to master
* Merge pull request &#35;110 in VFS/veilcluster from VFS-181 to master
* Merge pull request &#35;108 in VFS/veilcluster from VFS-246 to master
* Merge pull request &#35;109 in VFS/veilcluster from VFS-251 to master
* Merge pull request &#35;106 in VFS/veilcluster from VFS-251 to master
* Merge pull request &#35;107 in VFS/veilcluster from VFS-158 to master
* Merge pull request &#35;105 in VFS/veilcluster from VFS-226 to master
* Merge pull request &#35;104 in VFS/veilcluster from VFS-225 to master
* Merge pull request &#35;103 in VFS/veilcluster from VFS-158 to master
* Merge pull request &#35;102 in VFS/veilcluster from VFS-158 to master
* Merge pull request &#35;101 in VFS/veilcluster from VFS-158 to master
* Merge pull request &#35;99 in VFS/veilcluster from VFS-167 to master
* Merge pull request &#35;98 in VFS/veilcluster from VFS-190 to master
* Merge pull request &#35;96 in VFS/veilcluster from VFS-168 to master
* Merge pull request &#35;94 in VFS/veilcluster from VFS-167 to master
* Merge pull request &#35;95 in VFS/veilcluster from VFS-169 to master
* Merge pull request &#35;93 in VFS/veilcluster from VFS-170 to master
* Merge pull request &#35;92 in VFS/veilcluster from VFS-167 to master
* Merge pull request &#35;91 in VFS/veilcluster from VFS-172 to master
* Merge pull request &#35;90 in VFS/veilcluster from VFS-162 to master
* Merge pull request &#35;89 in VFS/veilcluster from VFS-151 to master
* Merge pull request &#35;88 in VFS/veilcluster from VFS-159 to master
* Merge pull request &#35;86 in VFS/veilcluster from VFS-154 to master
* Merge pull request &#35;85 in VFS/veilcluster from VFS-156_2 to master
* Merge pull request &#35;84 in VFS/veilcluster from VFS-155 to master
* Merge pull request &#35;83 in VFS/veilcluster from VFS-144 to master
* Merge pull request &#35;82 in VFS/veilcluster from VFS-150 to master
* Merge pull request &#35;81 in VFS/veilcluster from VFS-137 to master
* Merge pull request &#35;80 in VFS/veilcluster from VFS-147_2 to master
* Merge pull request &#35;79 in VFS/veilcluster from VFS-127 to master
* Merge pull request &#35;78 in VFS/veilcluster from VFS-145 to master
* Merge pull request &#35;77 in VFS/veilcluster from VFS-95 to master
* Merge pull request &#35;76 in VFS/veilcluster from VFS-138 to master
* Merge pull request &#35;75 in VFS/veilcluster from VFS-144 to master
* Merge pull request &#35;74 in VFS/veilcluster from VFS-139 to master
* Merge pull request &#35;73 in VFS/veilcluster from VFS-141 to master
* Merge pull request &#35;72 in VFS/veilcluster from VFS-108-cont to master
* Merge pull request &#35;71 in VFS/veilcluster from VFS-142 to master
* Merge pull request &#35;70 in VFS/veilcluster from VFS-132 to master
* Merge pull request &#35;69 in VFS/veilcluster from VFS-132 to master
* Merge pull request &#35;68 in VFS/veilcluster from VFS-136 to master
* Merge pull request &#35;67 in VFS/veilcluster from VFS-133 to master
* Merge pull request &#35;66 in VFS/veilcluster from VFS-134 to master
* Merge pull request &#35;64 in VFS/veilcluster from VFS-115 to master
* Merge pull request &#35;65 in VFS/veilcluster from VFS-114 to master
* Merge pull request &#35;63 in VFS/veilcluster from VFS-115 to master
* Merge pull request &#35;59 in VFS/veilcluster from VFS-113 to master
* Merge pull request &#35;62 in VFS/veilcluster from VFS-108 to master
* Merge pull request &#35;61 in VFS/veilcluster from VFS-129 to master
* Merge pull request &#35;58 in VFS/veilcluster from VFS-97 to master
* VFS-109, removed negative length problem
* VFS-100, Minor updates after merge
* VFS-100, Minor updates after merge
* VFS-100, Merge with branch
* VFS-100, Minor updates after merge
* VFS-100, Merged with master
* VFS-100, udpate of functions' description
* VFS-100, dns tests update
* VFS-100, dispatcher update procedure changed
* VFS-100, dispatcher update, tests of dispacher added
* VFS-100, dispatcher update
* VFS-23, minor fixes
* VFS-23, minor fixes
* VFS-23, minor fixes
* VFS-23, added veilprotocol to .gitignore
* VFS-23, added openid.hrl to git
* VFS-23, another env_setter fix
* VFS-23, added ibrowse to env_setter
* VFS-23, initial commit
* VFS-100, CCM tests update
* VFS-100, CCM tests update
* VFS-100, CCM tests update
* VFS-100, DNS update, DNS tests update
* VFS-101, Minor update
* VFS-101, updated env_setter to comply with new app.src
* VFS-101, Gen_dev.args updated
* VFS-101, Merged with master, confilicts resolved
* VFS-101, functions description updated
* VFS-101, automatic cleaning of old descriptors added
* VFS-100, update of cluster check procedure
* VFS-100, update of dispatcher
* VFS-82: Change to crypto:hash_* instead crypto:md5_* bacause those methods were deprecated
* VFS-82: list_descriptors 'by_expired_before'
* VFS-60, dao start procedure updated
* VFS-60, dns and loger start procedure changed
* VFS-102, changed default.yml syntax a bit
* VFS-60, behaviour of node manager of ccm updated
* VFS-60 init procedure updated
* VFS-60, scripts now use make compile rather than ./rebar compile
* VFS-60, gen_dev now creates a worker for every ccm node
* VFS-60, application start procedure changed
* VFS-102, minor fixes
* VFS-102, minor fixes
* VFS-102, makefile update (gen_config added to compile target)
* VFS-100, cluster load balancing algorithm added (dynamic starting and stopping of nodes)
* VFS-102, all configuration files are now located in /config. Their target position (after the script was run) is gitignored
* VFS-102, all configuration files are now located in /config. Their target position (after the script was run) is gitignored
* VFS-100, dns update
* VFS-100, new dns update procedure
* VFS-102, working script, configuration files are in /config
* VFS-100, node monitoring system ready
* VFS-100, monitoring app integrated
* VFS-100, node manager updated
* VFS-101, getting nodes statistics added
* VFS-100, dispatcher update
* dependencies updated
* dependencies updated
* VFS-101, getfilechildren updated
* VFS-101, renewfilelocation updated
* VFS-101, renewfilelocation updated
* VFS-101, directory creation procedure updated
* VFS-101, fslogic descriptors management update
* VFS-101, fslogic descriptors management update
* VFS-101, fslogic descriptors management update
* VFS-70: Terminate_child replaced with delete_child and adjusted time in nodes_monitoring_and_workers_test_SUITE.
* VFS-70: DNS no longer starts on every node.
* VFS-70: Fixed url in doc.
* VFS-70: Added missing tests for dns_utils and dns_worker.
* VFS-70: Implemented dns worker, dns handlers and tests for added functionality.
* VFS-82: revert gen_dev.args
* VFS-82: cleanup
* VFS-82: refactor: dao_helper:parse_view_result -> dao:list_records && worker_host:handle_info state match fix
* VFS-81, Env_setter update
* VFS-81, Merge with master conflicts resolved
* VFS-81, files description update
* VFS-81, makefile updated
* VFS-81, makefile updated
* VFS-81, makefile updated
* veilprotocil version updated
* VFS-81, more fuse requests handled
* VFS-81, more fuse requests handled (ls is not working yet)
* VFS-82: fix typo
* VFS-82: test fix
* VFS-82: doc update
* VFS-82: remove_descriptor update
* VFS-82: test fix
* VFS-82: unused export
* VFS-82: list_descriptors by file name and owner
* VFS-81, getting files location works
* VFS-81, Dispatcher is able to decode nested messages
* VFS-92: Readme update
* VFS-92: Ignore error codes returned by dialyzer
* VFS-92: Dialyzer
* VFS-92: fix typo
* VFS-92: Some fixes that was needed to make dizlyzer work
* VFS-92, script that starts distribted tests updated (cherry picked from commit f53dd9f)
* VFS-92: rebar cleanup ct path updated
* VFS-69 + VFS-83, cosmetic changes
* VFS-69 + VFS-83, cosmetic changes
* VFS-94, script that starts distribted tests updated
* VFS-73, includes in tests updated
* VFS-73, Env_setter update.
* VFS-73, Env_setter description update
* VFS-73, Files description updated.
* VFS-82: dao_vfs_tests &#35;7
* VFS-82: dao_vfs_tests &#35;6
* VFS-82: dao_vfs_tests &#35;5
* VFS-82: dao_vfs_tests &#35;4
* VFS-82: dao_vfs_tests &#35;3
* VFS-82: dao_vfs_tests &#35;2
* VFS-82: dao_vfs_tests &#35;1
* VFS-82: dao_vfs_tests init
* VFS-73, tests description added
* VFS-94, application tests moved from eunit to ct
* VFS-73, Cluster tester updated
* VFS-94, nodes synchronization during distributed tests added
* VFS-94, All modules tests updated
* VFS-94, most of tests updated
* VFS-82: root dir perms fix (cherry picked from commit 79b715b)
* VFS-82: Typo fix (cherry picked from commit 1d80151)
* VFS-82: root dir perms fix
* VFS-82: Typo fix
* VFS-82: Zmiana typu path()
* VFS-82: update pol &#35;file_descriptor
* VFS-69 + VFS-83, moved central_logging_backend to better location in repo
* VFS-69 + VFS-83, several fixes
* VFS-69 + VFS-83, first candidate for pull request
* VFS-92: surefire ct
* VFS-92: surefire ct
* VFS-92: Struktura plikow surefire
* VFS-92: Test coverage
* VFS-92: Opcja -noshell przy takim sposobie odpalania distributed testow jest konieczne jesli maja one dzialac poprzez Bamboo (cherry-picked from e49013d)
* VFS-92: surefire: zmiana sciezki (cherry-picked from de2b102)
* VFS-92: noshell w CT (cherry-picked from 5b555bf)
* VFS-92: Maven-like formatowanie w eunit (cherry-picked from 3315dd7)
* VFS-81, fslogic and dao cooperation tests
* VFS-82: dao_hosts_test teardown cleanup
* VFS-82: cleanup doa_hosts_test fix
* VFS-82: fix typo
* VFS-82: dao tests cleanup + test coverage
* VFS-82: typo fix
* VFS-82: dao_lib:apply/5 usuniete
* VFS-81, dispatcher may use MessageID
* VFS-82: dao_lib:apply/5 przechodzi w pelni przez dispatchera
* VFS-73, cluster_test uses many processes
* VFS-73, cluster_test_updated
* VFS-73, nodes monitoring distributed test added.
* VFS-73, distributed test update.
* Revert "Revert "VFS-82: worker_host proxy call dla cast'ow"" Ups :(
* VFS-82: Revert: Zmiany dotyczace testow/bamboo przeniesione do osobnego brancha
* Revert "VFS-82: worker_host proxy call dla cast'ow"
* VFS-82: Opcja -noshell przy takim sposobie odpalania distributed testow jest konieczne jesli maja one dzialac poprzez Bamboo
* VFS-85, Makefile updated
* VFS-82: surefire: zmiana sciezki
* VFS-85, Makefile updated
* VFS-82: noshell w CT
* VFS-85, proto dir removed
* VFS-85, submodule uses ssh
* VFS-82: Maven-like formatowanie w eunit
* VFS-73, Distributed tests reorganized
* VFS-73, Distributed tests reorganized
* VFS-73, VeilFS application starts in distributed test
* VFS-82: worker_host proxy call dla cast'ow
* VFS-82: Szybsze dao_lib:apply
* VFS-82: New: dao_lib:apply/5 - proste blokujace wykonywanie zapytan do DAO. Abstrakcja dla gen_server:call.
* VFS-82: dao_vfs:get_record/1 duplicate file handling
* VFS-82: revert dao:init/1
* VFS-82: DAO potrafi aktualizowac design dokumenty przy starcie. w razie potrzeby nie bedzie zadnego problemu aby uruchamiac odpowienia metode kiedy dusza zapragnie.
* VFS-82: typo fix
* VFS-82: Stale uprawnien
* VFS-82: Usuniety niepotrzebny define
* VFS-82: dao_vfs:list_dir/3 zwraca [&#35;veil_document] zamiast [&#35;file]
* VFS-82: dao_vfs:get_descriptor/1 return type check
* VFS-82: dao_vfs:get_file/1 return type check
* VFS-82: docs part &#35;3
* VFS-82: docs part &#35;2
* VFS-82: dao_lib update
* VFS-82: docs part &#35;1
* VFS-82: Cleanup headerow
* VFS-82: Nowa struktura file_location
* VFS-82: update testow
* VFS-82: Pierwsza dzialajaca wersja dao_vfs
* VFS-82: Normalizacja revision info (BigCouch...)
* VFS-82: Wstepna implementacja dao_vfs
* VFS-82: fix generatora widokow - duplikaty
* VFS-82: Dodanie widoku fd_by_file cd
* VFS-82: Dodanie widoku fd_by_file. To takie proste ;)
* VFS-82: Pobieranie pelnych informacji o dokumencie prosto z widoku
* VFS-82: obluga include_docs przy parsowaniu widoku
* VFS-82: Formatowanie widokow
* VFS-82: Fix uuid generation
* VFS-82: Zmiania logistyki przechowywania informacji o widokach
* VFS-82: Przelacznik baz banych
* VFS-82: cleanup
* VFS-82: Inicjalizacja struktury databases -> designs -> views przy starcie workera DAO
* VFS-82: zmiena wersji
* VFS-82: New: View management + usprawnienia w dzialaniu dao_hosts + pierwsze widoki
* VFS-82: New: dao_helper:open_design_doc/2, dodanie numeru wersji do Desing Documentow
* VFS-82: typo fix
* VFS-82: lager fix
* VFS-82: Refactor: Nazwa wrapera rekordow
* VFS-82: Init commit: Poczatkowe deklaracje rekordow oraz lekkie zwiekszenie mozliwosci save_record/1
* VFS-39: Definicje struktur vfs


### v0.0.6

* VFS-78: ccm infinite loop fix
* VFS-77, tests update
* VFS-67, config files update
* VFS-67, heartbeat update
* VFS-77: Zmiana wersji + aktualizacja testow
* VFS-77: Polaczenie SSL obslugiwane pakietowo
* VFS-77: Poprawa zapisu stanu workerow
* VFS-67, logs are gathered in one place
* VFS-67, configs test
* VFS-67, test starting script updated
* VFS-67, sample test created
* VFS-66: Łapanie wyjatku zamiast zapobiegania
* VFS-67, starter script for distributed test added
* VFS-66: monitoring_proc moze nie istniec (race condition)
* VFS-65, VFS-49: reltool.config update
* VFS-49: Zmiana return value w control_panel:init/1
* VFS-65: Wiekszy timeout przy operacjach na tabelach ETS
* VFS-65: Poprawka reltool.config + reformat kodu
* VFS-65: fix typo
* VFS-65: cowboy, nprocreg startuja razem z veil_cluster_node. Rozwiazanie tymczasowe
* VFS-65: Obsluga callback'ow 'terminate' w gen_server'ach
* VFS-65: BugFix
* VFS-66, environment variable for hot swapping configuration added
* VFS-66, minor update
* VFS-66, code hode swapping works for all modules
* VFS-66, code updates work
* VFS-66, hot swapping tests
* VFS-66, hot swapping tests
* VFS-65: Bezpieczniejsze uruchamianie DAO w testach
* VFS-65: Dodatkowy cleanup po testach
* VFS-65: Zmiana wersji
* VFS-65: Zbanowany host ktory odpowiedzial na request powinien byc reaktywowany
* VFS-65: Logowanie na poziomie "error"
* VFS-65: Stan DAO zarzadzany poprzez gen_server -> DAO nie zawiera zadnych "zywych" elementow
* VFS-65: Dodana mozliwosc wysylania sekwencyjnych komunikatow do modulow
* VFS-66, veil_modules use environment variable
* VFS-49, Zmiana nazewnictwa plikow / katalogow na bardziej przejrzyste. Pliki statyczne laduja w 'gui_static' w paczce.
* VFS-66, ccm gathers information about workers' versions
* VFS-67, ct tests
* VFS-49, Integracja CMT w najprostszym mozliwym wydaniu. Strona (na razie pusta) dostepna na porcie :8000 node'a, na którym działa control_panel.
* VFS-49, Integracja CMT w najprostszym mozliwym wydaniu. Strona (na razie pusta) dostepna na porcie :8000 node'a, na którym działa control_panel.
* VFS-67, ct tests
* VFS-67, ct tests
* VFS-49: Integracja CMT w najprostszym mozliwym wydaniu. Strona (na razie pusta) dostepna na porcie :8000 node'a, na którym działa control_panel.
* VFS-49: Integracja CMT w najprostszym mozliwym wydaniu. Strona (na razie pusta) dostepna na porcie :8000 node'a, na którym działa control_panel.
* VFS-68, readme update
* VFS-67, ct tests
* VFS-49: Integracja CMT w najprostszym mozliwym wydaniu. Strona (na razie pusta) dostepna na porcie :8000 node'a, na którym działa control_panel.
* VFS-49: Integracja CMT w najprostszym mozliwym wydaniu. Strona (na razie pusta) dostepna na porcie :8000 node'a, na którym działa control_panel.
* VFS-49: Integracja CMT w najprostszym mozliwym wydaniu. Strona (na razie pusta) dostepna na porcie :8000 node'a, na którym działa control_panel.
* VFS-68, protocol buffer integrated
* PingTest, heartbeat upgrade
* PingTest, manual tester added
* PingTest, minor changes
* VS-40, jeszcze poprawka
* VS-40, zmiany zgodnie z ustaleniami - paczki testowe trafiaja do releases/test_cluster. Głowny skrypt siedzi w bin/veil_cluster i ma argumenty w config.args
* VS-40, jeszcze jeden update vars.config
* VS-40, rozwiązane konflikty
* VFS-40, zmiana podejścia, kompletne przepisanie skryptow gen_dev i gen_test, usuniecie niepotrzebnych juz plikow, update makefile i readme.
* VFS-55, doc udpate
* VFS-55, cluster uses ssl
* VFS-55, dispatcher answers using protocol buffers
* VFS-55, protocol buffers for answer created
* VFS-55, request to workers translated with protocol buffer
* VFS-55, protocol buffers integrated
* VFS-55, tests for dispatcher added
* VFS-55, communication between cluster_manager and dispatcher added.
* VFS-55, dispatcher connected to ranch.
* VFS-55, Ranch integrated.
* VFS-40, edycja readme ...
* VFS-40, edycja readme ...
* VFS-40, edycja readme ...
* VFS-40, edycja readme ...
* VFS-40, edycja readme ...
* VFS-40, edycja readme ...
* VFS-40, edycja readme ...
* VFS-40, edycja readme ...
* VFS-40, edycja readme ...
* VFS-40, skrypt apply_config przerobiony zgodnie z wymogami, brakuje jeszcze opisu w readme
* VFS-40, dodanie do gen_dev ustawiania ciasteczka rownego IP maszyny. vars.config jest teraz przywracane do orginalnej postaci po generacji jak reltool.config poprzez gen_den -clean_up.
* VFS-40, poprawki
* VFS-40, dodalem do skryptow podawanie wezlow DBMS w argumentach,zupdatowalem README
* VFS-40, nowy skrypt do konfigurowania paczki releasowej, (/releases/config/), drobny update Makefile i innych plikow
* Drobne poprawki w gen_test.
* VFS-40, dodalem do skryptow podawanie wezlow DBMS w argumentach,zupdatowalem README
* Strona nie powala, w control_panel_SUITE znajduje sie podwalina pod generyczne testy gui / cometa. make website_test aby postawic strone na localhost:8000.
* Wrzucam co mi sie udalo zrobic: strona startuje jako modul, podczas trwania testu mozna przez przegladarke na porcie 8000 ja przegladac. Niestety nie dzialaja updaty cometowe i nie wiem o co chodzi.
* Testuje sobie obsluge Gita w IntelliJ
* Testuje sobie obsluge Gita w IntelliJ
* Testuje sobie obsluge Gita w IntelliJ
* Testuje sobie obsluge Gita w IntelliJ
* Testuje sobie obsluge Gita w IntelliJ
* Testuje sobie obsluge Gita w IntelliJ
* Testuje sobie obsluge Gita w IntelliJ
* Testuje sobie obsluge Gita w IntelliJ
* Testuje sobie obsluge Gita w IntelliJ
* Testuje sobie obsluge Gita w IntelliJ
* Zmienilem plik "dummy.txt"
* Zmienilem plik "dummy.txt"
* Zmienilem plik "dummy.txt"


### v0.0.5

* VFS-62, update of comments, version check added to modules
* VFS-62, Ping-pong test added
* VFS-54, Cluster state number management added.
* VFS-54, Tests of cluster manager initialization
* VFS-62, cluster initialization upgrade
* version 0.0.5
* VFS-54, Update of tests and documentation
* VFS-54, Merging cluster state of ccm and state from db
* VFS-54, Saving cluster state update
* VFS-61: Hotfixy: zwracane wartosci w get_record
* VFS-54, Comment updated
* VFS-61: Hotfixy: update testow
* VFS-61: Hotfixy: obsługa PID'ów w save_record + dao:init/1 czeka na pelne uruchomienie modulu
* VFS-54, Cluster state write to db
* VFS-54, Cluster state readed from db
* Doc to worker_host added.


### v0.0.4



### v.123

* Release v.123


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
