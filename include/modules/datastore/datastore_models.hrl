%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Models definitions. Extends datastore models.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_SPECIFIC_MODELS_HRL).
-define(DATASTORE_SPECIFIC_MODELS_HRL, 1).

-include("modules/datastore/qos.hrl").
-include("modules/events/subscriptions.hrl").
-include("modules/fslogic/fslogic_delete.hrl").
-include("modules/storage/luma/luma.hrl").
-include("modules/fslogic/file_attr.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models.hrl").

-type file_descriptors() :: #{session:id() => non_neg_integer()}.

% Graph Sync cache metadata, common for all od_* records (synchronized by
% Graph Sync).
-type cache_state() :: #{
    % maximum scope that is currently cached
    scope => gs_protocol:scope(),
    % revision (version of the record that increments with every update)
    revision => gs_protocol:revision(),
    % connection identifier to verify if the cache is not outdated.
    connection_ref => pid()
}.

%%%===================================================================
%%% Records synchronized via Graph Sync
%%%===================================================================

% Records starting with prefix od_ are special records that represent entities
% in the system and are synchronized to providers via Graph Sync.
% The entities can have various relations between them, especially effective
% membership is possible via groups and nested groups.
% these records are synchronized from OZ via Graph Sync.
%
%
% The below ASCII visual shows possible relations in entities graph.
%
%           provider
%              ^
%              |
%           storage                              share
%              ^                                   ^
%              |                                   |
%            space            handle_service<----handle
%           ^ ^ ^ ^             ^         ^       ^  ^
%          /  | |  \           /          |      /   |
%         /   | |   \         /           |     /    |
%        /   /   \   \       /            |    /     |
%       /   /     \   \     /             |   /      |
% share user harvester group             user      group
%              ^    ^     ^                          ^
%             /      \    |                          |
%            /        \   |                          |
%          user        group                        user
%                      ^   ^
%                     /     \
%                    /       \
%                  user      user
%

-record(od_user, {
    full_name :: undefined | binary(),
    username :: undefined | binary(),
    emails = [] :: [binary()],
    linked_accounts = [] :: [od_user:linked_account()],

    % Along with the value of 'blocked' user parameter, stores information
    % whether the value has changed during the last received user doc update
    % The '{undefined, unchanged}' value is set for shared user scope.
    blocked = {undefined, unchanged} :: {undefined | boolean(), changed | unchanged},

    % List of user's aliases for spaces
    space_aliases = #{} :: #{od_space:id() => od_space:alias()},

    % Direct relations to other entities
    eff_groups = [] :: [od_group:id()],
    eff_spaces = [] :: [od_space:id()],
    eff_handle_services = [] :: [od_handle_service:id()],
    eff_handles = [] :: [od_handle:id()],

    cache_state = #{} :: cache_state()
}).

%% Local, cached version of OZ group
-record(od_group, {
    name :: undefined | binary(),
    type = role :: od_group:type(),

    cache_state = #{} :: cache_state()
}).

%% Model for caching space details fetched from OZ
-record(od_space, {
    name :: undefined | binary(),

    owners = [] :: [od_user:id()],

    direct_users = #{} :: #{od_user:id() => [privileges:space_privilege()]},
    eff_users = #{} :: #{od_user:id() => [privileges:space_privilege()]},

    direct_groups = #{} :: #{od_group:id() => [privileges:space_privilege()]},
    eff_groups = #{} :: #{od_group:id() => [privileges:space_privilege()]},

    storages = #{} :: #{storage:id() => Size :: integer()},

    % This value is calculated after fetch from zone for performance reasons.
    % Contains an empty map if the provider does not support the space.
    storages_by_provider = #{} :: #{od_provider:id() => #{storage:id() => storage:access_type()}},

    providers = #{} :: #{od_provider:id() => Size :: integer()},

    shares = [] :: [od_share:id()],

    harvesters = [] :: [od_harvester:id()],

    cache_state = #{} :: cache_state()
}).

%% Model for caching share details fetched from OZ
-record(od_share, {
    name :: binary(),
    description :: binary(),
    public_url :: binary(),
    public_rest_url :: binary(),

    % Direct relations to other entities
    space = undefined :: undefined | od_space:id(),
    handle = undefined :: undefined | od_handle:id(),

    root_file :: binary(),
    file_type :: od_share:file_type(),

    cache_state = #{} :: cache_state()
}).

%% Model for caching provider details fetched from OZ
-record(od_provider, {
    name :: undefined | binary(),
    admin_email :: undefined | binary(),
    subdomain_delegation = false :: undefined | boolean(),
    domain :: binary(),
    subdomain = undefined :: undefined |  binary(),
    latitude = 0.0 :: float(),
    longitude = 0.0 :: float(),
    online = false :: boolean(),

    % Direct relations to other entities
    storages = [] :: [storage:id()],

    % Effective relations to other entities
    eff_spaces = #{} :: #{od_space:id() => Size :: integer()},
    eff_users = [] :: [od_user:id()],
    eff_groups = [] :: [od_group:id()],

    cache_state = #{} :: cache_state()
}).

%% Model for caching handle service details fetched from OZ
-record(od_handle_service, {
    name :: od_handle_service:name() | undefined,

    % Effective relations to other entities
    eff_users = #{} :: #{od_user:id() => [privileges:handle_service_privilege()]},
    eff_groups = #{} :: #{od_group:id() => [privileges:handle_service_privilege()]},

    cache_state = #{} :: cache_state()
}).

%% Model for caching handle details fetched from OZ
-record(od_handle, {
    public_handle :: od_handle:public_handle(),
    resource_type :: od_handle:resource_type() | undefined,
    resource_id :: od_handle:resource_id() | undefined,
    metadata :: od_handle:metadata() | undefined,

    % Direct relations to other entities
    handle_service :: od_handle_service:id() | undefined,

    % Effective relations to other entities
    eff_users = #{} :: #{od_user:id() => [privileges:handle_privilege()]},
    eff_groups = #{} :: #{od_group:id() => [privileges:handle_privilege()]},

    cache_state = #{} :: cache_state()
}).

-record(od_harvester, {
    indices = [] :: [od_harvester:index()],
    spaces = [] :: [od_space:id()],
    cache_state = #{} :: cache_state()
}).

-record(od_storage, {
    name = <<>> :: od_storage:name() | undefined,
    provider :: od_provider:id() | undefined,
    spaces = [] :: [od_space:id()],
    qos_parameters = #{} :: od_storage:qos_parameters(),
    imported = false :: boolean() | undefined,
    readonly = false :: boolean(),
    cache_state = #{} :: cache_state()
}).

-record(od_token, {
    revoked = false :: boolean(),

    cache_state = #{} :: cache_state()
}).

-record(temporary_token_secret, {
    generation :: temporary_token_secret:generation(),

    cache_state = #{} :: cache_state()
}).

%%%===================================================================
%%% Records specific for oneprovider
%%%===================================================================

%% Authorization of this provider, access and identity tokens are derived from
%% the root token and cached for a configurable time.
-record(provider_auth, {
    provider_id :: od_provider:id(),
    root_token :: tokens:serialized(),
    cached_access_token = {0, <<"">>} :: {ValidUntil :: time:seconds(), tokens:serialized()},
    cached_identity_token = {0, <<"">>} :: {ValidUntil :: time:seconds(), tokens:serialized()}
}).

-record(file_download_code, {
    expires :: time:seconds(),
    session_id :: session:id(),
    file_guids :: [fslogic_worker:file_guid()]
}).

-record(offline_access_credentials, {
    user_id :: od_user:id(),
    access_token :: auth_manager:access_token(),
    interface :: undefined | cv_interface:interface(),
    data_access_caveats_policy :: data_access_caveats:policy(),
    valid_until :: time:seconds(),
    next_renewal_threshold :: time:seconds(),
    next_renewal_backoff :: time:seconds()
}).

%% User session
-record(session, {
    status :: undefined | session:status(),
    accessed :: undefined | time:seconds(),
    type :: undefined | session:type(),
    mode = normal :: session:mode(),
    identity :: aai:subject(),
    credentials :: undefined | auth_manager:credentials(),
    data_constraints :: data_constraints:constraints(),
    node :: node(),
    supervisor :: undefined | pid(),
    event_manager :: undefined | pid(),
    watcher :: undefined | pid(),
    sequencer_manager :: undefined | pid(),
    async_request_manager :: undefined | pid(),
    connections = [] :: [pid()],
    proxy_via :: undefined | oneprovider:id(),
    % Key-value in-session memory
    memory = #{} :: map(),
    direct_io = #{} :: #{od_space:id() => boolean()}
}).

% An empty model used for creating session local links
% For more information see session_local_links.erl
-record(session_local_links, {}).

% Model used to cache idp access tokens
-record(idp_access_token, {
    token :: idp_access_token:token(),
    expiration_time :: idp_access_token:expiration_time()
}).

%% File handle used by the storage_driver module
-record(sd_handle, {
    file_handle :: undefined | helpers:file_handle(),
    file :: undefined | helpers:file_id(),
    session_id :: undefined | session:id(),
    % file_uuid can be `undefined` if sfm_handle is not
    % associated with any file in the system.
    % It is associated only with file on storage.
    file_uuid :: undefined | file_meta:uuid(),
    space_id :: undefined | od_space:id(),
    storage_id :: undefined | storage:id(),
    open_flag :: undefined | helpers:open_flag(),
    needs_root_privileges :: undefined | boolean(),
    file_size = 0 :: non_neg_integer(),
    share_id :: undefined | od_share:id()
}).

-record(storage_sync_info, {
    guid :: undefined | fslogic_worker:file_guid(),
    children_hashes = #{} :: storage_sync_info:hashes(),
    mtime :: undefined | time:seconds(),
    last_stat :: undefined | time:seconds(),
    % below counters are used to check whether all batches of given directory
    % were processed, as they are processed in parallel
    batches_to_process = 0 :: non_neg_integer(),
    batches_processed = 0:: non_neg_integer(),
    % below map contains new hashes, that will be used to update values in children_hashes
    % when counters batches_to_process == batches_processed
    hashes_to_update = #{} :: storage_sync_info:hashes(),
    % Flag which informs whether any of the protected children files has been modified on storage.
    % This flag allows to detect changes when flags are unset.
    any_protected_child_changed = false :: boolean()
}).

% An empty model used for creating storage_sync_links
% For more information see storage_sync_links.erl
-record(storage_sync_links, {}).

% This model can be associated with file and holds information about hooks
% for given file. Hooks will be executed on future change of given
% file's file_meta document.
-record(file_meta_posthooks, {
    hooks = #{} :: #{file_meta_posthooks:hook_identifier() => file_meta_posthooks:hook()}
}).

% This model holds information about QoS entries defined for given file.
% Each file can be associated with zero or one such record. It is used to
% calculate effective_file_qos. (see file_qos.erl)
-record(file_qos, {
    % List containing qos_entry IDs defined for given file.
    qos_entries = [] :: [qos_entry:id()],
    % Mapping storage ID -> list containing qos_entry IDs.
    % When new QoS entry is added for file or directory storages on which replicas
    % should be stored are calculated using QoS expression. Calculated storages
    % are used to create traverse requests in qos_entry document. When provider
    % notices change in qos_entry document, it checks whether traverse request
    % for his storage is present. If so, provider updates entry in assigned_entries
    % map for his local storage.
    assigned_entries = #{} :: file_qos:assigned_entries()
}).

% This model holds information about QoS entry, that is QoS requirement
% defined by the user for file or directory through QoS expression and
% number of required replicas. Each such requirement creates new qos_entry
% document even if expressions are exactly the same. For each file / directory
% multiple qos_entry can be defined.
-record(qos_entry, {
    type = user_defined :: qos_entry:type(),
    file_uuid :: file_meta:uuid(),
    expression = [] :: qos_expression:expression(),
    replicas_num = 1 :: qos_entry:replicas_num(), % Required number of file replicas.
    % These are requests to providers to start QoS traverse.
    traverse_reqs = #{} :: qos_traverse_req:traverse_reqs(),
    % Contains id of provider that marked given entry as possible or impossible.
    % If more than one provider concurrently marks entry as possible one provider is
    % deterministically selected during conflict resolution.
    possibility_check :: {possible | impossible, od_provider:id()}
}).

% This model holds information of QoS traverse state in a directory subtree in order
% to calculate entry status.
-record(qos_status, {
    % Initialize with empty binary so it always compares as lower than any actual filename
    previous_batch_last_filename = <<>> :: file_meta:name(),
    current_batch_last_filename = <<>> :: file_meta:name(),
    files_list = [] :: [file_meta:uuid()],
    child_dirs_count = 0 :: non_neg_integer(),
    is_last_batch = false :: boolean(),
    is_start_dir :: boolean()
}).

-record(file_meta, {
    name :: undefined | file_meta:name(),
    type :: undefined | file_meta:type(),
    mode = 0 :: file_meta:posix_permissions(),
    protection_flags = 0 :: data_access_control:bitmask(),
    acl = [] :: acl:acl(), % VFS-7437 Handle conflict resolution similarly to hardlinks
    owner :: undefined | od_user:id(), % undefined for hardlink doc
    is_scope = false :: boolean(),
    provider_id :: undefined | oneprovider:id(), %% Id of provider that created this file
    shares = [] :: [od_share:id()], % VFS-7437 Handle conflict resolution similarly to hardlinks
    deleted = false :: boolean(),
    parent_uuid :: file_meta:uuid(),
    references = file_meta_hardlinks:empty_references() :: file_meta_hardlinks:references(),
    symlink_value :: undefined | file_meta_symlinks:symlink(),
    % this field is used to cache value from #dataset.state field
    % TODO VFS-7533 handle conflict on file_meta with remote provider
    dataset_state :: undefined | dataset:state()
}).

% Model used for storing information associated with dataset.
% One document is stored for one dataset.
% Key of the document is file_meta:uuid().
-record(dataset, {
    creation_time :: time:seconds(),
    state :: dataset:state(),
    detached_info :: undefined | dataset:detached_info()
}).


% An empty model used for creating deletion_markers
% For more information see deletion_marker.erl
-record(deletion_marker, {}).


-record(storage_config, {
    helper :: helpers:helper(),
    luma_config :: storage:luma_config()
}).


-record(luma_db, {
    table :: luma_db:table(),
    record :: luma_db:db_record(),
    storage_id :: storage:id(),
    feed :: luma:feed()
}).


-record(space_unsupport_job, {
    stage = init :: space_unsupport:stage(),
    task_id :: traverse:id(),
    space_id :: od_space:id(),
    storage_id :: storage:id(),
    % Id of task that was created in slave job (e.g. QoS entry id or cleanup traverse id).
    % It is persisted so when slave job is restarted no additional task is created.
    subtask_id = undefined :: space_unsupport:subtask_id() | undefined,
    % Id of process waiting to be notified of task finish.
    % NOTE: should be updated after provider restart
    slave_job_pid  = undefined :: pid() | undefined
}).

%% Model that stores config of file-popularity mechanism per given space.
-record(file_popularity_config, {
    enabled = false :: boolean(),
    last_open_hour_weight = 1.0 :: number(),
    avg_open_count_per_day_weight = 20.0 :: number(),
    max_avg_open_count_per_day = 100.0 :: number()
}).

%% Helper record for autocleaning model.
%% Record contains setting of one autocleaning selective rule.
%% A rule is a filter which can be used to limit the set of file replicas
%% which can be deleted by auto-cleaning mechanism.
%% If a rule is disabled, its constraint is ignored.
-record(autocleaning_rule_setting, {
    %% The rule can be enabled/disabled by setting enabled field.
    enabled = false :: boolean(),
    %% The value is a numerical value, which defines the limit value for given rule.
    %% Depending on the rule's name, the value can be lower/upper limit.
    value = 0 :: non_neg_integer()
}).

%% Helper record for autocleaning model.
%% Record contains settings for all autocleaning selective rules.
%% Rules are filters which can be used to limit the set of file replicas
%% which can be deleted by auto-cleaning mechanism.
%% File replica must satisfy all enabled rules to be deleted.
%% Satisfying all enabled rules is necessary but not sufficient.
%% Files will be deleted in order determined by the file_popularity_view.
%% Each rule is defined by one #autocleaning_rule_setting record.
-record(autocleaning_rules, {
    % informs whether selective rules should be used by auto-cleaning mechanism
    % this field has higher priority than enabled field in each #autocleaning_rule_setting
    % which means that setting this field to false results in ignoring all rules,
    % because it overrides specific rules' settings
    enabled = false :: boolean(),
    % min size of file that can be evicted during autocleaning
    min_file_size = #autocleaning_rule_setting{} :: autocleaning_config:rule_setting(),
    % max size of file that can be evicted during autocleaning
    max_file_size = #autocleaning_rule_setting{} :: autocleaning_config:rule_setting(),
    % files which were opened for the last time more than given number of
    % hours ago may be cleaned
    min_hours_since_last_open = #autocleaning_rule_setting{} :: autocleaning_config:rule_setting(),
    % files that have been opened less than max_open_count times may be cleaned.
    max_open_count = #autocleaning_rule_setting{} :: autocleaning_config:rule_setting(),
    % files that have moving average of open operations count per hour less
    % than given value may be cleaned. The average is calculated in 24 hours window.
    max_hourly_moving_average = #autocleaning_rule_setting{} :: autocleaning_config:rule_setting(),
    % files that have moving average of open operations count per day less than
    % given value may be cleaned. The average is calculated in 31 days window.
    max_daily_moving_average = #autocleaning_rule_setting{} :: autocleaning_config:rule_setting(),
    % files that have moving average of open operations count per month less
    % than given value may be cleaned. The average is calculated in 12 months window.
    max_monthly_moving_average = #autocleaning_rule_setting{} :: autocleaning_config:rule_setting()
}).

%% Helper record for autocleaning model.
%% Record contains configuration of auto-cleaning mechanism for given space.
-record(autocleaning_config, {
    % this field enables/disables auto-cleaning mechanism in given space
    enabled = false :: boolean(),
    % storage occupancy at which auto-cleaning will stop
    target = 0 :: non_neg_integer(),
    % auto-cleaning will start after exceeding threshold level of occupancy
    threshold = 0 :: non_neg_integer(),
    rules = #autocleaning_rules{} :: autocleaning_config:rules()
}).

%% Model for holding information about auto-cleaning runs.
%% Each record stores information about one specific run.
-record(autocleaning_run, {
    space_id :: undefined | od_space:id(),
    started_at = 0 :: time:seconds(),
    stopped_at :: undefined | time:seconds(),
    status :: undefined | autocleaning_run:status(),

    released_bytes = 0 :: non_neg_integer(),
    bytes_to_release = 0 :: non_neg_integer(),
    released_files = 0 :: non_neg_integer(),

    view_traverse_token :: undefined | view_traverse:token()
}).

%% Model which stores information about auto-cleaning per given space.
-record(autocleaning, {
    % id of current auto-cleaning run
    current_run :: undefined | autocleaning:run_id(),
    % record describing configuration of auto-cleaning per given space
    config :: undefined | autocleaning:config()
}).

-record(helper_handle, {
    handle :: helpers_nif:helper_handle(),
    timeout = infinity :: timeout()
}).

%% Model for storing file's location data
-record(file_location, {
    uuid :: file_meta:uuid(),
    provider_id :: undefined | oneprovider:id(),
    storage_id :: undefined | storage:id(),
    file_id :: undefined | helpers:file_id(),
    rename_src_file_id :: undefined | helpers:file_id(),
    blocks = [] :: fslogic_location_cache:stored_blocks(),
    version_vector = #{} :: version_vector:version_vector(),
    size = 0 :: non_neg_integer() | undefined,
    space_id :: undefined | od_space:id(),
    recent_changes = {[], []} :: {
        OldChanges :: [replica_changes:change()],
        NewChanges :: [replica_changes:change()]
    },
    last_rename :: undefined | replica_changes:last_rename(),
    storage_file_created = false :: boolean(),
    last_replication_timestamp :: undefined | time:seconds(),
    % synced_gid field is set by storage import, only on POSIX-compatible storages.
    % It is used to override display gid, only in
    % the syncing provider, with the gid that file
    % belongs to on synced storage.
    synced_gid :: undefined | luma:gid()
}).

%% Model for storing file's blocks
-record(file_local_blocks, {
    last = true :: boolean(),
    blocks = [] :: fslogic_blocks:blocks()
}).

%% Model for storing dir's location data
-record(dir_location, {
    storage_file_created = false :: boolean(),
    storage_file_id :: undefined | helpers:file_id(),
    % synced_gid field is set by storage import, only on POSIX-compatible storages.
    % It is used to override display gid, only in
    % the syncing provider, with the gid that file
    % belongs to on synced storage.
    synced_gid :: undefined | luma:gid()
}).

%% Model that stores configuration of storage import
-record(storage_import_config, {
    mode :: storage_import_config:mode(),
    % below field is undefined for manual mode
    auto_storage_import_config :: undefined | auto_storage_import_config:config()
}).

%% @TODO VFS-6767 deprecated, included for upgrade procedure. Remove in next major release after 21.02.*.
%% Model that stores configuration of storage import mechanism
-record(storage_sync_config, {
    import_enabled = false :: boolean(),
    update_enabled = false :: boolean(),
    import_config = #{} :: space_strategies:import_config(),
    update_config = #{} :: space_strategies:update_config()
}).

%% @TODO VFS-6767 deprecated, included for upgrade procedure. Remove in next major release after 21.02.*.
%% Model that maps space to storage strategies
-record(space_strategies, {
    sync_configs = #{} :: space_strategies:sync_configs()
}).

%% @TODO VFS-6767 deprecated, included for upgrade procedure. Remove in next major release after 21.02.*.
-record(storage_sync_monitoring, {
    scans = 0 :: non_neg_integer(), % overall number of finished scans,
    import_start_time :: undefined | time:seconds(),
    import_finish_time :: undefined | time:seconds(),
    last_update_start_time :: undefined | time:seconds(),
    last_update_finish_time :: undefined | time:seconds(),

    % counters used for scan management, they're reset on the beginning of each scan
    to_process = 0 :: non_neg_integer(),
    imported = 0 :: non_neg_integer(),
    updated = 0 :: non_neg_integer(),
    deleted = 0 :: non_neg_integer(),
    failed = 0 :: non_neg_integer(),
    % counter for tasks which don't match to any one of the above categories
    % i.e.
    %   * remote files that were processed by sync algorithm but not deleted
    %   * directories are processed many times (for each batch) but we increase
    %     `updated` counter only for 1 batch, for other batches we increase
    %     `other_processed_tasks` to keep track of algorithm and check whether
    %     it performs as intended
    other_processed = 0 :: non_neg_integer(),

    imported_sum = 0 :: non_neg_integer(),
    updated_sum = 0 :: non_neg_integer(),
    deleted_sum = 0 :: non_neg_integer(),

    imported_min_hist :: time_slot_histogram:histogram(),
    imported_hour_hist :: time_slot_histogram:histogram(),
    imported_day_hist :: time_slot_histogram:histogram(),

    updated_min_hist :: time_slot_histogram:histogram(),
    updated_hour_hist :: time_slot_histogram:histogram(),
    updated_day_hist :: time_slot_histogram:histogram(),

    deleted_min_hist :: time_slot_histogram:histogram(),
    deleted_hour_hist :: time_slot_histogram:histogram(),
    deleted_day_hist :: time_slot_histogram:histogram(),

    queue_length_min_hist :: time_slot_histogram:histogram(),
    queue_length_hour_hist :: time_slot_histogram:histogram(),
    queue_length_day_hist :: time_slot_histogram:histogram()
}).

%% Model that storage monitoring data of auto storage import.
%% The doc is stored per space.
-record(storage_import_monitoring, {
    finished_scans = 0 :: non_neg_integer(), % overall number of finished scans,
    status :: storage_import_monitoring:status(),

    % start/stop timestamps of last scan in millis
    scan_start_time :: undefined | time:millis(),
    scan_stop_time :: undefined | time:millis(),

    % Counters of files (regular and directories) processed during current (or last finished) scan.
    % Each processed file is counted just once.
    created = 0 :: non_neg_integer(),
    modified = 0 :: non_neg_integer(),
    deleted = 0 :: non_neg_integer(),
    failed = 0 :: non_neg_integer(),
    unmodified = 0 :: non_neg_integer(),

    % histograms for files (both directories and regular files) creations
    created_min_hist :: time_slot_histogram:histogram(),
    created_hour_hist :: time_slot_histogram:histogram(),
    created_day_hist :: time_slot_histogram:histogram(),

    % histograms for files (both directories and regular files) modification
    modified_min_hist :: time_slot_histogram:histogram(),
    modified_hour_hist :: time_slot_histogram:histogram(),
    modified_day_hist :: time_slot_histogram:histogram(),

    % histograms for files (both directories and regular files) deletions
    deleted_min_hist :: time_slot_histogram:histogram(),
    deleted_hour_hist :: time_slot_histogram:histogram(),
    deleted_day_hist :: time_slot_histogram:histogram(),

    % histograms for length of jobs' queue
    queue_length_min_hist :: time_slot_histogram:histogram(),
    queue_length_hour_hist :: time_slot_histogram:histogram(),
    queue_length_day_hist :: time_slot_histogram:histogram()
}).


%% Model that holds synchronization state for a space
-record(dbsync_state, {
    seq = #{} :: #{od_provider:id() => {couchbase_changes:seq(), datastore_doc:timestamp()}}
}).

%% Model that holds state entries for DBSync worker
-record(dbsync_batches, {
    batches = #{} :: map()
}).

%% Model that holds files created by root, whose owner needs to be changed when
%% the user will be present in current provider.
%% The Key of this document is UserId.
-record(files_to_chown, {
    file_guids = [] :: [fslogic_worker:file_guid()]
}).

-record(storage_traverse_job, {
    % Information about execution environment and processing task
    pool :: traverse:pool(),
    task_id :: traverse:id(),
    callback_module :: traverse:callback_module(),
    % storage traverse specific fields
    storage_file_id :: helper:name(),
    space_id :: od_space:id(),
    storage_id :: storage:id(),
    iterator_module :: storage_traverse:iterator_type(),
    offset = 0 ::  non_neg_integer(),
    batch_size :: non_neg_integer(),
    marker :: undefined | helpers:marker(),
    max_depth :: non_neg_integer(),
    % flag that informs whether slave_job should be scheduled on directories
    execute_slave_on_dir :: boolean(),
    % flag that informs whether children master jobs should be scheduled asynchronously
    async_children_master_jobs :: boolean(),
    % flag that informs whether job for processing next batch of given directory should be scheduled asynchronously
    async_next_batch_job :: boolean(),
    % initial argument for compute function (see storage_traverse.erl for more info)
    fold_children_init :: term(),
    % flag that informs whether compute function should be executed (see storage_traverse.erl for more info)
    fold_children_enabled :: boolean(),
    info :: storage_traverse:info()
}).

%% Model for holding current quota state for spaces
-record(space_quota, {
    current_size = 0 :: non_neg_integer()
}).

%% Record that holds monitoring id
-record(monitoring_id, {
    main_subject_type = undefined :: atom(),
    main_subject_id = <<"">> :: datastore:key(),
    metric_type = undefined :: atom(),
    secondary_subject_type = undefined :: atom(),
    secondary_subject_id = <<"">> :: datastore:key(),
    provider_id = oneprovider:get_id_or_undefined() :: oneprovider:id()
}).

%% Model for holding state of monitoring
-record(monitoring_state, {
    monitoring_id = #monitoring_id{} :: #monitoring_id{},
    rrd_guid :: undefined | binary(),
    state_buffer = #{} :: map(),
    last_update_time :: undefined | time:seconds()
}).

%% Model that stores file handles
-record(file_handles, {
    % this field informs whether file was deleted and
    % whether removal was performed locally or in remote provider
    removal_status = ?NOT_REMOVED :: file_handles:removal_status(),
    descriptors = #{} :: file_descriptors(),
    creation_handle :: file_handles:creation_handle()
}).

%% Model that holds file's custom metadata
-record(custom_metadata, {
    space_id :: undefined | od_space:id(),
    file_objectid :: undefined | file_id:objectid(), % undefined only for upgraded docs
    value = #{} :: json_utils:json_term()
}).

%% Model that manages caches of files' permissions
-record(permissions_cache, {
    value = undefined :: term()
}).

%% Helper model for caching files' permissions
-record(permissions_cache_helper, {
    value = undefined :: term()
}).

%% Helper model for caching files' permissions
-record(permissions_cache_helper2, {
    value = undefined :: term()
}).

%% Model that holds file timestamps
-record(times, {
    atime = 0 :: times:time(),
    ctime = 0 :: times:time(),
    mtime = 0 :: times:time()
}).

%% Model that tracks popularity of file
-record(file_popularity, {
    file_uuid :: undefined | file_meta:uuid(),
    space_id :: undefined  | od_space:id(),
    size = 0 :: non_neg_integer(),
    open_count = 0 :: non_neg_integer(),
    last_open = 0 :: time:hours(),
    hr_hist = [] :: list(),
    dy_hist = [] :: list(),
    mth_hist = [] :: list(),
    hr_mov_avg = 0 :: number(),
    dy_mov_avg = 0 :: number(),
    mth_mov_avg = 0 :: number()
}).

%% Model that holds information about an ongoing (or completed) transfer
-record(transfer, {
    file_uuid :: undefined | file_meta:uuid(),
    space_id :: undefined | od_space:id(),
    user_id :: undefined | od_user:id(),
    rerun_id = undefined :: undefined | transfer:id(),
    path :: undefined | file_meta:path(),
    callback :: undefined | transfer:callback(),
    enqueued = true :: boolean(),
    cancel = false :: boolean(),
    replication_status :: undefined | transfer:subtask_status(),
    eviction_status :: undefined | transfer:subtask_status(),
    scheduling_provider :: od_provider:id(),
    replicating_provider :: undefined | od_provider:id(),
    evicting_provider :: undefined | od_provider:id(),
    % pid of replication or replica_eviction controller, as both cannot execute
    % simultaneously for given TransferId
    pid :: undefined | binary(), %todo VFS-3657

    % counters used for transfer management
    % if file is migrated (replicated and invalidated)
    % it will be included twice in both counters
    files_to_process = 0 :: non_neg_integer(),
    files_processed = 0 :: non_neg_integer(), % sum of successfully and unsuccessfully processed files
    failed_files = 0 :: non_neg_integer(),

    % counters interesting for users
    files_replicated = 0 :: non_neg_integer(),
    bytes_replicated = 0 :: non_neg_integer(),
    files_evicted = 0 :: non_neg_integer(),
    schedule_time = 0 :: time:seconds(),
    start_time = 0 :: time:seconds(),
    finish_time = 0 :: time:seconds(),

    % Histograms with different time spans (last minute, hour, day and month)
    % of transferred bytes per provider, last_update per provider is
    % required to keep track in histograms.
    % Length of each histogram type is defined in transfer.hrl
    last_update = #{} :: #{od_provider:id() => time:seconds()},
    min_hist = #{} :: #{od_provider:id() => histogram:histogram()},
    hr_hist = #{} :: #{od_provider:id() => histogram:histogram()},
    dy_hist = #{} :: #{od_provider:id() => histogram:histogram()},
    mth_hist = #{} :: #{od_provider:id() => histogram:histogram()},

    % Only replication of files existing in given view will be scheduled
    % if this value is undefined, whole subtree will be iterated
    index_name :: transfer:view_name(),
    % query_view_params are directly passed to couchbase
    % if index_name (view_name) is undefined query_view_params are ignored
    query_view_params = [] :: transfer:query_view_params()
}).

%% Model that tracks process' open handles
-record(process_handles, {
    process :: pid(),
    handles = #{} :: #{lfm_context:handle_id() => lfm:handle()}
}).

%% Model that tracks what files are currently transferred
-record(transferred_file, {
    ongoing_transfers = ordsets:new() :: ordsets:ordset(transferred_file:entry()),
    ended_transfers = ordsets:new() :: ordsets:ordset(transferred_file:entry())
}).

%% Model that holds aggregated statistics about transfers featuring
%% given space and target provider.
-record(space_transfer_stats, {
    % Histograms with different time spans (last minute, hour, day and month)
    % of transferred bytes per provider, last_update per provider is
    % required to keep track in histograms.
    % Length of each histogram type is defined in transfer.hrl
    last_update = #{} :: #{od_provider:id() => time:seconds()},
    min_hist = #{} :: #{od_provider:id() => histogram:histogram()},
    hr_hist = #{} :: #{od_provider:id() => histogram:histogram()},
    dy_hist = #{} :: #{od_provider:id() => histogram:histogram()},
    mth_hist = #{} :: #{od_provider:id() => histogram:histogram()}
}).

%% Model that holds statistics about all transfers for given space (memory only and node-wide).
-record(space_transfer_stats_cache, {
    % Time at which the cache record will expire.
    expiration_timer = countdown_timer:start_millis(0) :: countdown_timer:instance(),
    % Timestamp of last update for stats.
    last_update = 0 :: time:seconds(),
    % Mapping of providers to their data input and sources
    stats_in = #{} :: #{od_provider:id() => histogram:histogram()},
    % Mapping of providers to their data output and destinations
    stats_out = #{} :: #{od_provider:id() => histogram:histogram()},
    % Providers mapping to providers they recently sent data to
    active_channels = #{} :: undefined | #{od_provider:id() => [od_provider:id()]}
}).

%% Model used for communication between providers during
%% deletion of file replica.
-record(replica_deletion, {
    file_uuid :: undefined | file_meta:uuid(),
    space_id :: undefined | od_space:id(),
    action :: replica_deletion:action(),
    requested_blocks = [] :: fslogic_blocks:blocks(),
    supported_blocks = [] :: fslogic_blocks:blocks(),
    version_vector = #{} :: version_vector:version_vector(),
    requester :: od_provider:id(),
    requestee :: od_provider:id(),
    job_id :: replica_deletion:job_id(),
    job_type :: replica_deletion:job_type()
}).

%% Model used for setting read-write lock to synchronize replica deletion
%% of file replicas.
-record(replica_deletion_lock, {
    read = 0 :: non_neg_integer(),
    write = 0 :: non_neg_integer()
}).

%% Model that holds information on database view.
%% Specifying reduce function is optional in case of non-spatial views and
%% forbidden in case of spatial ones (map_function is treated as
%% spacial function).
-record(index, {
    name :: index:name(),
    space_id :: od_space:id(),
    spatial = false :: boolean(),
    map_function :: index:view_function(),
    reduce_function :: undefined | index:view_function(),
    index_options = [] :: index:options(),
    providers = [] :: all | [od_provider:id()]
}).

%% Model that holds information about state of harvesting for given space
-record(harvesting_state, {
    % structure that holds progress of harvesting for {Harvester, Index} pairs
    progress :: harvesting_progress:progress()
}).

% Model that stores information about single traverse job that processes single directory/file (see tree_traverse.erl)
-record(tree_traverse_job, {
    % Information about execution environment and processing task
    pool :: traverse:pool(),
    callback_module :: traverse:callback_module(),
    task_id :: traverse:id(),
    % Uuid of processed directory/file
    doc_id :: file_meta:uuid(),
    % User who scheduled the traverse
    user_id :: od_user:id(),
    % Information needed to restart directory listing
    use_listing_token = true :: boolean(),
    last_name :: file_meta:name(),
    last_tree :: od_provider:id(),
    % Traverse task specific info
    child_dirs_job_generation_policy :: tree_traverse:child_dirs_job_generation_policy(),
    children_master_jobs_mode :: tree_traverse:children_master_jobs_mode(),
    track_subtree_status :: boolean(),
    batch_size :: tree_traverse:batch_size(),
    traverse_info :: binary()
}).

%% Model that holds information necessary to tell whether whole subtree
%% of a directory was traversed so this directory can be cleaned up.
-record(tree_traverse_progress, {
    % number of children jobs listed but not yet processed
    to_process = 0 :: non_neg_integer(),
    % number of children jobs processed
    processed = 0 :: non_neg_integer(),
    % flag that informs whether all batches of children have been listed
    all_batches_listed = false :: boolean()
}).

-endif.