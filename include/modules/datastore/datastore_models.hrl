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
-include_lib("ctool/include/posix/file_attr.hrl").
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

    direct_users = #{} :: #{od_user:id() => [privileges:space_privilege()]},
    eff_users = #{} :: #{od_user:id() => [privileges:space_privilege()]},

    direct_groups = #{} :: #{od_group:id() => [privileges:space_privilege()]},
    eff_groups = #{} :: #{od_group:id() => [privileges:space_privilege()]},

    storages = #{} :: #{storage:id() => Size :: integer()},

    % This value is calculated after fetch from zone for performance reasons.
    local_storages = [] :: [storage:id()],

    providers = #{} :: #{od_provider:id() => Size :: integer()},

    shares = [] :: [od_share:id()],

    harvesters = [] :: [od_harvester:id()],

    support_parameters_per_provider = #{} :: support_parameters:per_provider(),
    support_stage_per_provider = #{} :: support_stage:per_provider(),

    cache_state = #{} :: cache_state()
}).

%% Model for caching share details fetched from OZ
-record(od_share, {
    name :: binary(),
    public_url :: binary(),

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
    timestamp = od_handle:actual_timestamp() :: od_handle:timestamp(),

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
    name = <<>> :: od_storage:name(),
    provider :: od_provider:id() | undefined,
    spaces = [] :: [od_space:id()],
    qos_parameters = #{} :: od_storage:qos_parameters(),
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
    cached_access_token = {0, <<"">>} :: {ValidUntil :: time_utils:seconds(), tokens:serialized()},
    cached_identity_token = {0, <<"">>} :: {ValidUntil :: time_utils:seconds(), tokens:serialized()}
}).

-record(file_download_code, {
    session_id :: session:id(),
    file_guid :: fslogic_worker:file_guid()
}).

-record(file_force_proxy, {
    provider_id :: undefined | oneprovider:id()
}).

%% User session
-record(session, {
    status :: undefined | session:status(),
    accessed :: undefined | integer(),
    type :: undefined | session:type(),
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

% Model used to cache idp access tokens
-record(idp_access_token, {
    token :: idp_access_token:token(),
    expiration_time :: idp_access_token:expires()
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
    children_hashes = #{} :: storage_sync_info:hashes(),
    mtime :: undefined | non_neg_integer(),
    last_stat :: undefined | non_neg_integer(),
    % below counters are used to check whether all batches of given directory
    % were processed, as they are processed in parallel
    batches_to_process = 0 :: non_neg_integer(),
    batches_processed = 0:: non_neg_integer(),
    % below map contains new hashes, that will be used to update values in children_hashes
    % when counters batches_to_process == batches_processed
    hashes_to_update = #{} :: storage_sync_info:hashes()
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
    expression = [] :: qos_expression:rpn(), % QoS expression in RPN form.
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
    acl = [] :: acl:acl(),
    owner :: od_user:id(),
    is_scope = false :: boolean(),
    provider_id :: undefined | oneprovider:id(), %% ID of provider that created this file
    shares = [] :: [od_share:id()],
    deleted = false :: boolean(),
    parent_uuid :: undefined | file_meta:uuid(),

    % Below fields are set by storage_sync.
    % They are used to override display gid, only in
    % the syncing provider, with the gid that file
    % belongs to on synced storage.
    synced_storage :: undefined | storage:id(),
    synced_gid :: undefined | luma:gid()
}).

-record(storage_config, {
    helper :: helpers:helper(),
    readonly = false :: boolean(),
    luma_config :: undefined | luma_config:config(),
    imported_storage = false :: boolean()
}).


%%% @TODO VFS-5856 deprecated, included for upgrade procedure. Remove in next major release.
-record(storage, {
    name = <<>> :: storage_config:name(),
    helpers = [] :: [helpers:helper()],
    readonly = false :: boolean(),
    luma_config = undefined :: undefined | luma_config:config()
}).

-record(luma_config, {
    url :: luma_config:url(),
    api_key :: luma_config:api_key()
}).

%% Model that stores local user mappings.
%% Documents are stored per storage (document's key is a StorageId).
-record(luma_users_cache, {
   users = #{} :: #{od_user:id() => luma_user:entry()}
}).

%% Model that stores default local user mapping for spaces supported by given storage.
%% Documents are stored per storage (document's key is a StorageId).
-record(luma_spaces_cache, {
    spaces = #{} :: #{od_space:id() => luma_space:entry()}
}).

%% Model that stores reverse local user mappings.
%% Documents are stored per storage (document's key is a StorageId).
-record(luma_reverse_cache, {
    users = #{} :: #{luma:uid() => od_user:id()},
    acl_users = #{} :: #{luma:acl_who() => od_user:id()},
    acl_groups = #{} ::  #{luma:acl_who() => od_group:id()}
}).

%% Model that maps space to storage
%%% @TODO VFS-5856 deprecated, included for upgrade procedure. Remove in next major release.
-record(space_storage, {
    storage_ids = [] :: [storage:id()],
    mounted_in_root = [] :: [storage:id()]
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

%% Model that holds information necessary to tell whether whole subtree
%% of a directory was traversed so this directory can be cleaned up.
-record(cleanup_traverse_status, {
    % number of children listed but not yet traversed
    pending_children_count = 0 :: non_neg_integer(),
    % flag that informs whether all batches of children have been listed
    all_batches_listed = false :: boolean()
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
    started_at = 0 :: non_neg_integer(),
    stopped_at :: undefined | non_neg_integer(),
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
    last_replication_timestamp :: non_neg_integer() | undefined
}).

%% Model for storing file's blocks
-record(file_local_blocks, {
    last = true :: boolean(),
    blocks = [] :: fslogic_blocks:blocks()
}).

%% Model for storing dir's location data
-record(dir_location, {
    storage_file_created = false :: boolean()
}).

%% Model that stores configuration of storage_sync mechanism
-record(storage_sync_config, {
    import_enabled = false :: boolean(),
    update_enabled = false :: boolean(),
    import_config = #{} :: space_strategies:import_config(),
    update_config = #{} :: space_strategies:update_config()
}).

%% Model that maps space to storage strategies
-record(space_strategies, {
    % todo VFS-5717 rename model to storage_sync_configs?
    sync_configs = #{} :: space_strategies:sync_configs()
}).

-record(storage_sync_monitoring, {
    scans = 0 :: non_neg_integer(), % overall number of finished scans,
    import_start_time :: undefined | non_neg_integer(),
    import_finish_time :: undefined | non_neg_integer(),
    last_update_start_time :: undefined | non_neg_integer(),
    last_update_finish_time :: undefined | non_neg_integer(),

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
    iterator_module :: storage_traverse:iterator_module(),
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
    last_update_time :: undefined | non_neg_integer()
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
    last_open = 0 :: non_neg_integer(),
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
    replication_status :: undefined | transfer:status(),
    eviction_status :: undefined | transfer:status(),
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
    schedule_time = 0 :: non_neg_integer(),
    start_time = 0 :: non_neg_integer(),
    finish_time = 0 :: non_neg_integer(),

    % Histograms with different time spans (last minute, hour, day and month)
    % of transferred bytes per provider, last_update per provider is
    % required to keep track in histograms.
    % Length of each histogram type is defined in transfer.hrl
    last_update = #{} :: #{od_provider:id() => non_neg_integer()},
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
    last_update = #{} :: #{od_provider:id() => non_neg_integer()},
    min_hist = #{} :: #{od_provider:id() => histogram:histogram()},
    hr_hist = #{} :: #{od_provider:id() => histogram:histogram()},
    dy_hist = #{} :: #{od_provider:id() => histogram:histogram()},
    mth_hist = #{} :: #{od_provider:id() => histogram:histogram()}
}).

%% Model that holds statistics about all transfers for given space.
-record(space_transfer_stats_cache, {
    % Time at which the cache record will expire.
    expires = 0 :: non_neg_integer(),
    % Time of last update for stats.
    timestamp = 0 :: non_neg_integer(),
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
    % Information needed to restart directory listing
    last_name :: file_meta:name(),
    last_tree :: od_provider:id(),
    % Traverse task specific info
    execute_slave_on_dir :: tree_traverse:execute_slave_on_dir(),
    batch_size :: tree_traverse:batch_size(),
    traverse_info :: binary()
}).

-endif.