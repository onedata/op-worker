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

-include("modules/events/subscriptions.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").

-type file_descriptors() :: #{session:id() => non_neg_integer()}.
-type indexes_value() :: #{indexes:index_id() => indexes:index()}.

%%%===================================================================
%%% Records synchronized via subscriptions
%%%===================================================================

% Records starting with prefix od_ are special records that represent entities
% in the system and are synchronized to providers via subscriptions.
% The entities can have various relations between them, especially effective
% membership is possible via groups and nested groups.
% these records are synchronized from OZ via subscriptions.
%
% The below ASCII visual shows possible relations in entities graph.
%
%  provider    share
%      ^         ^
%       \       /
%        \     /
%         space    handle_service     handle
%         ^  ^        ^        ^       ^   ^
%         |   \      /         |      /    |
%         |    \    /          |     /     |
%        user    group          user      group
%                  ^                        ^
%                  |                        |
%                  |                        |
%                group                     user
%                ^   ^
%               /     \
%              /       \
%            user     user
%

-record(od_user, {
    name = <<"">> :: binary(),
    alias = <<"">> :: binary(), % TODO currently always empty
    email_list = [] :: [binary()], % TODO currently always empty
    connected_accounts = [] :: [od_user:connected_account()], % TODO currently always empty
    default_space :: binary() | undefined,
    % List of user's aliases for spaces
    space_aliases = [] :: [{od_space:id(), SpaceName :: od_space:alias()}],

    % Direct relations to other entities
    groups = [] :: [od_group:id()],
    spaces = [] :: [od_space:id()], % TODO currently always empty
    handle_services = [] :: [od_handle_service:id()],
    handles = [] :: [od_handle:id()],

    % Effective relations to other entities
    eff_groups = [] :: [od_group:id()],
    eff_spaces = [] :: [od_space:id()], % TODO currently always empty
    eff_shares = [] :: [od_share:id()], % TODO currently always empty
    eff_providers = [] :: [od_provider:id()], % TODO currently always empty
    eff_handle_services = [] :: [od_handle_service:id()], % TODO currently always empty
    eff_handles = [] :: [od_handle:id()], % TODO currently always empty

    % This field means that only public information is available about this
    % user. This is the case when given user hasn't ever logged in to this
    % provider, but basic information about him is required (e. g. he is a
    % member of space or group together with user that is currently logged in).
    % Public information contains id and name.
    public_only = false :: boolean(),
    revision_history = [] :: [subscriptions:rev()]
}).

%% Local, cached version of OZ group
-record(od_group, {
    name :: undefined | binary(),
    % Public means that only public data could be retrieved from the OZ as
    % no user in this provider has rights to view group data.
    type :: undefined | public | od_group:type(),

    % Group graph related entities
    parents = [] :: [binary()],
    children = [] :: [{od_group:id(), [privileges:group_privilege()]}],
    eff_parents = [] :: [od_group:id()], % TODO currently always empty
    eff_children = [] :: [{od_group:id(), [privileges:group_privilege()]}], % TODO currently always empty

    % Direct relations to other entities
    users = [] :: [{od_user:id(), [privileges:group_privilege()]}],
    spaces = [] :: [od_space:id()],
    handle_services = [] :: [od_handle_service:id()],
    handles = [] :: [od_handle:id()],

    % Effective relations to other entities
    eff_users = [] :: [{od_user:id(), [privileges:group_privilege()]}],
    eff_spaces = [] :: [od_space:id()], % TODO currently always empty
    eff_shares = [] :: [od_share:id()], % TODO currently always empty
    eff_providers = [] :: [od_provider:id()], % TODO currently always empty
    eff_handle_services = [] :: [od_handle_service:id()], % TODO currently always empty
    eff_handles = [] :: [od_handle:id()], % TODO currently always empty

    revision_history = [] :: [subscriptions:rev()]
}).

%% Model for caching space details fetched from OZ
-record(od_space, {
    name :: undefined | binary(),

    % Direct relations to other entities
    providers_supports = [] :: [{od_provider:id(), Size :: pos_integer()}],
    %% Same as providers_supports but simplified for convenience
    providers = [] :: [oneprovider:id()],
    users = [] :: [{od_user:id(), [privileges:space_privilege()]}],
    groups = [] :: [{od_group:id(), [privileges:space_privilege()]}],
    % All shares that belong to this space.
    shares = [] :: [od_share:id()],

    % Effective relations to other entities
    eff_users = [] :: [{od_user:id(), [privileges:space_privilege()]}], % TODO currently always empty
    eff_groups = [] :: [{od_group:id(), [privileges:space_privilege()]}], % TODO currently always empty

    revision_history = [] :: [subscriptions:rev()]
}).

%% Model for caching share details fetched from OZ
-record(od_share, {
    name = undefined :: undefined | binary(),
    public_url = undefined :: undefined | binary(),

    % Direct relations to other entities
    space = undefined :: undefined | od_space:id(),
    handle = undefined :: undefined | od_handle:id(),
    root_file = undefined :: undefined | binary(),

    % Effective relations to other entities
    eff_users = [] :: [od_user:id()], % TODO currently always empty
    eff_groups = [] :: [od_group:id()], % TODO currently always empty

    revision_history = [] :: [subscriptions:rev()]
}).

%% Model for caching provider details fetched from OZ
-record(od_provider, {
    client_name :: undefined | binary(),
    urls = [] :: [binary()],

    % Direct relations to other entities
    spaces = [] :: [od_space:id()],

    public_only = false :: boolean(), %% see comment in onedata_users
    revision_history = [] :: [subscriptions:rev()]
}).

%% Model for caching handle service details fetched from OZ
-record(od_handle_service, {
    name :: od_handle_service:name() | undefined,
    proxy_endpoint :: od_handle_service:proxy_endpoint() | undefined,
    service_properties = [] :: od_handle_service:service_properties(),

    % Effective relations to other entities
    users = [] :: [{od_user:id(), [privileges:handle_service_privilege()]}],
    groups = [] :: [{od_group:id(), [privileges:handle_service_privilege()]}],

    % Effective relations to other entities
    eff_users = [] :: [{od_user:id(), [privileges:handle_service_privilege()]}], % TODO currently always empty
    eff_groups = [] :: [{od_group:id(), [privileges:handle_service_privilege()]}], % TODO currently always empty

    revision_history = [] :: [subscriptions:rev()]
}).

%% Model for caching handle details fetched from OZ
-record(od_handle, {
    public_handle :: od_handle:public_handle() | undefined,
    resource_type :: od_handle:resource_type() | undefined,
    resource_id :: od_handle:resource_id() | undefined,
    metadata :: od_handle:metadata() | undefined,
    timestamp = od_handle:actual_timestamp() :: od_handle:timestamp() | undefined,

    % Direct relations to other entities
    handle_service :: od_handle_service:id() | undefined,
    users = [] :: [{od_user:id(), [privileges:handle_privilege()]}],
    groups = [] :: [{od_group:id(), [privileges:handle_privilege()]}],

    % Effective relations to other entities
    eff_users = [] :: [{od_user:id(), [privileges:handle_privilege()]}], % TODO currently always empty
    eff_groups = [] :: [{od_group:id(), [privileges:handle_privilege()]}], % TODO currently always empty

    revision_history = [] :: [subscriptions:rev()]
}).

%%%===================================================================
%%% Records specific for oneprovider
%%%===================================================================

% State of subscription tracking.
-record(subscriptions_state, {
    refreshing_node :: node(),
    largest :: undefined | subscriptions:seq(),
    missing :: undefined | [subscriptions:seq()],
    users :: undefined | sets:set(od_user:id())
}).

%% Identity containing user_id
-record(user_identity, {
    user_id :: undefined | od_user:id(),
    provider_id :: undefined | oneprovider:id()
}).

-record(file_force_proxy, {
    provider_id :: undefined | oneprovider:id()
}).

%% User session
-record(session, {
    status :: undefined | session:status(),
    accessed :: undefined | integer(),
    type :: undefined | session:type(),
    identity :: undefined | session:identity(),
    auth :: undefined | session:auth(),
    node :: node(),
    supervisor :: undefined | pid(),
    event_manager :: undefined | pid(),
    watcher :: undefined | pid(),
    sequencer_manager :: undefined | pid(),
    connections = [] :: [pid()],
    proxy_via :: oneprovider:id() | undefined,
    response_map = #{} :: maps:map(),
    % Key-value in-session memory
    memory = #{} :: maps:map(),
    open_files = sets:new() :: sets:set(fslogic_worker:file_guid()),
    transfers = [] :: [transfer:id()],
    direct_io = false :: boolean()
}).

%% File handle used by the module
-record(sfm_handle, {
    file_handle :: undefined | helpers:file_handle(),
    file :: undefined | helpers:file_id(),
    session_id :: undefined | session:id(),
    file_uuid :: file_meta:uuid(),
    space_id :: undefined | od_space:id(),
    storage :: undefined | storage:doc(),
    storage_id :: undefined | storage:id(),
    open_flag :: undefined | helpers:open_flag(),
    needs_root_privileges :: undefined | boolean(),
    file_size :: undefined | non_neg_integer(),
    share_id :: undefined | od_share:id()
}).

-record(storage_sync_info, {
    children_attrs_hashes = #{} :: #{non_neg_integer() => binary()},
    last_synchronized_mtime = undefined :: undefined | non_neg_integer()
}).

-record(file_meta, {
    name :: undefined | file_meta:name(),
    type :: undefined | file_meta:type(),
    mode = 0 :: file_meta:posix_permissions(),
    owner :: undefined | od_user:id(),
    group_owner :: undefined | od_group:id(),
    size = 0 :: undefined | file_meta:size(),
    version = 0, %% Snapshot version
    is_scope = false :: boolean(),
    scope :: datastore:key(),
    provider_id :: undefined | oneprovider:id(), %% ID of provider that created this file
    %% symlink_value for symlinks, file_guid for phantom files (redirection)
    link_value :: undefined | file_meta:symlink_value() | fslogic_worker:file_guid(),
    shares = [] :: [od_share:id()],
    deleted = false :: boolean(),
    storage_sync_info = #storage_sync_info{} :: file_meta:storage_sync_info(),
    parent_uuid :: undefined | file_meta:uuid()
}).

-record(storage, {
    name = <<>> :: storage:name(),
    helpers = [] :: [storage:helper()],
    readonly = false :: boolean(),
    luma_config = undefined :: undefined | luma_config:config()
}).

-record(luma_config, {
    url :: luma_config:url(),
    cache_timeout :: luma_config:cache_timeout(),
    api_key :: luma_config:api_key()
}).

%% Model that maps space to storage
-record(space_storage, {
    storage_ids = [] :: [storage:id()],
    mounted_in_root = [] :: [storage:id()],
    file_popularity_enabled = false :: boolean(),
    cleanup_enabled = false :: boolean(),
    cleanup_in_progress :: undefined | autocleaning:id(),
    autocleaning_config :: undefined | autocleaning_config:config()
}).

%% Record containing autocleaning configuration
-record(autocleaning_config, {
    % lowest size of file that can be invalidated during autocleaning
    lower_file_size_limit :: undefined | non_neg_integer(),
    % highest size of file that can be invalidated during autocleaning
    upper_file_size_limit :: undefined |  non_neg_integer(),
    % if file hasn't been opened for this number of hours or longer it will be deleted
    max_file_not_opened_hours :: undefined |  non_neg_integer(),
    % storage occupancy at which autocleaning will stop
    target :: non_neg_integer(),
    % autocleaning will start after exceeding threshold level of occupancy
    threshold :: non_neg_integer()
}).

%% Model for holding information about autocleaning procedures
-record(autocleaning, {
    space_id :: undefined | od_space:id(),
    started_at = 0 :: non_neg_integer(),
    stopped_at :: undefined | non_neg_integer(),
    released_bytes = 0 :: non_neg_integer(),
    bytes_to_release = 0 :: non_neg_integer(),
    released_files = 0 :: non_neg_integer(),
    status :: undefined | autocleaning:status(),
    config :: undefined | autocleaning_config:config()
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
    blocks = [] :: [fslogic_blocks:block()],
    version_vector = #{},
    size = 0 :: non_neg_integer() | undefined,
    space_id :: undefined | od_space:id(),
    recent_changes = {[], []} :: {
        OldChanges :: [replica_changes:change()],
        NewChanges :: [replica_changes:change()]
    },
    last_rename :: replica_changes:last_rename(),
    storage_file_created = false :: boolean()
}).

-define(DEFAULT_FILENAME_MAPPING_STRATEGY, {simple, #{}}).
-define(DEFAULT_STORAGE_IMPORT_STRATEGY, {no_import, #{}}).
-define(DEFAULT_STORAGE_UPDATE_STRATEGY, {no_update, #{}}).

%% Model that maps space to storage strategies
-record(storage_strategies, {
    filename_mapping = ?DEFAULT_FILENAME_MAPPING_STRATEGY :: space_strategy:config(),
    storage_import = ?DEFAULT_STORAGE_IMPORT_STRATEGY :: space_strategy:config(),
    storage_update = ?DEFAULT_STORAGE_UPDATE_STRATEGY :: space_strategy:config(),
    import_start_time :: space_strategy:timestamp(),
    import_finish_time :: space_strategy:timestamp(),
    last_update_start_time :: space_strategy:timestamp(),
    last_update_finish_time :: space_strategy:timestamp()
}).

-define(DEFAULT_FILE_CONFLICT_RESOLUTION_STRATEGY, {ignore_conflicts, #{}}).
-define(DEFAULT_FILE_CACHING_STRATEGY, {no_cache, #{}}).
-define(DEFAULT_ENOENT_HANDLING_STRATEGY, {error_passthrough, #{}}).

%% Model that maps space to storage strategies
-record(space_strategies, {
    storage_strategies = #{} :: maps:map(), %todo dializer crashes on: #{storage:id() => #storage_strategies{}},
    file_conflict_resolution = ?DEFAULT_FILE_CONFLICT_RESOLUTION_STRATEGY :: space_strategy:config(),
    file_caching = ?DEFAULT_FILE_CACHING_STRATEGY :: space_strategy:config(),
    enoent_handling = ?DEFAULT_ENOENT_HANDLING_STRATEGY :: space_strategy:config()
}).

%% Model that holds synchronization state for a space
-record(dbsync_state2, {
    seq = #{} :: maps:map([{od_provider:id(), couchbase_changes:seq()}])
}).

%% Model that holds state entries for DBSync worker
-record(dbsync_batches, {
    batches = #{} :: maps:map()
}).

%% Model that holds files created by root, whose owner needs to be changed when
%% the user will be present in current provider.
%% The Key of this document is UserId.
-record(files_to_chown, {
    file_guids = [] :: [fslogic_worker:file_guid()]
}).

%% Model for holding current quota state for spaces
-record(space_quota, {
    current_size = 0 :: non_neg_integer()
}).

%% Record that holds monitoring id
-record(monitoring_id, {
    main_subject_type = undefined :: atom(),
    main_subject_id = <<"">> :: datastore:id(),
    metric_type = undefined :: atom(),
    secondary_subject_type = undefined :: atom(),
    secondary_subject_id = <<"">> :: datastore:id(),
    provider_id = oneprovider:get_provider_id() :: oneprovider:id()
}).

%% Model for holding state of monitoring
-record(monitoring_state, {
    monitoring_id = #monitoring_id{} :: #monitoring_id{},
    rrd_guid :: undefined | binary(),
    state_buffer = #{} :: maps:map(),
    last_update_time :: undefined | non_neg_integer()
}).

%% Model that stores file handles
-record(file_handles, {
    is_removed = false :: boolean(),
    descriptors = #{} :: file_descriptors()
}).

%% Model that holds file's custom metadata
-record(custom_metadata, {
    space_id :: undefined | od_space:id(),
    file_objectid :: undefined | cdmi_id:object_id(), % undefined only for upgraded docs
    value = #{} :: maps:map()
}).

%% Model that holds database views
-record(indexes, {
    value = #{} :: indexes_value()
}).

%% Model that caches files' permissions
-record(permissions_cache, {
    value = undefined :: term()
}).

%% Helper model for caching files' permissions
-record(permissions_cache_helper, {
    value = undefined :: term()
}).

%% Model that holds file timestamps
-record(times, {
    atime = 0 :: times:time(),
    ctime = 0 :: times:time(),
    mtime = 0 :: times:time()
}).

-record(luma_cache, {
    timestamp = 0 :: luma_cache:timestamp(),
    value :: undefined | luma_cache:value()
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
    hr_mov_avg = 0 :: non_neg_integer(),
    dy_mov_avg = 0 :: non_neg_integer(),
    mth_mov_avg = 0 :: non_neg_integer()
}).

%% Model holds information about ongoing transfer
-record(transfer, {
    file_uuid :: undefined | file_meta:uuid(),
    space_id :: undefined | od_space:id(),
    path :: undefined | file_meta:path(),
    callback :: undefined | transfer:callback(),
    transfer_status :: undefined | transfer:status(),
    invalidation_status :: undefined | transfer:status(),
    source_provider_id :: undefined | oneprovider:id(),
    target_provider_id :: undefined | oneprovider:id(),
    invalidate_source_replica :: undefined | boolean(),
    % pid of transfer or invalidation controller, as both cannot execute
    % simultaneously for given TransferId
    pid :: undefined | binary(), %todo VFS-3657

    files_to_transfer = 0 :: non_neg_integer(),
    files_transferred = 0 :: non_neg_integer(),
    bytes_to_transfer = 0 :: non_neg_integer(),
    bytes_transferred = 0 :: non_neg_integer(),
    files_to_invalidate = 0 :: non_neg_integer(),
    files_invalidated = 0 :: non_neg_integer(),
    start_time = 0 :: non_neg_integer(),
    last_update = 0 :: non_neg_integer(),

    % Histograms of transferred bytes, head of list is most recent:
    % list of 60 integers counting bytes transferred during each minute of last hour,
    min_hist :: undefined | [non_neg_integer()],
    % list of 24 integers counting bytes transferred during each hour of last day,
    hr_hist :: undefined | [non_neg_integer()],
    % list of 30 integers counting bytes transferred during each day of last month,
    dy_hist :: undefined | [non_neg_integer()]
}).

%% Model for storing storage_sync monitoring data.
-record(storage_sync_histogram, {
    values = [] :: storage_sync_histogram:values(),
    timestamp :: undefined | storage_sync_histogram:timestamp()
}).

-endif.
