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

%% Message ID containing recipient for remote response.
-record(message_id, {
    issuer :: undefined | client | server,
    id :: undefined | binary(),
    recipient :: pid() | undefined
}).

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
    open_files = sets:new() :: sets:set(file_meta:uuid()),
    transfers = [] :: [transfer:id()]
}).

%% File handle used by the module
-record(sfm_handle, {
    file_handle :: undefined | helpers:file_handle(),
    file :: undefined | helpers:file_id(),
    session_id :: undefined | session:id(),
    file_uuid :: file_meta:uuid(),
    space_uuid :: file_meta:uuid(),
    storage :: undefined | storage:doc(),
    storage_id :: undefined | storage:id(),
    open_flag :: undefined | helpers:open_flag(),
    needs_root_privileges :: undefined | boolean(),
    is_local = false :: boolean(),
    provider_id :: undefined | oneprovider:id(),
    file_size :: undefined | non_neg_integer(), %% Available only if file is_local
    share_id :: undefined | od_share:id()
}).

-record(file_meta, {
    name :: undefined | file_meta:name(),
    type :: undefined | file_meta:type(),
    mode = 0 :: file_meta:posix_permissions(),
    owner :: undefined | od_user:id(),
    size = 0 :: undefined | file_meta:size(),
    version = 0, %% Snapshot version
    is_scope = false :: boolean(),
    scope :: datastore:key(),
    provider_id :: undefined | oneprovider:id(), %% ID of provider that created this file
    %% symlink_value for symlinks, file_guid for phantom files (redirection)
    link_value :: undefined | file_meta:symlink_value() | fslogic_worker:file_guid(),
    shares = [] :: [od_share:id()]
}).

-record(storage, {
    name = <<>> :: storage:name(),
    helpers = [] :: [storage:helper()],
    readonly = false :: boolean()
}).

%% Model that maps space to storage
-record(space_storage, {
    storage_ids = [] :: [storage:id()],
    mounted_in_root = [] :: [storage:id()]
}).

-record(helper_handle, {
    handle :: helpers_nif:helper_handle(),
    timeout = infinity :: timeout()
}).

%% Model for storing file's location data
-record(file_location, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    provider_id :: undefined | oneprovider:id(),
    storage_id :: undefined | storage:id(),
    file_id :: undefined | helpers:file_id(),
    blocks = [] :: [fslogic_blocks:block()],
    version_vector = #{},
    size = 0 :: non_neg_integer() | undefined,
    space_id :: undefined | od_space:id(),
    recent_changes = {[], []} :: {
        OldChanges :: [fslogic_file_location:change()],
        NewChanges :: [fslogic_file_location:change()]
    },
    last_rename :: fslogic_file_location:last_rename()
}).

-define(DEFAULT_FILENAME_MAPPING_STRATEGY, {simple, #{}}).
-define(DEFAULT_STORAGE_IMPORT_STRATEGY, {no_import, #{}}).
-define(DEFAULT_STORAGE_UPDATE_STRATEGIES, [{no_import, #{}}]).

%% Model that maps space to storage strategies
-record(storage_strategies, {
    filename_mapping = ?DEFAULT_FILENAME_MAPPING_STRATEGY :: space_strategy:config(),
    storage_import = ?DEFAULT_STORAGE_IMPORT_STRATEGY :: space_strategy:config(),
    storage_update = ?DEFAULT_STORAGE_UPDATE_STRATEGIES :: [space_strategy:config()],
    last_import_time :: integer() | undefined
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

%% Model that holds state entries for DBSync worker
-record(dbsync_state, {
    entry :: term()
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
    value = #{} :: maps:map()
}).

%% Model that holds database views
-record(indexes, {
    value = #{} :: indexes_value()
}).

%% Model that keeps track of consistency of file metadata
-record(file_consistency, {
    components_present = [] :: [file_consistency:component()],
    waiting = [] :: [file_consistency:waiting()]
}).

%% Model that caches files' permissions
-record(permissions_cache, {
    value = undefined :: term()
}).

%% Helper model for caching files' permissions
-record(permissions_cache_helper, {
    value = undefined :: term()
}).

%% Record that controls change propagation
-record(change_propagation_controller, {
    change_revision = 0 :: non_neg_integer(),
    space_id = <<"">> :: binary(),
    verify_module :: atom(),
    verify_function :: atom()
}).

%% Model that holds file timestamps
-record(times, {
    atime = 0 :: times:time(),
    ctime = 0 :: times:time(),
    mtime = 0 :: times:time()
}).

-endif.
