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

-type active_descriptors() :: #{session:id() => non_neg_integer()}.
-type ceph_user_ctx() :: #{storage:id() => ceph_user:ctx()}.
-type posix_user_ctx() :: #{storage:id() => posix_user:ctx()}.
-type s3_user_ctx() :: #{storage:id() => s3_user:ctx()}.
-type swift_user_ctx() :: #{storage:id() => swift_user:ctx()}.
-type indexes_value() :: #{indexes:index_id() => indexes:index()}.


%% Local, cached version of OZ user
-record(od_user, {
    name = <<"">> :: binary(),
    alias = <<"">> :: binary(),
    email_list = [] :: [binary()],
    connected_accounts = [] :: [od_user:connected_account()],
    default_space :: binary() | undefined,

    groups = [] :: [od_group:id()],
    space_aliases = [] :: [{od_space:id(), SpaceName :: binary()}],
    handle_services = [] :: [od_handle_service:id()],
    handles = [] :: [od_handle:id()],

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
    users = [] :: [{od_user:id(), [privileges:group_privilege()]}],
    effective_users = [] :: [{od_user:id(), [privileges:group_privilege()]}],
    nested_groups = [] :: [{od_group:id(), [privileges:group_privilege()]}],
    parent_groups = [] :: [binary()],
    spaces = [] :: [od_space:id()],
    handle_services = [] :: [od_handle_service:id()],
    handles = [] :: [od_handle:id()],
    revision_history = [] :: [subscriptions:rev()]
}).


%% Model for caching space details fetched from OZ
-record(od_space, {
    name :: undefined | binary(),
    providers = [] :: [oneprovider:id()], %% Same as providers_supports but simplified for convenience
    providers_supports = [] :: [{od_provider:id(), Size :: pos_integer()}],
    users = [] :: [{od_user:id(), [privileges:space_privilege()]}],
    groups = [] :: [{od_group:id(), [privileges:space_privilege()]}],
    % All shares that belong to this space.
    shares = [] :: [od_share:id()],
    revision_history = [] :: [subscriptions:rev()]
}).


%% Model for caching share details fetched from OZ
-record(od_share, {
    name = undefined :: undefined | binary(),
    public_url = undefined :: undefined | binary(),
    root_file_id = undefined :: undefined | binary(),
    parent_space = undefined :: undefined | binary(),
    handle = undefined :: undefined | binary(),
    revision_history = [] :: [subscriptions:rev()]
}).


%% Model for caching provider details fetched from OZ
-record(od_provider, {
    client_name :: undefined | binary(),
    urls = [] :: [binary()],
    space_ids = [] :: [od_space:id()],
    public_only = false :: boolean(), %% see comment in onedata_users
    revision_history = [] :: [subscriptions:rev()]
}).


%% Model for caching handle service details fetched from OZ
-record(od_handle_service, {
    name :: od_handle_service:name() | undefined,
    proxy_endpoint :: od_handle_service:proxy_endpoint() | undefined,
    service_properties = [] :: od_handle_service:service_properties(),
    users = [] :: [{od_user:id(), [privileges:handle_service_privilege()]}],
    groups = [] :: [{od_group:id(), [privileges:handle_service_privilege()]}],
    revision_history = [] :: [subscriptions:rev()]
}).


%% Model for caching handle details fetched from OZ
-record(od_handle, {
    handle_service_id :: od_handle_service:id() | undefined,
    public_handle :: od_handle:public_handle() | undefined,
    resource_type :: od_handle:resource_type() | undefined,
    resource_id :: od_handle:resource_id() | undefined,
    metadata :: od_handle:metadata() | undefined,
    users = [] :: [{od_user:id(), [privileges:handle_privilege()]}],
    groups = [] :: [{od_group:id(), [privileges:handle_privilege()]}],
    timestamp = od_handle:actual_timestamp() :: od_handle:timestamp(),
    revision_history = [] :: [subscriptions:rev()]
}).


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
    proxy_via :: session:id() | undefined,
    response_map = #{} :: maps:map(),
    % Key-value in-session memory
    memory = [] :: [{Key :: term(), Value :: term()}],
    open_files = sets:new() :: sets:set(file_meta:uuid()),
    transfers = [] :: [transfer:id()]
}).


%% File handle used by the module
-record(sfm_handle, {
    helper_handle :: undefined | helpers:handle(),
    file :: undefined | helpers:file(),
    session_id :: undefined | session:id(),
    file_uuid :: file_meta:uuid(),
    space_uuid :: file_meta:uuid(),
    storage :: datastore:document() | undefined,
    storage_id :: undefined | storage:id(),
    open_mode :: undefined | helpers:open_mode(),
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
    mtime :: undefined | file_meta:time(),
    atime :: undefined | file_meta:time(),
    ctime :: undefined | file_meta:time(),
    uid :: undefined | od_user:id(), %% Reference to od_user that owns this file
    size = 0 :: undefined | file_meta:size(),
    version = 0, %% Snapshot version
    is_scope = false :: boolean(),
    scope :: datastore:key(),
    provider_id :: undefined | oneprovider:id(), %% ID of provider that created this file
    %% symlink_value for symlinks, file_guid for phantom files (redirection)
    link_value :: undefined | file_meta:symlink_value() | fslogic_worker:file_guid(),
    shares = [] :: [od_share:id()]
}).


%% Helper name and its arguments
-record(helper_init, {
    name :: helpers:name(),
    args :: helpers:args()
}).


%% Model for storing storage information
-record(storage, {
    name :: undefined | storage:name(),
    helpers :: undefined | [helpers:init()]
}).


%% Model for storing file's location data
-record(file_location, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    provider_id :: undefined | oneprovider:id(),
    storage_id :: undefined | storage:id(),
    file_id :: undefined | helpers:file(),
    blocks = [] :: [fslogic_blocks:block()],
    version_vector = #{},
    size = 0 :: non_neg_integer() | undefined,
    handle_id :: binary() | undefined,
    space_id :: undefined | od_space:id(),
    recent_changes = {[], []} :: {
        OldChanges :: [fslogic_file_location:change()],
        NewChanges :: [fslogic_file_location:change()]
    },
    last_rename :: fslogic_file_location:last_rename()
}).


%% Model that maps space to storage
-record(space_storage, {
    storage_ids = [] :: [storage:id()]
}).


%% Model that maps onedata user to Ceph user
-record(ceph_user, {
    ctx = #{} :: ceph_user_ctx()
}).


%% Model that maps onedata user to POSIX user
-record(posix_user, {
    ctx = #{} :: posix_user_ctx()
}).


%% Model that maps onedata user to Amazon S3 user
-record(s3_user, {
    ctx = #{} :: s3_user_ctx()
}).


%% Model that maps onedata user to Openstack Swift user
-record(swift_user, {
    ctx = #{} :: swift_user_ctx()
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
    file_uuids = [] :: [file_meta:uuid()]
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
    rrd_path :: undefined | binary(),
    state_buffer = #{} :: maps:map(),
    last_update_time :: undefined | non_neg_integer()
}).

%% Model that stores open file
-record(open_file, {
    is_removed = false :: true | false,
    active_descriptors = #{} :: active_descriptors()
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

-endif.
