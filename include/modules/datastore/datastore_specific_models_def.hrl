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

%% Message ID containing recipient for remote response.
-record(message_id, {
    issuer :: client | server,
    id :: binary(),
    recipient :: pid() | undefined
}).

% State of subscription tracking.
-record(subscriptions_state, {
    refreshing_node :: node(),
    largest :: subscriptions:seq(),
    missing :: [subscriptions:seq()],
    users :: sets:set(onedata_user:id())
}).

%% Identity containing user_id
-record(identity, {
    user_id :: onedata_user:id(),
    provider_id :: oneprovider:id()
}).

%% User session
-record(session, {
    status :: session:status(),
    accessed :: erlang:timestamp(),
    type :: session:type(),
    identity :: session:identity(),
    auth :: session:auth(),
    node :: node(),
    supervisor :: pid(),
    event_manager :: pid(),
    watcher :: pid(),
    sequencer_manager :: pid(),
    connections = [] :: [pid()],
    proxy_via :: session:id() | undefined,
    response_map = #{} :: #{},
    % Key-value in-session memory
    memory = [] :: [{Key :: term(), Value :: term()}],
    % Handles for opened files
    handles = #{} :: #{binary() => storage_file_manager:handle()},
    open_files = sets:new() :: sets:set(file_meta:uuid()),
    transfers = [] :: [transfer:id()]
}).

%% Local, cached version of OZ user
-record(onedata_user, {
    name :: binary(),
    spaces = [] :: [{SpaceId :: binary(), SpaceName :: binary()}],
    default_space :: binary() | undefined,
    group_ids :: [binary()],
    effective_group_ids = [] :: [binary()],
    connected_accounts :: [onedata_user:connected_account()],
    alias :: binary(),
    email_list :: [binary()],
    revision_history = [] :: [subscriptions:rev()],
    % This field means that only public information is available about this
    % user. This is the case when given user hasn't ever logged in to this
    % provider, but basic information about him is required (e. g. he is a
    % member of space or group together with user that is currently logged in).
    % Public information contains id and name.
    public_only = false :: boolean()
}).

%% Local, cached version of OZ group
-record(onedata_group, {
    name :: binary(),
    type :: onedata_group:type(),
    users = [] :: [{UserId :: binary(), [privileges:group_privilege()]}],
    effective_users = [] :: [{UserId :: binary(), [privileges:group_privilege()]}],
    nested_groups = [] :: [{GroupId :: binary(), [privileges:group_privilege()]}],
    parent_groups = [] :: [binary()],
    spaces = [] :: [SpaceId :: binary()],
    revision_history = [] :: [subscriptions:rev()]
}).

-record(file_meta, {
    name :: file_meta:name(),
    type :: file_meta:type(),
    mode = 0 :: file_meta:posix_permissions(),
    mtime :: file_meta:time(),
    atime :: file_meta:time(),
    ctime :: file_meta:time(),
    uid :: onedata_user:id(), %% Reference to onedata_user that owns this file
    size = 0 :: file_meta:size(),
    version = 1, %% Snapshot version
    is_scope = false :: boolean(),
    scope :: datastore:key(),
    %% symlink_value for symlinks, file_guid for phantom files (redirection)
    link_value :: file_meta:symlink_value() | fslogic_worker:file_guid()
}).


%% Helper name and its arguments
-record(helper_init, {
    name :: helpers:name(),
    args :: helpers:args()
}).

%% Model for storing storage information
-record(storage, {
    name :: storage:name(),
    helpers :: [#helper_init{}]
}).


%% Model for storing file's location data
-record(file_location, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    provider_id :: oneprovider:id(),
    storage_id :: storage:id(),
    file_id :: helpers:file(),
    blocks = [] :: [fslogic_blocks:block()],
    version_vector = #{},
    size = 0 :: non_neg_integer() | undefined,
    handle_id :: binary() | undefined,
    space_id :: space_info:id(),
    recent_changes = {[], []} :: {
        OldChanges :: [fslogic_file_location:change()],
        NewChanges :: [fslogic_file_location:change()]
    }
}).

%% Model for caching provider details fetched from OZ
-record(provider_info, {
    client_name :: binary(),
    urls = [] :: [binary()],
    space_ids = [] :: [SpaceId :: binary()],
    public_only = false :: boolean(), %% see comment in onedata_users
    revision_history = [] :: [subscriptions:rev()]
}).

%% Model for caching space details fetched from OZ
-record(space_info, {
    name :: binary(),
    providers = [] :: [oneprovider:id()], %% Same as providers_supports but simplified for convenience
    providers_supports = [] :: [{ProviderId :: oneprovider:id(), Size :: pos_integer()}],
    users = [] :: [{UserId :: binary(), [privileges:space_privilege()]}],
    groups = [] :: [{GroupId :: binary(), [privileges:space_privilege()]}],
    revision_history = [] :: [subscriptions:rev()]
}).

%% Model that maps space to storage
-record(space_storage, {
    storage_ids = [] :: [storage:id()]
}).

%% Model that maps onedata user to Ceph user
-record(ceph_user, {
    credentials :: #{storage:id() => ceph_user:credentials()}
}).

%% Model that maps onedata user to Amazon S3 user
-record(s3_user, {
    credentials :: #{storage:id() => s3_user:credentials()}
}).

%% Model that holds state entries for DBSync worker
-record(dbsync_state, {
    entry :: term()
}).

%% Model that holds files created by root, whose owner needs to be changed when
%% the user will be present in current provider.
%% The Key of this document is UserId.
-record(files_to_chown, {
    file_uuids = [] :: [file_meta:uuid()]
}).

%% Model that maps onedata user to POSIX user
-record(posix_user, {
    credentials :: #{storage:id() => posix_user:credentials()}
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
    rrd_file = undefinied :: rrd_utils:rrd_file(),
    monitoring_interval = 0 :: non_neg_integer(),
    active = true :: boolean(),
    state_buffer = #{} :: maps:map()
}).

%% Model for holding lightweight version of monitoring state
-record(monitoring_init_state, {
    monitoring_id = #monitoring_id{} :: #monitoring_id{}
}).

%% Model that stores open file
-record(open_file, {
    is_removed = false :: true | false,
    active_descriptors = #{} :: #{session:id() => non_neg_integer()}
}).

%% Model that maps onedata user to Openstack Swift user
-record(swift_user, {
    credentials :: #{storage:id() => swift_user:credentials()}
}).

%% Model that holds file's custom metadata
-record(custom_metadata, {
    json = #{} :: #{}
}).

-endif.
