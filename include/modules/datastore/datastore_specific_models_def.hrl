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
    watcher :: pid(),
    event_manager :: pid(),
    sequencer_manager :: pid(),
    connections = [] :: [pid()],
    % Key-value in-session memory
    memory = [] :: [{Key :: term(), Value :: term()}]
}).

%% Local, cached version of OZ user
-record(onedata_user, {
    name :: binary(),
    space_ids :: [binary()],
    group_ids :: [binary()]
}).

%% Local, cached version of OZ group
-record(onedata_group, {
    name :: binary()
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
    version = 1, %% Snaphot version
    is_scope = false :: boolean()
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
    uuid :: file_meta:uuid(),
    provider_id :: oneprovider:id(),
    space_id :: file_meta:uuid(),
    storage_id :: storage:id(),
    file_id :: helpers:file(),
    blocks = [] :: [fslogic_blocks:block()],
    version_vector = #{},
    size = 0 :: non_neg_integer() | undefined,
    recent_changes = {[], []} :: {
        OldChanges :: [fslogic_file_location:change()],
        NewChanges :: [fslogic_file_location:change()]
    }
}).

%% Model for caching space details fetched from Global Registry
-record(space_info, {
    id :: binary(),
    name :: binary()
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

%% Model that maps onedata user to POSIX user
-record(posix_user, {
    credentials :: #{storage:id() => posix_user:credentials()}
}).

-endif.
