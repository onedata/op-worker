%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Models definitions. Shall not be included directly in any erl file.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_MODELS_HRL).
-define(DATASTORE_MODELS_HRL, 1).

-include("modules/events/subscriptions.hrl").

%% Wrapper for all models' records
-record(document, {
    key :: datastore:ext_key(),
    rev :: term(),
    value :: datastore:value(),
    links :: term()
}).

%% Models' definitions

%% List of all available models
-define(MODELS, [
    some_record,
    subscription,
    session,
    onedata_user,
    identity,
    file_meta,
    cache_controller,
    task_pool,
    storage,
    file_location
]).

%% List of all global caches
-define(GLOBAL_CACHES, [
    some_record,
    file_meta,
    storage,
    file_location
]).

%% List of all local caches
-define(LOCAL_CACHES, [
]).

%% Model that controls utilization of cache
-record(cache_controller, {
    timestamp = {0, 0, 0} :: erlang:timestamp(),
    action = non :: atom(),
    last_user = non :: string() | non,
    last_action_time = {0, 0, 0} :: erlang:timestamp(),
    deleted_links = [] :: list()
}).

%% Description of task to be done
-record(task_pool, {
    task :: task_manager:task(),
    owner :: pid(),
    node :: node()
}).

%% Sample model with example fields
-record(some_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% Identity containing user_id
-record(identity, {
    user_id :: onedata_user:id()
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
    connections = [] :: [pid()]
}).

%% Local, cached version of globalregistry user
-record(onedata_user, {
    name :: binary(),
    space_ids :: [binary()]
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
    version = 1,    %% Snaphot version
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
    storage_id :: storage:id(),
    file_id :: helpers:file(),
    blocks = [] :: [fslogic_blocks:block()],
    size = 0 :: non_neg_integer() | undefined
}).

-endif.