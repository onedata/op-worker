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

-include("proto/common/credentials.hrl").

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
    task_pool
]).

%% List of all global caches
-define(GLOBAL_CACHES, [
    some_record,
    file_meta
]).

%% List of all local caches
-define(LOCAL_CACHES, [
]).

%% Model that controls utilization of cache
-record(cache_controller, {
    timestamp = {0,0,0} :: tuple(),
    action = non :: atom(),
    last_user = non :: string() | non,
    last_action_time = {0,0,0} :: tuple(),
    deleted_links = [] :: list()
}).

%% sample model with example fields
-record(task_pool, {
    task :: task_manager:task(),
    owner :: pid(),
    node :: node()
}).

%% sample model with example fields
-record(some_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% event manager model:
%% value - mapping from subscription ID to subscription
-record(subscription, {
    value :: event_manager:subscription()
}).

%% Identity containing user_id
-record(identity, {
    user_id :: onedata_user:id()
}).

%% session:
%% identity - user identity
-record(session, {
    identity :: #identity{},
    type = fuse :: fuse | gui,
    auth :: #auth{},
    node = node() :: node(),
    session_sup = undefined :: pid() | undefined,
    event_manager = undefined :: pid() | undefined,
    sequencer_manager = undefined :: pid() | undefined,
    communicator = undefined :: pid() | undefined
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
    is_scope = false :: boolean()
}).

-endif.