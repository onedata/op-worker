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
    global_cache_controller,
    local_cache_controller
]).

%% List of all global caches
-define(GLOBAL_CACHES, [
    some_record,
    file_meta
]).

%% List of all local caches
-define(LOCAL_CACHES, [
]).

%% Model that controls utilization of global cache
-record(global_cache_controller, {
    timestamp :: tuple(),
    action = non :: atom(),
    last_user = non :: pid() | non,
    last_action_time :: tuple()
}).

%% Model that controls utilization of local cache
-record(local_cache_controller, {
    timestamp :: tuple()
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
    node :: node(),
    session_sup :: pid(),
    event_manager :: pid(),
    sequencer_manager :: pid(),
    communicator :: pid()
}).

%% Local, cached version of globalregistry user
-record(onedata_user, {
    name :: binary()
}).


-record(file_meta, {
    name :: binary(),
%%     type :: file_meta:type(),
%%     posix_permissions :: file_meta:posix_permissions(),
    is_scope = false :: boolean(),
    mtime :: non_neg_integer(),
    atime :: non_neg_integer(),
    ctime :: non_neg_integer()
}).

-endif.