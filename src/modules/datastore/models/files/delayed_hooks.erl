%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% fixme
%%% @end
%%%-------------------------------------------------------------------
-module(delayed_hooks).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
%% functions operating on record using datastore model API
-export([get/1, delete/1, add_hook/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).


-type key() :: file_meta:uuid().
-type hook() :: fun(() -> term()).

-export_type([id/0, hook/0]).

-define(CTX, #{
    model => ?MODULE
}).

%%%===================================================================
%%% API
%%%===================================================================

%%%===================================================================
%%% Functions operating on record using datastore_model API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec add_hook(key(), hook()) -> {ok, key()} | {error, term()}.
add_hook(Key, Hook) ->
    EncodedHook = term_to_binary(Hook, [compressed]),
    datastore_model:update(?CTX, Key, fun(#delayed_hooks{hooks = Hooks}) ->
        {ok, [EncodedHook | Hooks]}
    end, #delayed_hooks{hooks = [EncodedHook]}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, [hook()]}.
get(Key) ->
    {ok, #document{value = #delayed_hooks{hooks = EncodedHooks}}} =
        datastore_model:get(?CTX, Key),
    Hooks = lists:map(fun(EncodedHook) ->
        binary_to_term(EncodedHook)
    end, EncodedHooks),
    {ok, Hooks}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {hooks, [string]}
    ]}.

