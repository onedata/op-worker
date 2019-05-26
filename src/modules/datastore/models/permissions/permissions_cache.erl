%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model used for caching permissions to files (used by check_permissions module).
%%% @end
%%%-------------------------------------------------------------------
-module(permissions_cache).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/elements/task_manager/task_manager.hrl").

%% API
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, create_or_update/2]).
-export([check_permission/1, cache_permission/2, invalidate/0]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type key() :: datastore:key().
-type record() :: #permissions_cache{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

%% Key of document that keeps information about whole cache status.
-define(STATUS_UUID, <<"status">>).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined,
    volatile => true
}).

%% First helper module to be used
-define(START_MODULE, permissions_cache_helper).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves permission cache.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, key()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates permission cache.
%% @end
%%--------------------------------------------------------------------
-spec update(key(), diff()) -> {ok, key()} | {error, term()}.
update(?STATUS_UUID, Diff) ->
    Ctx = ?CTX#{volatile => false},
    ?extract_key(datastore_model:update(Ctx, ?STATUS_UUID, Diff));
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Creates permission cache.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, key()} | {error, term()}.
create(Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(doc(), diff()) ->
    {ok, key()} | {error, term()}.
create_or_update(#document{key = ?STATUS_UUID, value = Default}, Diff) ->
    Ctx = ?CTX#{volatile => false},
    ?extract_key(datastore_model:update(Ctx, ?STATUS_UUID, Diff, Default));
create_or_update(#document{key = Key, value = Default}, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff, Default)).

%%--------------------------------------------------------------------
%% @doc
%% Returns permission cache.
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes permission cache.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether permission cache exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(key()) -> boolean().
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks permission in cache.
%% @end
%%--------------------------------------------------------------------
-spec check_permission(Rule :: term()) -> {ok, term()} | calculate | no_return().
check_permission(Rule) ->
    case permissions_cache:get(?STATUS_UUID) of
        {ok, #document{value = #permissions_cache{value = {clearing, _}}}} ->
            calculate;
        {ok, #document{value = #permissions_cache{value = {Model, _}}}} ->
            get_rule(Model, Rule);
        {error, not_found} ->
            get_rule(?START_MODULE, Rule)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves permission in cache.
%% @end
%%--------------------------------------------------------------------
-spec cache_permission(Rule :: term(), Value :: term()) ->
    ok | {ok, key()} | {error, term()}.
cache_permission(Rule, Value) ->
    CurrentModel = case permissions_cache:get(?STATUS_UUID) of
        {ok, #document{value = #permissions_cache{value = {Model, _}}}} ->
            Model;
        {error, not_found} ->
            ?START_MODULE
    end,

    case CurrentModel of
        clearing ->
            ok;
        permissions_cache_helper ->
            permissions_cache_helper:save(#document{key = get_uuid(Rule), value =
            #permissions_cache_helper{value = Value}});
        permissions_cache_helper2 ->
            permissions_cache_helper2:save(#document{key = get_uuid(Rule), value =
            #permissions_cache_helper2{value = Value}})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Clears all permissions from cache.
%% @end
%%--------------------------------------------------------------------
-spec invalidate() -> ok.
invalidate() ->
    CurrentModel = case permissions_cache:get(?STATUS_UUID) of
        {ok, #document{value = #permissions_cache{value = {Model, _}}}} ->
            Model;
        {error, not_found} ->
            ?START_MODULE
    end,

    case CurrentModel of
        clearing ->
            ok;
        _ ->
            case start_clearing(CurrentModel) of
                {ok, _} ->
                    task_manager:start_task(fun() ->
                        erlang:apply(CurrentModel, delete_all, []),
                        ok = stop_clearing(CurrentModel)
                    end, ?CLUSTER_LEVEL);
                {error, parallel_cleaning} ->
                    ok
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets rule's uuid.
%% @end
%%--------------------------------------------------------------------
-spec get_uuid(Rule :: term()) -> binary().
get_uuid(Rule) ->
    base64:encode(term_to_binary(Rule)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets rule value from cache.
%% @end
%%--------------------------------------------------------------------
-spec get_rule(Model :: atom(), Rule :: term()) -> {ok, term()} | calculate | no_return().
get_rule(Model, Rule) ->
    case erlang:apply(Model, get, [get_uuid(Rule)]) of
        {ok, #document{value = #permissions_cache_helper{value = V}}} ->
            {ok, V};
        {ok, #document{value = #permissions_cache_helper2{value = V}}} ->
            {ok, V};
        {error, not_found} ->
            calculate
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves information about clearing start.
%% @end
%%--------------------------------------------------------------------
-spec start_clearing(CurrentModel :: atom()) -> {ok, key()} | {error, term()}.
start_clearing(CurrentModel) ->
    NewDoc = #document{key = ?STATUS_UUID, value = #permissions_cache{value = {permissions_cache_helper2, clearing}}},
    UpdateFun = fun
        (#permissions_cache{value = {S1, clearing}}) ->
            case S1 of
                CurrentModel ->
                    {ok, #permissions_cache{value = {clearing, clearing}}};
                _ ->
                    {error, parallel_cleaning}
            end;
        (#permissions_cache{value = {S1, Helper}}) ->
            case S1 of
                CurrentModel ->
                    {ok, #permissions_cache{value = {Helper, clearing}}};
                _ ->
                    {error, parallel_cleaning}
            end
    end,

    create_or_update(NewDoc, UpdateFun).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves information about clearing stop.
%% @end
%%--------------------------------------------------------------------
-spec stop_clearing(CurrentModel :: atom()) -> ok | no_return().
stop_clearing(CurrentModel) ->
    UpdateFun = fun
        (#permissions_cache{value = {clearing, clearing}}) ->
            {ok, #permissions_cache{value = {CurrentModel, clearing}}};
        (#permissions_cache{value = {S1, clearing}}) ->
            case S1 of
                CurrentModel ->
                    {error, already_cleared};
                _ ->
                    {ok, #permissions_cache{value = {S1, CurrentModel}}}
            end;
        (#permissions_cache{value = {clearing, S2}}) ->
            case S2 of
                CurrentModel ->
                    {ok, #permissions_cache{value = {CurrentModel, clearing}}};
                _ ->
                    {ok, #permissions_cache{value = {CurrentModel, S2}}}
            end;
        (_) ->
            {error, already_cleared}
    end,
    case update(?STATUS_UUID, UpdateFun) of
        {ok, _} -> ok;
        {error, already_cleared} -> ok
    end.

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