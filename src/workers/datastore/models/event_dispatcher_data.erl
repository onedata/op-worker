%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Sequencer model.
%%% @end
%%%-------------------------------------------------------------------
-module(event_dispatcher_data).
-author("Krzysztof Trzepla").
-behaviour(model_behaviour).

-include("workers/datastore/datastore.hrl").
-include("workers/datastore/datastore_internal.hrl").

-define(BATCH_SIZE, 100).

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1, model_init/0, 'after'/5, before/4]).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1. 
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    datastore:save(global_only, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2. 
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(global_only, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1. 
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(global_only, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(global_only, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error().
list() ->
    {ok, Handle} = datastore:list_init(global_only, ?MODULE, ?BATCH_SIZE),
    list(Handle, []).
list(Handle, Acc) ->
    case datastore:list_next(global_only, ?MODULE, Handle) of
        {ok, {[], _}} ->
            {ok, lists:flatten(Acc)};
        {ok, {[_ | _] = Items, NewHandle}} ->
            list(NewHandle, [Items | Acc]);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(global_only, ?MODULE, Key, ?PRED_ALWAYS).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1. 
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) ->
    true | false | datastore:generic_error().
exists(Key) ->
    datastore:exists(global_only, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0. 
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODULE_CONFIG(event_dispatcher_bucket, []).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5. 
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4. 
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) ->
    ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.