%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model that stores list of storage IDs attached to the space.
%%% @end
%%%-------------------------------------------------------------------
-module(space_storage).
-author("Krzysztof Trzepla").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").

-type doc() :: datastore:document().
-export_type([doc/0]).

%% API
-export([add/2, get_storage_ids/1]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).
-export([record_struct/1]).

-type id() :: od_space:id().
-type model() :: #space_storage{}.
-type doc() :: #document{value :: model()}.

-export_type([id/0, model/0, doc/0]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) ->
    datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {storage_ids, [string]}
    ]}.

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
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(space_storage_bucket, [], ?GLOBALLY_CACHED_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(model_behaviour:model_type(), model_behaviour:model_action(),
    datastore:store_level(), Context :: term(), ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(model_behaviour:model_type(), model_behaviour:model_action(),
    datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds storage to the space.
%% @end
%%--------------------------------------------------------------------
-spec add(od_space:id(), storage:id()) ->
    {ok, od_space:id()} | {error, Reason :: term()}.
add(SpaceId, StorageId) ->
    Doc = #document{
        key = SpaceId, value = #space_storage{storage_ids = [StorageId]}
    },

    Diff = fun(#space_storage{storage_ids = StorageIds} = Model) ->
        case lists:member(StorageId, StorageIds) of
            true -> {error, already_exists};
            false ->
                {ok, Model#space_storage{storage_ids = [StorageId | StorageIds]}}
        end
    end,

    case datastore:create_or_update(?STORE_LEVEL, Doc, Diff) of
        {ok, SpaceId} ->
            ok = space_strategies:add_storage(SpaceId, StorageId),
            {ok, SpaceId};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage IDs attached to the space.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_ids(model() | doc()) -> [storage:id()].
get_storage_ids(#space_storage{storage_ids = StorageIds}) ->
    StorageIds;
get_storage_ids(#document{value = #space_storage{} = Value}) ->
    get_storage_ids(Value).
