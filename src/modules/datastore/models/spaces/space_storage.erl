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

%% API
-export([add/2, add/3]).
-export([get_storage_ids/1, get_mounted_in_root/1, is_cleanup_enabled/1]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).
-export([record_struct/1, record_upgrade/2]).

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
    ]};
record_struct(2) ->
    {record, [
        {storage_ids, [string]},
        {mounted_in_root, [string]}
    ]};
record_struct(3) ->
    {record, [
        {storage_ids, [string]},
        {mounted_in_root, [string]},
        {cleanup_enabled, boolean}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades record from specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_upgrade(datastore_json:record_version(), tuple()) ->
    {datastore_json:record_version(), tuple()}.
record_upgrade(1, {?MODEL_NAME, StorageIds}) ->
    {2, #space_storage{storage_ids = StorageIds}};
record_upgrade(2, {?MODEL_NAME, StorageIds, MountedInRoot}) ->
    {3, #space_storage{storage_ids = StorageIds, mounted_in_root = MountedInRoot}}.

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
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
%TODO - luma gets undefined storage
get(undefined) ->
    {error, {not_found, ?MODULE}};
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    Config = ?MODEL_CONFIG(space_storage_bucket, [], ?GLOBALLY_CACHED_LEVEL),
    Config#model_config{version = 3}.

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
    add(SpaceId, StorageId, false).


%%--------------------------------------------------------------------
%% @doc
%% Adds storage to the space.
%% @end
%%--------------------------------------------------------------------
-spec add(od_space:id(), storage:id(), boolean()) ->
    {ok, od_space:id()} | {error, Reason :: term()}.
add(SpaceId, StorageId, MountInRoot) ->
    Doc = new(SpaceId, StorageId, MountInRoot),
    Diff = fun(#space_storage{
        storage_ids = StorageIds,
        mounted_in_root = MountedInRoot
    } = Model) ->
        case lists:member(StorageId, StorageIds) of
            true -> {error, already_exists};
            false ->
                SpaceStorage = Model#space_storage{
                    storage_ids = [StorageId | StorageIds]
                },
                case MountInRoot of
                    true ->
                        {ok, SpaceStorage#space_storage{
                            mounted_in_root = [StorageId | MountedInRoot]
                        }};
                    _ ->
                        {ok, SpaceStorage}
                end
        end
    end,

    case model:execute_with_default_context(?MODULE, create_or_update, [Doc, Diff]) of
        {ok, SpaceId} ->
            ok = space_strategies:add_storage(SpaceId, StorageId, MountInRoot),
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

%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage IDs attached to the space that have been mounted in
%% storage root.
%% @end
%%--------------------------------------------------------------------
-spec get_mounted_in_root(model() | doc()) -> [storage:id()].
get_mounted_in_root(#space_storage{mounted_in_root = StorageIds}) ->
    StorageIds;
get_mounted_in_root(#document{value = #space_storage{} = Value}) ->
    get_mounted_in_root(Value).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of storage IDs attached to the space that have been mounted in
%% storage root.
%% @end
%%--------------------------------------------------------------------
-spec is_cleanup_enabled(od_space:id()) -> boolean().
is_cleanup_enabled(SpaceId) ->
    {ok, Doc} = space_storage:get(SpaceId),
    Doc#document.value#space_storage.cleanup_enabled.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns space_storage document.
%% @end
%%--------------------------------------------------------------------
-spec(new(od_space:id(), storage:id(), boolean()) -> doc()).
new(SpaceId, StorageId, true) ->
    #document{key = SpaceId, value = #space_storage{
        storage_ids = [StorageId],
        mounted_in_root = [StorageId]}};
new(SpaceId, StorageId, _) ->
    #document{key = SpaceId, value = #space_storage{storage_ids = [StorageId]}}.


