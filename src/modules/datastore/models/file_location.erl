%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding files' location data.
%%% @end
%%%-------------------------------------------------------------------
-module(file_location).
-author("Rafal Slota").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).
-export([run_synchronized/2, save_and_bump_version/1, ensure_blocks_not_empty/1]).

-type id() :: binary().
-type doc() :: datastore:document().

-export_type([id/0, doc/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Runs given function within locked ResourceId. This function makes sure that 2 funs with same ResourceId won't
%% run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec run_synchronized(ResourceId :: binary(), Fun :: fun(() -> Result :: term())) -> Result :: term().
run_synchronized(ResourceId, Fun) ->
    datastore:run_synchronized(?MODEL_NAME, ResourceId, Fun).

%%--------------------------------------------------------------------
%% @doc
%% Increase version in version_vector and save document.
%% @end
%%--------------------------------------------------------------------
-spec save_and_bump_version(doc()) -> {ok, datastore:key()} | datastore:generic_error().
save_and_bump_version(FileLocationDoc) ->
    file_location:save(version_vector:bump_version(FileLocationDoc)).

%%--------------------------------------------------------------------
%% @doc
%% Ensures that blocks of file location contains at least one entry (so client
%% will know the storageId and fileId of file). If blocks are empty, function adds
%% one block with offset and size set to 0.
%% @end
%%--------------------------------------------------------------------
-spec ensure_blocks_not_empty(#file_location{}) -> #file_location{}.
ensure_blocks_not_empty(Loc = #file_location{blocks = [], file_id = FileId, storage_id = StorageId}) ->
    Loc#file_location{blocks = [#file_block{offset = 0, size = 0,
        storage_id = StorageId, file_id = FileId}]};
ensure_blocks_not_empty(Loc) ->
    Loc.

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
save(Document = #document{key = Key, value = #file_location{space_id = SpaceId}}) ->
    NewSize = count_bytes(Document),
    case get(Key) of
        {ok, #document{value = #file_location{space_id = SpaceId}} = OldDoc} ->
            OldSize = count_bytes(OldDoc),
            space_quota:apply_size_change_and_maybe_emit(SpaceId,  NewSize - OldSize);
        {ok, #document{value = #file_location{space_id = OldSpaceId}} = OldDoc} ->
            OldSize = count_bytes(OldDoc),
            space_quota:apply_size_change_and_maybe_emit(OldSpaceId,  -1 * OldSize),
            space_quota:apply_size_change_and_maybe_emit(SpaceId,  NewSize);
        _ ->
            space_quota:apply_size_change_and_maybe_emit(SpaceId,  NewSize)
    end,
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
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
create(Document = #document{value = #file_location{space_id = SpaceId}}) ->
    NewSize = count_bytes(Document),
    space_quota:apply_size_change_and_maybe_emit(SpaceId, NewSize),
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
    case get(Key) of
        {ok, #document{value = #file_location{space_id = SpaceId}} = Doc} ->
            space_quota:apply_size_change_and_maybe_emit(SpaceId, -1 * count_bytes(Doc));
        _ ->
            ok
    end,
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
    ?MODEL_CONFIG(file_locations_bucket, [], ?DISK_ONLY_LEVEL). % todo fix links and use GLOBALLY_CACHED

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

count_bytes(#document{value = #file_location{blocks = Blocks}}) ->
    count_bytes(Blocks, 0).

count_bytes([], Size) ->
    Size;
count_bytes([#file_block{size = Size} | T], TotalSize) ->
    count_bytes(T, TotalSize + Size).