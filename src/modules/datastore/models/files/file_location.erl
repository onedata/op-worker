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
-export([critical_section/2, save_and_bump_version/1, ensure_blocks_not_empty/1]).
-export([record_struct/1, record_upgrade/2]).

-type id() :: binary().
-type doc() :: datastore:document().

-export_type([id/0, doc/0]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {uuid, string},
        {provider_id, string},
        {storage_id, string},
        {file_id, string},
        {blocks, [term]},
        {version_vector, #{term => integer}},
        {size, integer},
        {handle_id, string},
        {space_id, string},
        {recent_changes, {[term], [term]}},
        {last_rename, {{string, string}, integer}}
    ]};
record_struct(2) ->
    {record, Struct} = record_struct(1),
    {record, proplists:delete(handle_id, Struct)}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades record from specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_upgrade(datastore_json:record_version(), tuple()) ->
    {datastore_json:record_version(), tuple()}.
record_upgrade(1, {?MODEL_NAME, Uuid, ProviderId, StorageId, FileId, Blocks,
    VersionVector, Size, _HandleId, SpaceId, RecentChanges, LastRename}) ->
    {2, #file_location{
        uuid = Uuid, provider_id = ProviderId, storage_id = StorageId,
        file_id = FileId, blocks = Blocks, version_vector = VersionVector,
        size = Size, space_id = SpaceId, recent_changes = RecentChanges,
        last_rename = LastRename
    }}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Runs given function within locked ResourceId. This function makes sure that 2 funs with same ResourceId won't
%% run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec critical_section(ResourceId :: binary(), Fun :: fun(() -> Result :: term())) -> Result :: term().
critical_section(ResourceId, Fun) ->
    critical_section:run([?MODEL_NAME, ResourceId], Fun).

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
ensure_blocks_not_empty(Loc = #file_location{blocks = []}) ->
    Loc#file_location{blocks = [#file_block{offset = 0, size = 0}]};
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
save(Document = #document{key = Key, value = #file_location{uuid = Uuid, space_id = SpaceId}}) ->
    NewSize = count_bytes(Document),
    UserId = get_user_id(Uuid),
    case get(Key) of
        {ok, #document{value = #file_location{space_id = SpaceId}} = OldDoc} ->
            OldSize = count_bytes(OldDoc),
            space_quota:apply_size_change_and_maybe_emit(SpaceId, NewSize - OldSize),
            monitoring_event:emit_storage_used_updated(SpaceId, UserId, NewSize - OldSize);

        {ok, #document{value = #file_location{space_id = OldSpaceId}} = OldDoc} ->
            OldSize = count_bytes(OldDoc),

            space_quota:apply_size_change_and_maybe_emit(OldSpaceId, -1 * OldSize),
            monitoring_event:emit_storage_used_updated(OldSpaceId, UserId, -1 * OldSize),

            space_quota:apply_size_change_and_maybe_emit(SpaceId, NewSize),
            monitoring_event:emit_storage_used_updated(SpaceId, UserId, NewSize);
        _ ->
            space_quota:apply_size_change_and_maybe_emit(SpaceId, NewSize),
            monitoring_event:emit_storage_used_updated(SpaceId, UserId, NewSize)
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
create(Document = #document{value = #file_location{uuid = Uuid, space_id = SpaceId}}) ->
    NewSize = count_bytes(Document),
    UserId = get_user_id(Uuid),

    space_quota:apply_size_change_and_maybe_emit(SpaceId, NewSize),
    monitoring_event:emit_storage_used_updated(SpaceId, UserId, NewSize),

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
        {ok, #document{value = #file_location{uuid = Uuid, space_id = SpaceId}} = Doc} ->
            Size = count_bytes(Doc),
            UserId = get_user_id(Uuid),
            space_quota:apply_size_change_and_maybe_emit(SpaceId, -1 * Size),
            monitoring_event:emit_storage_used_updated(SpaceId, UserId, -1 * Size);
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
    Config = ?MODEL_CONFIG(file_locations_bucket, [], ?GLOBALLY_CACHED_LEVEL),
    Config#model_config{
        version = 2,
        sync_enabled = true
    }.

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


%%--------------------------------------------------------------------
%% @doc
%% Returns total size used by given file_location.
%% @end
%%--------------------------------------------------------------------
-spec count_bytes(datastore:doc() | fslogic_blocks:blocks()) ->
    Size :: non_neg_integer().
count_bytes(#document{value = #file_location{blocks = Blocks}}) ->
    count_bytes(Blocks, 0).

count_bytes([], Size) ->
    Size;
count_bytes([#file_block{size = Size} | T], TotalSize) ->
    count_bytes(T, TotalSize + Size).

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Return user id for given file uuid.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(file_meta:uuid()) -> datastore:id().
get_user_id(FileUuid) ->
    {ok, #document{value = #file_meta{owner = UserId}}} =
        file_meta:get({uuid, FileUuid}),
    UserId.