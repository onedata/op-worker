%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding files' location data.
%%% @end
%%%-------------------------------------------------------------------
-module(file_location).
-author("Rafal Slota").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("proto/oneclient/common_messages.hrl").

% API
-export([local_id/1, id/2, critical_section/2, save_and_bump_version/1,
    is_storage_file_created/1, get/2, get_local/1, get_blocks/1,
    get_version_vector/1]).
-export([create/1, create/2, save/1, get/1, update/2, delete/1, delete/2]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type id() :: datastore:id().
-type record() :: #file_location{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-export_type([id/0, doc/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns id of local file location
%% @end
%%--------------------------------------------------------------------
-spec local_id(file_meta:uuid()) -> file_location:id().
local_id(FileUuid) ->
    id(FileUuid, oneprovider:get_id()).

%%--------------------------------------------------------------------
%% @doc
%% Returns id of local file location
%% @end
%%--------------------------------------------------------------------
-spec id(file_meta:uuid(), od_provider:id()) -> file_location:id().
id(FileUuid, ProviderId) ->
    datastore_utils:gen_key(ProviderId, FileUuid).

%%--------------------------------------------------------------------
%% @doc
%% Runs given function within locked ResourceId. This function makes sure that
%% 2 funs with same ResourceId won't run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec critical_section(ResourceId :: binary(), Fun :: fun(() -> term())) ->
    term().
critical_section(ResourceId, Fun) ->
    critical_section:run([?MODULE, ResourceId], Fun).

%%--------------------------------------------------------------------
%% @doc
%% Increases version in version_vector and save document.
%% @end
%%--------------------------------------------------------------------
-spec save_and_bump_version(doc()) -> {ok, doc()} | {error, term()}.
save_and_bump_version(FileLocationDoc) ->
    fslogic_blocks:save_location(version_vector:bump_version(FileLocationDoc)).

%%--------------------------------------------------------------------
%% @doc
%% Creates file location.
%% @equiv create(Doc, false)
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, doc()} | {error, term()}.
create(Doc) ->
    create(Doc, false).

%%--------------------------------------------------------------------
%% @doc
%% Creates file location.
%% @end
%%--------------------------------------------------------------------
-spec create(doc(), boolean()) -> {ok, doc()} | {error, term()}.
create(Doc = #document{value = #file_location{
    uuid = FileUuid,
    space_id = SpaceId
}}, GeneratedKey) ->
    NewSize = count_bytes(Doc),
    space_quota:apply_size_change_and_maybe_emit(SpaceId, NewSize),
    case get_owner_id(FileUuid) of
        {ok, UserId} ->
            monitoring_event:emit_storage_used_updated(SpaceId, UserId, NewSize);
        {error, not_found} ->
            ok
    end,
    case GeneratedKey of
        true ->
            ?extract_key(datastore_model:save(?CTX#{generated_key => GeneratedKey},
                Doc#document{scope = SpaceId}));
        _ ->
            ?extract_key(datastore_model:create(?CTX,
                Doc#document{scope = SpaceId}))
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves file location.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc = #document{key = Key, value = #file_location{
    uuid = FileUuid,
    space_id = SpaceId
}}) ->
    NewSize = count_bytes(Doc),
    {ok, UserId} = get_owner_id(FileUuid),
    case datastore_model:get(?CTX, Key) of
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
    ?extract_key(datastore_model:save(?CTX, Doc#document{scope = SpaceId})).

%%--------------------------------------------------------------------
%% @doc
%% Returns file location.
%% @end
%%--------------------------------------------------------------------
-spec get(id()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns file location associated with given FileUuid and ProviderId.
%% @end
%%--------------------------------------------------------------------
-spec get(file_meta:uuid(), od_provider:id()) -> {ok, doc()} | {error, term()}.
get(FileUuid, ProviderId) ->
    ?MODULE:get(?MODULE:id(FileUuid, ProviderId)).

%%--------------------------------------------------------------------
%% @doc
%% @equiv get(FileUuid, oneprovider:get_id()).
%% @end
%%--------------------------------------------------------------------
-spec get_local(file_meta:uuid()) -> {ok, doc()} | {error, term()}.
get_local(FileUuid) ->
    ?MODULE:get(FileUuid, oneprovider:get_id()).

%%--------------------------------------------------------------------
%% @doc
%% Updates file location.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(Key, Diff) ->
    datastore_model:update(?CTX, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Deletes file location.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    case datastore_model:get(?CTX, Key) of
        {ok, Doc = #document{value = #file_location{
            uuid = FileUuid,
            space_id = SpaceId
        }}} ->
            Size = count_bytes(Doc),
            space_quota:apply_size_change_and_maybe_emit(SpaceId, -1 * Size),
            {ok, UserId} = get_owner_id(FileUuid),
            monitoring_event:emit_storage_used_updated(SpaceId, UserId, -1 * Size);
        _ ->
            ok
    end,
   datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes doc and emits event for owner.
%% @end
%%--------------------------------------------------------------------
-spec delete(id(), od_user:id()) -> ok | {error, term()}.
delete(Key, UserId) ->
    case datastore_model:get(?CTX, Key) of
        {ok, Doc = #document{value = #file_location{space_id = SpaceId}}} ->
            Size = count_bytes(Doc),
            space_quota:apply_size_change_and_maybe_emit(SpaceId, -1 * Size),
            monitoring_event:emit_storage_used_updated(SpaceId, UserId, -1 * Size);
        _ ->
            ok
    end,
    datastore_model:delete(?CTX, Key).

%%-------------------------------------------------------------------
%% @doc
%% Returns value of storage_file_created field
%% @end
%%-------------------------------------------------------------------
-spec is_storage_file_created(doc() | record() | undefined) -> boolean().
is_storage_file_created(undefined) ->
    false;
is_storage_file_created(#file_location{storage_file_created = StorageFileCreated}) ->
    StorageFileCreated;
is_storage_file_created(#document{value=FileLocation}) ->
    is_storage_file_created(FileLocation).

%%-------------------------------------------------------------------
%% @doc
%% Getter for blocks field of #file_location{} record.
%% @end
%%-------------------------------------------------------------------
-spec get_blocks(doc() | record()) -> fslogic_blocks:blocks().
get_blocks(#document{value = FileLocation}) ->
    get_blocks(FileLocation);
get_blocks(#file_location{blocks = Blocks}) ->
    Blocks.

%-------------------------------------------------------------------
%% @doc
%% Getter for version_vector field of #file_location{} record.
%% @end
%%-------------------------------------------------------------------
-spec get_version_vector(doc() | record()) -> version_vector:version_vector().
get_version_vector(#document{value = FileLocation}) ->
    get_version_vector(FileLocation);
get_version_vector(#file_location{version_vector = VV}) ->
    VV.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns total size used by given file_location.
%% @end
%%--------------------------------------------------------------------
-spec count_bytes(doc()) -> non_neg_integer().
count_bytes(#document{value = #file_location{blocks = Blocks}}) ->
    count_bytes(Blocks, 0).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns total size used by given file_location.
%% @end
%%--------------------------------------------------------------------
-spec count_bytes([fslogic_blocks:block()], non_neg_integer()) -> 
    non_neg_integer().
count_bytes([], Size) ->
    Size;
count_bytes([#file_block{size = Size} | T], TotalSize) ->
    count_bytes(T, TotalSize + Size).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Return user id for given file uuid.
%% @end
%%--------------------------------------------------------------------
-spec get_owner_id(file_meta:uuid()) -> {ok, od_user:id()} | {error, term()}.
get_owner_id(FileUuid) ->
    case file_meta:get_including_deleted(FileUuid) of
        {ok, #document{value = #file_meta{owner = UserId}}} ->
            {ok, UserId};
        {error, Reason} ->
            {error, Reason}
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

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    3.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
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
get_record_struct(2) ->
    {record, Struct} = get_record_struct(1),
    {record, proplists:delete(handle_id, Struct)};
get_record_struct(3) ->
    {record, Struct} = get_record_struct(2),
    {record, Struct ++ [{storage_file_created, boolean}]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, Uuid, ProviderId, StorageId, FileId, Blocks,
    VersionVector, Size, _HandleId, SpaceId, RecentChanges, LastRename}) ->
    {2, #file_location{
        uuid = Uuid, provider_id = ProviderId, storage_id = StorageId,
        file_id = FileId, blocks = Blocks, version_vector = VersionVector,
        size = Size, space_id = SpaceId, recent_changes = RecentChanges,
        last_rename = LastRename
    }};
upgrade_record(2, {?MODULE, Uuid, ProviderId, StorageId, FileId, Blocks,
    VersionVector, Size, SpaceId, RecentChanges, LastRename}) ->
    {3, #file_location{
        uuid = Uuid, provider_id = ProviderId, storage_id = StorageId,
        file_id = FileId, blocks = Blocks, version_vector = VersionVector,
        size = Size, space_id = SpaceId, recent_changes = RecentChanges,
        last_rename = LastRename, storage_file_created = true
    }}.