%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides functions operating on file_location.
%%% @end
%%%--------------------------------------------------------------------
-module(location_and_link_utils).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_new_file_location_doc/3, is_location_created/2,
    mark_location_created/3]).
-export([create_imported_file_location/6, update_imported_file_location/2]).
-export([get_cannonical_paths_cache_name/1, invalidate_cannonical_paths_cache/1,
    init_cannonical_paths_cache_group/0, init_cannonical_paths_cache/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates file location of storage file
%% @end
%%--------------------------------------------------------------------
-spec get_new_file_location_doc(file_ctx:ctx(), StorageFileCreated :: boolean(),
    GeneratedKey :: boolean()) -> {file_location:record(), file_ctx:ctx(), boolean()}.
get_new_file_location_doc(FileCtx, StorageFileCreated, GeneratedKey) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {StorageFileId, FileCtx2} = file_ctx:get_new_storage_file_id(FileCtx),
    {StorageId, FileCtx3} = file_ctx:get_storage_id(FileCtx2),
    {Size, FileCtx4} = file_ctx:get_file_size_from_remote_locations(FileCtx3),
    Location = #file_location{
        provider_id = oneprovider:get_id(),
        file_id = StorageFileId,
        storage_id = StorageId,
        uuid = FileUuid,
        space_id = SpaceId,
        storage_file_created = StorageFileCreated,
        size = Size
    },
    LocId = file_location:local_id(FileUuid),
    case fslogic_location_cache:create_location(#document{
        key = LocId,
        value = Location
    }, GeneratedKey) of
        {ok, _LocId} ->
            FileCtx5 = file_ctx:add_file_location(FileCtx4, LocId),
            {Location, FileCtx5, true};
        {error, already_exists} ->
            {#document{value = FileLocation}, FileCtx5} =
                file_ctx:get_local_file_location_doc(FileCtx4),
            {FileLocation, FileCtx5, false}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file location is created.
%% @end
%%--------------------------------------------------------------------
-spec is_location_created(file_meta:uuid(), file_location:id()) -> boolean().
is_location_created(FileUuid, FileLocationId) ->
    case fslogic_location_cache:get_location(FileLocationId, FileUuid, false) of
        {ok, #document{
            value = #file_location{storage_file_created = Created}
        }} ->
            Created;
        {ok, _} ->
            false
    end.

%%--------------------------------------------------------------------
%% @doc
%% Marks that file is created on storage.
%% @end
%%--------------------------------------------------------------------
-spec mark_location_created(file_meta:uuid(), file_location:id(),
    helpers:file_id()) -> {ok, file_location:doc()} | {error, term()}.
mark_location_created(FileUuid, FileLocationId, StorageFileId) ->
    fslogic_location_cache:update_location(FileUuid, FileLocationId,
        fun(FileLocation = #file_location{storage_file_created = false}) ->
            {ok, FileLocation#file_location{storage_file_created = true,
                file_id = StorageFileId}}
        end, false).

%%--------------------------------------------------------------------
%% @doc
%% Creates file_location
%% @end
%%--------------------------------------------------------------------
-spec create_imported_file_location(od_space:id(), storage:id(), file_meta:uuid(),
    file_meta:path(), file_meta:size(), od_user:id()) -> ok.
create_imported_file_location(SpaceId, StorageId, FileUuid, CanonicalPath, Size, OwnerId) ->
    Location = #file_location{
        provider_id = oneprovider:get_id(),
        file_id = CanonicalPath,
        storage_id = StorageId,
        uuid = FileUuid,
        space_id = SpaceId,
        size = Size,
        storage_file_created = true
    },
    LocationDoc = #document{
        key = file_location:local_id(FileUuid),
        value = Location,
        scope = SpaceId
    },
    LocationDoc2 = fslogic_location_cache:set_blocks(LocationDoc, create_file_blocks(Size)),
    {ok, _LocId} = file_location:save_and_bump_version(LocationDoc2, OwnerId),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Updates file_location
%% @end
%%-------------------------------------------------------------------
-spec update_imported_file_location(file_ctx:ctx(), non_neg_integer()) -> ok.
update_imported_file_location(FileCtx, StorageSize) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    NewFileBlocks = create_file_blocks(StorageSize),
    replica_updater:update(FileCtx, NewFileBlocks, StorageSize, true),
    ok = lfm_event_emitter:emit_file_written(
        FileGuid, NewFileBlocks, StorageSize, {exclude, []}).

%%-------------------------------------------------------------------
%% @doc
%% Gets name of cache for particular space.
%% @end
%%-------------------------------------------------------------------
-spec get_cannonical_paths_cache_name(od_space:id()) -> atom().
get_cannonical_paths_cache_name(Space) ->
    binary_to_atom(<<"cannonical_paths_cache_", Space/binary>>, utf8).

%%-------------------------------------------------------------------
%% @doc
%% Invalidates cache for particular space.
%% @end
%%-------------------------------------------------------------------
-spec invalidate_cannonical_paths_cache(od_space:id()) -> ok.
invalidate_cannonical_paths_cache(Space) ->
    ok = bounded_cache:invalidate(get_cannonical_paths_cache_name(Space)).

%%-------------------------------------------------------------------
%% @doc
%% Initializes caches' group.
%% @end
%%-------------------------------------------------------------------
-spec init_cannonical_paths_cache_group() -> ok.
init_cannonical_paths_cache_group() ->
    CheckFrequency = application:get_env(?APP_NAME, cannonical_paths_cache_frequency, 30000),
    Size = application:get_env(?APP_NAME, cannonical_paths_cache_size, 20000),
    ok = bounded_cache:init_group(<<"cannonical_paths_cache">>, #{
        check_frequency => CheckFrequency,
        size => Size,
        worker => true
    }).

%%-------------------------------------------------------------------
%% @doc
%% Initializes cache for particular space or all spaces.
%% @end
%%-------------------------------------------------------------------
-spec init_cannonical_paths_cache(od_space:id() | all) -> ok.
init_cannonical_paths_cache(all) ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            lists:foreach(fun(Space) ->
                ok = init_cannonical_paths_cache(Space)
            end, SpaceIds);
        ?ERROR_NO_CONNECTION_TO_OZ ->
            ?debug("Unable to initialize cannonical_paths bounded caches due to: ~p", [?ERROR_NO_CONNECTION_TO_OZ]);
        ?ERROR_UNREGISTERED_PROVIDER ->
            ?debug("Unable to initialize cannonical_paths bounded caches due to: ~p", [?ERROR_UNREGISTERED_PROVIDER]);
        Error = {error, _} ->
            ?critical("Unable to initialize cannonical_paths bounded caches due to: ~p", [Error])
    catch
        Error2:Reason ->
            ?critical_stacktrace("Unable to initialize cannonical_paths bounded caches due to: ~p", [{Error2, Reason}])
    end;
init_cannonical_paths_cache(Space) ->
    try
        Name = get_cannonical_paths_cache_name(Space),
        case bounded_cache:cache_exists(Name) of
            true ->
                ok;
            _ ->
                case bounded_cache:init_cache(Name, #{group => <<"cannonical_paths_cache">>}) of
                    ok ->
                        ok;
                    Error = {error, _} ->
                        ?critical("Unable to initialize cannonical_paths bounded cache for space ~p due to: ~p",
                            [Space, Error])
                end
        end
    catch
        Error2:Reason ->
            ?critical_stacktrace("Unable to initialize cannonical_paths bounded cache for space ~p due to: ~p",
                [Space, {Error2, Reason}])
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns list containing one block with given size.
%% Is Size == 0 returns empty list.
%% @end
%%-------------------------------------------------------------------
-spec create_file_blocks(non_neg_integer()) -> fslogic_blocks:blocks().
create_file_blocks(0) -> [];
create_file_blocks(Size) -> [#file_block{offset = 0, size = Size}].