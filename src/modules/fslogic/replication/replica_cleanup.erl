%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Functions that clean replicated files, their metadata, locations and storage files.
%%% @end
%%%--------------------------------------------------------------------
-module(replica_cleanup).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([clean_replica_files/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% List all file_locations, remove them with associated storage files.
%% @end
%%--------------------------------------------------------------------
-spec clean_replica_files(file_meta:uuid()) -> ok.
clean_replica_files(FileUuid) ->
    LocalProviderId = oneprovider:get_provider_id(),
    {ok, Locations} = file_meta:get_locations_by_uuid(FileUuid),
    RemoveLocation =
        fun(LocationId) ->
            case file_location:get(LocationId) of
                {ok, #document{value = #file_location{storage_id = StorageId, file_id = FileId,
                    provider_id = LocalProviderId, space_id = SpaceId}}} ->
                    {ok, Storage} = storage:get(StorageId),
                    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
                    SFMHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceUuid, FileUuid, Storage, FileId),
                    storage_file_manager:unlink(SFMHandle),
                    file_location:delete(LocationId);
                {ok, _} ->
                    ok;
                Error ->
                    ?error("Cannot get file_location ~p, due to ~p", [LocationId, Error])
            end
        end,
    lists:foreach(RemoveLocation, Locations).

%%%===================================================================
%%% Internal functions
%%%===================================================================