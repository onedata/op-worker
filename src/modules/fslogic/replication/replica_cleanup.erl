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
-spec clean_replica_files(file_ctx:ctx()) -> ok.
clean_replica_files(FileCtx) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    SpaceDirUuid = file_ctx:get_space_dir_uuid_const(FileCtx),
    {LocalLocations, _FileCtx2} = file_ctx:get_local_file_location_docs(FileCtx),
    RemoveLocation =
        fun(#document{
            key = LocationId,
            value = #file_location{
                storage_id = StorageId,
                file_id = FileId
        }}) ->
            {ok, Storage} = storage:get(StorageId), %todo possible duplicate with fslogic_deletion_worker:delete_file_on_storage
            SFMHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceDirUuid, FileUuid, Storage, FileId),
            storage_file_manager:unlink(SFMHandle),
            file_location:delete(LocationId)
        end,
    lists:foreach(RemoveLocation, LocalLocations).

%%%===================================================================
%%% Internal functions
%%%===================================================================