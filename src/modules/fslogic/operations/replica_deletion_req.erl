%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling requests for deleting file
%%% replicas.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_deletion_req).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([delete_blocks/3]).


%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Deletes given Blocks in file associated with given FileCtx.
%% Before deletion, checks whether current Version is equal or
%% lesser than AllowedVV
%% @end
%%-------------------------------------------------------------------
-spec delete_blocks(file_ctx:ctx(), [sync_req:block()],
    version_vector:version_vector()) -> ok | {error, term()}.
delete_blocks(FileCtx, _Blocks, AllowedVV) ->
    %todo VFS-3728 implement deletion of file parts (blocks)
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    file_location:critical_section(FileUuid, fun() ->
        delete_whole_file_replica(FileCtx, AllowedVV)
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes replica of file.
%% Before deletion checks whether version of local replica is identical
%% or lesser to allowed.
%% NOTE!!! This function is not responsible whether given replica is
%% unique
%% @end
%%--------------------------------------------------------------------
-spec delete_whole_file_replica(file_ctx:ctx(), version_vector:version_vector())
        -> ok | {error, term()}.
delete_whole_file_replica(FileCtx, AllowedVV) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    try
        LocalFileId = file_location:local_id(FileUuid),
        {ok, LocDoc} = file_location:get(LocalFileId),
        CurrentVV = file_location:get_version_vector(LocDoc),
        case version_vector:compare(CurrentVV, AllowedVV) of
            ComparisonResult when
                ComparisonResult =:= identical orelse
                ComparisonResult =:= lesser
            ->
                UserCtx = user_ctx:new(?ROOT_SESS_ID),
                #fuse_response{status = #status{code = ?OK}} =
                    truncate_req:truncate_insecure(UserCtx, FileCtx, 0, false),
                %todo VFS-4433 file_popularity should be updated after updates on file_location
                case fslogic_blocks:delete_location(FileUuid, LocalFileId) of
                    ok ->
                        ok;
                    {error, {not_found, _}} ->
                        ok
                end,
                fslogic_event_emitter:emit_file_location_changed(FileCtx, []);
            _ ->
                {error, file_modified_locally}
        end
    catch
        Error:Reason ->
            ?error_stacktrace("Deletion of replica of file ~p failed due to ~p",
                [FileUuid, {Error, Reason}]),
            {error, Reason}
    end.