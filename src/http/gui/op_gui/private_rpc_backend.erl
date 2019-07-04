%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements callback_backend_behaviour.
%%% It is used to handle RPC calls from clients with active session.
%%% @end
%%%-------------------------------------------------------------------
-module(private_rpc_backend).
-behaviour(rpc_backend_behaviour).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([handle/2]).

%%%===================================================================
%%% rpc_backend_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link rpc_backend_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(FunctionId :: binary(), RequestData :: term()) ->
    ok | {ok, ResponseData :: term()} | op_gui_error:error_result().
%%--------------------------------------------------------------------
%% File upload related procedures
%%--------------------------------------------------------------------
handle(<<"fileUploadSuccess">>, Props) ->
    SessionId = op_gui_session:get_session_id(),
    UploadId = proplists:get_value(<<"uploadId">>, Props),
    ParentId = proplists:get_value(<<"parentId">>, Props),
    {FileId, FileHandle} = page_file_upload:wait_for_file_new_file_id(SessionId, UploadId),
    page_file_upload:upload_map_delete(SessionId, UploadId),
    ok = lfm:fsync(FileHandle),
    ok = lfm:release(FileHandle),
    file_data_backend:report_file_upload(FileId, ParentId);

handle(<<"fileUploadFailure">>, Props) ->
    SessionId = op_gui_session:get_session_id(),
    UploadId = proplists:get_value(<<"uploadId">>, Props),
    page_file_upload:upload_map_delete(SessionId, UploadId),
    ok;

handle(<<"fileBatchUploadComplete">>, Props) ->
    ParentId = proplists:get_value(<<"parentId">>, Props),
%%    page_file_upload:clean_upload_map(op_gui_session:get_session_id()),
    file_data_backend:report_file_batch_complete(ParentId);

% Checks if file can be downloaded (i.e. can be read by the user) and if so,
% returns download URL.
handle(<<"getFileDownloadUrl">>, [{<<"fileId">>, FileId}]) ->
    SessionId = op_gui_session:get_session_id(),
    case page_file_download:get_file_download_url(SessionId, FileId) of
        {ok, URL} ->
            {ok, [{<<"fileUrl">>, URL}]};
        ?ERROR_FORBIDDEN ->
            gui_error:report_error(<<"Permission denied">>);
        {error, ?ENOENT} ->
            gui_error:report_error(<<"File does not exist or was deleted - try refreshing the page.">>);
        Error ->
            ?debug("Cannot resolve file download url for file ~p - ~p", [FileId, Error]),
            gui_error:report_error(<<"Cannot resolve file download url - try refreshing the page.">>)
    end;

% Checks if file that is displayed in shares view can be downloaded
% (i.e. can be read by the user) and if so, returns download URL.
handle(<<"getSharedFileDownloadUrl">>, [{<<"fileId">>, AssocId}]) ->
    {_, FileId} = op_gui_utils:association_to_ids(AssocId),
    handle(<<"getFileDownloadUrl">>, [{<<"fileId">>, FileId}]);

%%--------------------------------------------------------------------
%% File manipulation procedures
%%--------------------------------------------------------------------

handle(<<"createFile">>, Props) ->
    SessionId = op_gui_session:get_session_id(),
    Name = proplists:get_value(<<"fileName">>, Props),
    ParentId = proplists:get_value(<<"parentId">>, Props, null),
    Type = proplists:get_value(<<"type">>, Props),
    case file_data_backend:create_file(SessionId, Name, ParentId, Type) of
        {ok, FileId} ->
            {ok, [{<<"fileId">>, FileId}]};
        Error ->
            Error
    end;

handle(<<"fetchMoreDirChildren">>, Props) ->
    SessionId = op_gui_session:get_session_id(),
    file_data_backend:fetch_more_dir_children(SessionId, Props);

handle(<<"createFileShare">>, Props) ->
    SessionId = op_gui_session:get_session_id(),
    UserId = op_gui_session:get_user_id(),
    FileId = proplists:get_value(<<"fileId">>, Props),
    Name = proplists:get_value(<<"shareName">>, Props),
    case lfm:create_share(SessionId, {guid, FileId}, Name) of
        {ok, {ShareId, _}} ->
            op_gui_async:push_updated(
                <<"user">>,
                user_data_backend:user_record(SessionId, UserId)
            ),
            % Push file data so GUI knows that is is shared
            {ok, FileData} = file_data_backend:file_record(SessionId, FileId),
            op_gui_async:push_created(<<"file">>, FileData),
            {ok, [{<<"shareId">>, ShareId}]};
        {error, ?EACCES} ->
            op_gui_error:report_warning(<<"You do not have permissions to "
            "manage shares in this space.">>);
        _ ->
            op_gui_error:report_warning(
                <<"Cannot create share due to unknown error.">>)
    end;

%%--------------------------------------------------------------------
%% Transfer related procedures
%%--------------------------------------------------------------------

handle(<<"getSpaceTransfers">>, Props) ->
    SpaceId = proplists:get_value(<<"spaceId">>, Props),
    Type = proplists:get_value(<<"type">>, Props),
    StartFromIndex = proplists:get_value(<<"startFromIndex">>, Props, null),
    Offset = proplists:get_value(<<"offset">>, Props, 0),
    Limit = proplists:get_value(<<"size">>, Props, all),
    transfer_data_backend:list_transfers(SpaceId, Type, StartFromIndex, Offset, Limit);

handle(<<"getTransfersForFile">>, Props) ->
    FileGuid = proplists:get_value(<<"fileId">>, Props),
    EndedInfo = proplists:get_value(<<"endedInfo">>, Props, <<"count">>),
    {ok, Result} = transfer_data_backend:get_transfers_for_file(FileGuid),
    case EndedInfo of
        <<"count">> ->
            EndedCount = length(proplists:get_value(ended, Result)),
            {ok, [{ended, EndedCount} | proplists:delete(ended, Result)]};
        <<"ids">> ->
            {ok, Result}
    end;

handle(<<"cancelTransfer">>, Props) ->
    TransferId = proplists:get_value(<<"transferId">>, Props),
    transfer_data_backend:cancel_transfer(TransferId);

handle(<<"rerunTransfer">>, Props) ->
    SessionId = op_gui_session:get_session_id(),
    TransferId = proplists:get_value(<<"transferId">>, Props),
    transfer_data_backend:rerun_transfer(SessionId, TransferId);

handle(_, _) ->
    op_gui_error:report_error(<<"Not implemented">>).
