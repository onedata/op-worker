%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc FSLogic request handlers for ProxyIO helper.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_proxyio).
-author("Konrad Zemek").

-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").

%% API
-export([write/6, read/6]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Writes data to a location uniquely specified by {StorageID, FileID}
%% pair.
%% @end
%%--------------------------------------------------------------------
-spec write(SessId :: session:id(), SpaceId :: file_meta:uuid(),
    StorageId :: storage:id(), FileId :: helpers:file(),
    Offset :: non_neg_integer(), Data :: binary()) ->
    #proxyio_response{}.
write(SessionId, SpaceId, StorageId, FileId, Offset, Data) ->
    {ok, Storage} = storage:get(StorageId),

    SFMHandle =
        storage_file_manager:new_handle(SessionId, SpaceId, Storage, FileId),

    {Status, Response} =
        case storage_file_manager:open(SFMHandle, write) of
            {ok, Handle} ->
                case storage_file_manager:write(Handle, Offset, Data) of
                    {ok, Wrote} ->
                        {
                            #status{code = ?OK},
                            #remote_write_result{wrote = Wrote}
                        };

                    Error1 ->
                        {fslogic_errors:gen_status_message(Error1), undefined}
                end;

            Error2 ->
                {fslogic_errors:gen_status_message(Error2), undefined}
        end,

    #proxyio_response{status = Status, proxyio_response = Response}.


%%--------------------------------------------------------------------
%% @doc
%% Reads data from a location uniquely specified by {StorageID, FileID}
%% pair.
%% @end
%%--------------------------------------------------------------------
-spec read(SessId :: session:id(), SpaceId :: file_meta:uuid(),
    StorageId :: storage:id(), FileId :: helpers:file(),
    Offset :: non_neg_integer(), Size :: pos_integer()) ->
    #proxyio_response{}.
read(SessionId, SpaceId, StorageId, FileId, Offset, Size) ->
    {ok, Storage} = storage:get(StorageId),

    SFMHandle =
        storage_file_manager:new_handle(SessionId, SpaceId, Storage, FileId),

    {Status, Response} =
        case storage_file_manager:open(SFMHandle, read) of
            {ok, Handle} ->
                case storage_file_manager:read(Handle, Offset, Size) of
                    {ok, Data} ->
                        {
                            #status{code = ?OK},
                            #remote_data{data = Data}
                        };
                    Error1 ->
                        {fslogic_errors:gen_status_message(Error1), undefined}
                end;

            Error2 ->
                {fslogic_errors:gen_status_message(Error2), undefined}
        end,

    #proxyio_response{status = Status, proxyio_response = Response}.
