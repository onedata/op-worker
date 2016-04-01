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
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([write/5, read/6]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Writes data to a location uniquely specified by {StorageID, FileID}
%% pair.
%% @end
%%--------------------------------------------------------------------
-spec write(SessId :: session:id(), FileUuid :: file_meta:uuid(),
    StorageId :: storage:id(), FileId :: helpers:file(),
    ByteSequence :: #byte_sequence{}) ->
    #proxyio_response{}.
write(SessionId, FileUuid, StorageId, FileId, ByteSequence) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} =
        session:get(SessionId),
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space({uuid, FileUuid}, UserId),
    {ok, Storage} = storage:get(StorageId),

    SFMHandle =
        storage_file_manager:new_handle(SessionId, SpaceUUID, FileUuid, Storage, FileId),

    {ok, Handle} = storage_file_manager:open(SFMHandle, write),
    Wrote =
        lists:foldl(fun(#byte_sequence{offset = Offset, data = Data}, WroteAcc) ->
            {ok, WroteNow} = write_all(Handle, Offset, Data),
            WroteAcc + WroteNow
        end, 0, ByteSequence),

    #proxyio_response{status = #status{code = ?OK},
                      proxyio_response = #remote_write_result{wrote = Wrote}}.


write_all(Handle, Offset, Data) -> write_all(Handle, Offset, Data, 0).
write_all(Handle, Offset, <<>>, Wrote) -> {ok, Wrote};
write_all(Handle, Offset, Data, Wrote) ->
    {ok, WroteNow} = storage_file_manager:write(Handle, Offset, Data),
    write_all(Handle, Offset + WroteNow,
              binary_part(Data, {byte_size(Data), WroteNow - byte_size(Data)}),
              WroteNow).


%%--------------------------------------------------------------------
%% @doc
%% Reads data from a location uniquely specified by {StorageID, FileID}
%% pair.
%% @end
%%--------------------------------------------------------------------
-spec read(SessId :: session:id(), FileUuid :: file_meta:uuid(),
    StorageId :: storage:id(), FileId :: helpers:file(),
    Offset :: non_neg_integer(), Size :: pos_integer()) ->
    #proxyio_response{}.
read(SessionId, FileUuid, StorageId, FileId, Offset, Size) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} =
        session:get(SessionId),
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space({uuid, FileUuid}, UserId),
    {ok, Storage} = storage:get(StorageId),

    SFMHandle =
        storage_file_manager:new_handle(SessionId, SpaceUUID, FileUuid, Storage, FileId),

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
