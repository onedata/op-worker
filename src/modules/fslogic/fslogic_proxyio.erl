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

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").

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
-spec write(SessId :: session:id(), Parameters :: #{binary() => binary()},
    StorageId :: storage:id(), FileId :: helpers:file(),
    Offset :: non_neg_integer(), Data :: binary()) ->
    #proxyio_response{}.
write(SessionId, Parameters, StorageId, FileId, Offset, Data) ->

    {Status, Response} =
        case get_handle(SessionId, Parameters, StorageId, FileId, write) of
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
-spec read(SessionId :: session:id(), Parameters :: #{binary() => binary()},
    StorageId :: storage:id(), FileId :: helpers:file(),
    Offset :: non_neg_integer(), Size :: pos_integer()) ->
    #proxyio_response{}.
read(SessionId, Parameters, StorageId, FileId, Offset, Size) ->

    {Status, Response} =
        case get_handle(SessionId, Parameters, StorageId, FileId, read) of
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns handle by either retrieving it from session or opening file
%% @end
%%--------------------------------------------------------------------
-spec get_handle(SessionId :: session:id(), Parameters :: #{binary() => binary()},
    StorageId :: storage:id(), FileId :: helpers:file(), OpenMode :: helpers:open_mode()) ->
    {ok, storage_file_manager:handle()} | logical_file_manager:error_reply().
get_handle(SessionId, Parameters, StorageId, FileId, OpenMode)->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}, handles = Handles}}} =
        session:get(SessionId),
    case maps:get(?HANDLE_ID_KEY, Parameters, undefined) of
        undefined ->
            FileUuid = maps:get(?FILE_UUID_KEY, Parameters),
            {ok, #document{key = SpaceUUID}} =
                fslogic_spaces:get_space({uuid, FileUuid}, UserId),
            {ok, Storage} = storage:get(StorageId),
            SFMHandle =
                storage_file_manager:new_handle(SessionId, SpaceUUID, FileUuid, Storage, FileId),
            storage_file_manager:open(SFMHandle, OpenMode);
        HandleId ->
            {ok, maps:get(HandleId, Handles)}
    end.