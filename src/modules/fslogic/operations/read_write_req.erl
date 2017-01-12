%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% FSLogic request handlers for ProxyIO helper.
%%% @end
%%%-------------------------------------------------------------------
-module(read_write_req).
-author("Konrad Zemek").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").

%% API
-export([write/6, read/7]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Reads data from a location uniquely specified by {StorageID, FileID} pair.
%% @end
%%--------------------------------------------------------------------
-spec read(user_ctx:ctx(), file_ctx:ctx(), HandleId :: storage_file_manager:handle_id(),
    StorageId :: storage:id(), FileId :: helpers:file(),
    Offset :: non_neg_integer(), Size :: pos_integer()) ->
    fslogic_worker:proxyio_response().
read(UserCtx, FileCtx, HandleId, StorageId, FileId, Offset, Size) ->
    #fuse_response{status = #status{code = ?OK}} =
        synchronization_req:synchronize_block(UserCtx, FileCtx, #file_block{offset = Offset, size = Size}, false),
    {ok, Handle} =  get_handle(UserCtx, FileCtx, HandleId, StorageId, FileId, read),
    {ok, Data} = storage_file_manager:read(Handle, Offset, Size),
    #proxyio_response{status = #status{code = ?OK}, proxyio_response = #remote_data{data = Data}}.

%%--------------------------------------------------------------------
%% @doc
%% Writes data to a location uniquely specified by {StorageID, FileID} pair.
%% @end
%%--------------------------------------------------------------------
-spec write(user_ctx:ctx(), file_ctx:ctx(),
    HandleId :: storage_file_manager:handle_id(), StorageId :: storage:id(),
    FileId :: helpers:file(), ByteSequences :: [#byte_sequence{}]) ->
    fslogic_worker:proxyio_response().
write(UserCtx, FileCtx, HandleId, StorageId, FileId, ByteSequences) ->
    {ok, Handle} = get_handle(UserCtx, FileCtx, HandleId, StorageId, FileId, write),
    Wrote =
        lists:foldl(fun(#byte_sequence{offset = Offset, data = Data}, Acc) ->
            Acc + write_all(Handle, Offset, Data, 0)
        end, 0, ByteSequences),

    #proxyio_response{status = #status{code = ?OK},
                      proxyio_response = #remote_write_result{wrote = Wrote}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns handle by either retrieving it from session or opening file
%% @end
%%--------------------------------------------------------------------
-spec get_handle(user_ctx:ctx(), file_ctx:ctx(), HandleId :: storage_file_manager:handle_id(),
    StorageId :: storage:id(), FileId :: helpers:file(), OpenFlag :: helpers:open_flag()) ->
    {ok, storage_file_manager:handle()} | logical_file_manager:error_reply().
get_handle(UserCtx, FileCtx, undefined, StorageId, FileId, OpenFlag)->
    SessId = user_ctx:get_session_id(UserCtx),
    SpaceDirUuid = file_ctx:get_space_dir_uuid_const(FileCtx),
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    {ok, Storage} = storage:get(StorageId),
    ShareId = file_ctx:get_share_id_const(FileCtx),
    SFMHandle =
        storage_file_manager:new_handle(SessId, SpaceDirUuid, FileUuid, Storage, FileId, ShareId),
    storage_file_manager:open(SFMHandle, OpenFlag);
get_handle(UserCtx, _FileCtx, HandleId, _StorageId, _FileId, _OpenFlag)->
    SessId = user_ctx:get_session_id(UserCtx),
    session:get_handle(SessId, HandleId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Writes all of the data to the storage or dies trying.
%% @end
%%--------------------------------------------------------------------
-spec write_all(Handle :: storage_file_manager:handle(),
    Offset :: non_neg_integer(), Data :: binary(),
    Wrote :: non_neg_integer()) -> non_neg_integer().
write_all(_Handle, _Offset, <<>>, Wrote) -> Wrote;
write_all(Handle, Offset, Data, Wrote) ->
    {ok, WroteNow} = storage_file_manager:write(Handle, Offset, Data),
    write_all(Handle, Offset + WroteNow,
              binary_part(Data, {byte_size(Data), WroteNow - byte_size(Data)}),
              Wrote + WroteNow).
