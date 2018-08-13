%%%-------------------------------------------------------------------
%%% @author Konrad Zemek
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing request for ProxyIO helper.
%%% @end
%%%-------------------------------------------------------------------
-module(read_write_req).
-author("Konrad Zemek").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([read/5, write/4]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Reads data from a location uniquely specified by {StorageID, FileID} pair.
%% @end
%%--------------------------------------------------------------------
-spec read(user_ctx:ctx(), file_ctx:ctx(), HandleId :: storage_file_manager:handle_id(),
    Offset :: non_neg_integer(), Size :: pos_integer()) ->
    fslogic_worker:proxyio_response().
read(UserCtx, FileCtx, HandleId, Offset, Size) ->
    {ok, Handle} =  get_handle(UserCtx, FileCtx, HandleId),
    {ok, Data} = storage_file_manager:read(Handle, Offset, Size),
    #proxyio_response{
        status = #status{code = ?OK},
        proxyio_response = #remote_data{data = Data}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Writes data to a location uniquely specified by {StorageID, FileID} pair.
%% @end
%%--------------------------------------------------------------------
-spec write(user_ctx:ctx(), file_ctx:ctx(),
    HandleId :: storage_file_manager:handle_id(),
    ByteSequences :: [#byte_sequence{}]) -> fslogic_worker:proxyio_response().
write(UserCtx, FileCtx, HandleId, ByteSequences) ->
    {ok, Handle0} = get_handle(UserCtx, FileCtx, HandleId),
    {Written, _} =
        lists:foldl(fun(#byte_sequence{offset = Offset, data = Data}, {Acc, Handle}) ->
            {WrittenNow, NewHandle} = write_all(Handle, Offset, Data, 0),
            {Acc + WrittenNow, NewHandle}
        end, {0, Handle0}, ByteSequences),

    #proxyio_response{
        status = #status{code = ?OK},
        proxyio_response = #remote_write_result{wrote = Written}
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns handle by either retrieving it from session or opening file.
%% @end
%%--------------------------------------------------------------------
-spec get_handle(user_ctx:ctx(), file_ctx:ctx(),
    HandleId :: storage_file_manager:handle_id()) ->
    {ok, storage_file_manager:handle()} | logical_file_manager:error_reply().
get_handle(UserCtx, FileCtx, HandleId) ->
    SessId = user_ctx:get_session_id(UserCtx),
    case session:get_handle(SessId, HandleId) of
        {error, not_found} ->
            ?warning("Hanlde not found, session id: ~p, handle id: ~p",
                [SessId, HandleId]),
            create_handle(UserCtx, FileCtx, HandleId),
            session:get_handle(SessId, HandleId);
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates handle to file.
%% @end
%%--------------------------------------------------------------------
-spec create_handle(user_ctx:ctx(), file_ctx:ctx(),
    HandleId :: storage_file_manager:handle_id()) -> ok.
create_handle(UserCtx, FileCtx, HandleId) ->
    Node = consistent_hasing:get_node(file_ctx:get_uuid_const(FileCtx)),
    #fuse_response{
        status = #status{code = ?OK}
    } = rpc:call(Node, file_req, open_file_insecure,
        [UserCtx, FileCtx, rdwr, HandleId]),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Writes all of the data to the storage or dies trying.
%% @end
%%--------------------------------------------------------------------
-spec write_all(Handle :: storage_file_manager:handle(),
    Offset :: non_neg_integer(), Data :: binary(),
    Wrote :: non_neg_integer()) -> {non_neg_integer(), storage_file_manager:handle()}.
write_all(Handle, _Offset, <<>>, Written) -> {Written, Handle};
write_all(Handle, Offset, Data, Written) ->
    {ok, WrittenNow} = storage_file_manager:write(Handle, Offset, Data),
    Handle2 = storage_file_manager:increase_size(Handle, WrittenNow),
    write_all(Handle2, Offset + WrittenNow,
              binary_part(Data, {byte_size(Data), WrittenNow - byte_size(Data)}),
              Written + WrittenNow).
