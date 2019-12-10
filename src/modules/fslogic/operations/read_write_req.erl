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
-export([read/5, write/4, get_proxyio_node/1]).

-type operation() :: read | write.

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
    {ok, Handle} =  get_handle(UserCtx, FileCtx, HandleId, read),
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
    {ok, Handle0} = get_handle(UserCtx, FileCtx, HandleId, write),
    {Written, _} =
        lists:foldl(fun(#byte_sequence{offset = Offset, data = Data}, {Acc, Handle}) ->
            {WrittenNow, NewHandle} = write_all(Handle, Offset, Data, 0),
            {Acc + WrittenNow, NewHandle}
        end, {0, Handle0}, ByteSequences),

    #proxyio_response{
        status = #status{code = ?OK},
        proxyio_response = #remote_write_result{wrote = Written}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns node that  handles proxy operations for particular file.
%% @end
%%--------------------------------------------------------------------
-spec get_proxyio_node(file_meta:uuid()) -> node().
get_proxyio_node(Uuid) ->
    consistent_hashing:get_node(Uuid).

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
    HandleId :: storage_file_manager:handle_id(), operation()) ->
    {ok, storage_file_manager:handle()} | lfm:error_reply().
get_handle(UserCtx, FileCtx, HandleId, Operation) ->
    SessId = user_ctx:get_session_id(UserCtx),
    case session_handles:get(SessId, HandleId) of
        {error, not_found} ->
            ?warning("Handle not found, session id: ~p, handle id: ~p",
                [SessId, HandleId]),
            create_handle(UserCtx, FileCtx, HandleId, Operation),
            session_handles:get(SessId, HandleId);
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
    HandleId :: storage_file_manager:handle_id(), operation()) -> ok.
create_handle(UserCtx, FileCtx, HandleId, Operation) ->
    try
        create_handle_helper(UserCtx, FileCtx, HandleId, rdwr, open_file)
    catch
        _:?EACCES ->
            case file_handles:get_creation_handle(file_ctx:get_uuid_const(FileCtx)) of
                % opening file with handle received from creation procedure
                % (should open even if the user does not have permissions)
                {ok, HandleId} -> create_handle_helper(UserCtx, FileCtx, HandleId, rdwr, open_file_insecure);
                % try opening for limited usage (read or write only)
                _ -> create_handle_helper(UserCtx, FileCtx, HandleId, Operation, open_file)
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates handle to file with particular open flag.
%% @end
%%--------------------------------------------------------------------
-spec create_handle_helper(user_ctx:ctx(), file_ctx:ctx(),
    HandleId :: storage_file_manager:handle_id(),
    fslogic_worker:open_flag(), open_file | open_file_insecure) -> ok.
create_handle_helper(UserCtx, FileCtx, HandleId, Flag, OpenFun) ->
    #fuse_response{
        status = #status{code = ?OK}
    } = file_req:OpenFun(UserCtx, FileCtx, Flag, HandleId),
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
