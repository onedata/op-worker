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


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Reads data from a location uniquely specified by {StorageID, FileID} pair.
%% @end
%%--------------------------------------------------------------------
-spec read(user_ctx:ctx(), file_ctx:ctx(), HandleId :: storage_driver:handle_id(),
    Offset :: non_neg_integer(), Size :: pos_integer()) ->
    fslogic_worker:proxyio_response().
read(UserCtx, FileCtx, HandleId, Offset, Size) ->
    {ok, Handle} =  get_handle(UserCtx, FileCtx, HandleId),
    {ok, Data} = storage_driver:read(Handle, Offset, Size),
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
    HandleId :: storage_driver:handle_id(),
    ByteSequences :: [#byte_sequence{}]) -> fslogic_worker:proxyio_response().
write(UserCtx, FileCtx, HandleId, ByteSequences) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, Handle0} = get_handle(UserCtx, FileCtx, HandleId),

    {Written, _} = lists:foldl(fun
        (#byte_sequence{offset = Offset, data = <<>>}, {Acc, Handle}) ->
            handle_empty_write(Handle, Offset),
            {Acc, Handle};
        (#byte_sequence{offset = Offset, data = Data}, {Acc, Handle}) ->
            space_quota:assert_write(SpaceId, max(0, size(Data))),
            {WrittenNow, NewHandle} = write_all(Handle, Offset, Data, 0),
            {Acc + WrittenNow, NewHandle}
    end, {0, Handle0}, ByteSequences),

    #proxyio_response{
        status = #status{code = ?OK},
        proxyio_response = #remote_write_result{wrote = Written}
    }.

%% @private
-spec handle_empty_write(storage_driver:handle(), non_neg_integer()) -> ok.
handle_empty_write(Handle, Offset) ->
    case helper:is_getting_size_supported(storage:get_helper(storage_driver:get_storage_id(Handle))) of
        true ->
            ok = case storage_driver:stat(Handle) of
                {ok, #statbuf{st_size = Size}} when Size < Offset ->
                    storage_driver:truncate(Handle, Offset, Size);
                _ ->
                    ok
            end;
        _ ->
            ok % it is possible to read with offset larger than file size, truncate not needed
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns node that  handles proxy operations for particular file.
%% @end
%%--------------------------------------------------------------------
-spec get_proxyio_node(file_meta:uuid()) -> node().
get_proxyio_node(Uuid) ->
    datastore_key:any_responsible_node(Uuid).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns handle by either retrieving it from session or opening file.
%% @end
%%--------------------------------------------------------------------
-spec get_handle(user_ctx:ctx(), file_ctx:ctx(), storage_driver:handle_id()) ->
    {ok, storage_driver:handle()} | lfm:error_reply().
get_handle(UserCtx, FileCtx, HandleId) ->
    SessId = user_ctx:get_session_id(UserCtx),
    case session_handles:get(SessId, HandleId) of
        {error, not_found} ->
            ?debug("Handle not found, session id: ~tp, handle id: ~tp",
                [SessId, HandleId]),
            create_handle(UserCtx, FileCtx, HandleId),
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
    HandleId :: storage_driver:handle_id()) -> ok.
create_handle(UserCtx, FileCtx, HandleId) ->
    Flag = file_handles:get_open_flag(HandleId),
    try
        create_handle_helper(UserCtx, FileCtx, HandleId, Flag, open_file)
    catch _:Reason when
        Reason =:= ?EACCES;
        Reason =:= ?EPERM;
        Reason =:= ?EROFS
    ->
        case file_handles:get_creation_handle(file_ctx:get_logical_uuid_const(FileCtx)) of
            {ok, HandleId} ->
                % opening file with handle received from creation procedure
                % (should open even if the user does not have permissions)
                create_handle_helper(UserCtx, FileCtx, HandleId, Flag, open_file_insecure);
            _ ->
                error(Reason)
        end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates handle to file with particular open flag.
%% @end
%%--------------------------------------------------------------------
-spec create_handle_helper(user_ctx:ctx(), file_ctx:ctx(),
    HandleId :: storage_driver:handle_id(),
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
-spec write_all(Handle :: storage_driver:handle(),
    Offset :: non_neg_integer(), Data :: binary(),
    Wrote :: non_neg_integer()) -> {non_neg_integer(), storage_driver:handle()}.
write_all(Handle, _Offset, <<>>, Written) -> {Written, Handle};
write_all(Handle, Offset, Data, Written) ->
    {ok, WrittenNow} = storage_driver:write(Handle, Offset, Data),
    Handle2 = storage_driver:increase_size(Handle, WrittenNow),
    write_all(Handle2, Offset + WrittenNow,
              binary_part(Data, {byte_size(Data), WrittenNow - byte_size(Data)}),
              Written + WrittenNow).
