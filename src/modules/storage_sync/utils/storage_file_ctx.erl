%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% This module contains definition and functions to operate on
%%% storage_file_ctx, a utility data structure used by storage sync.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_file_ctx).
-author("Jakub Kudzia").

-include("modules/storage_file_manager/helpers/helpers.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").


-record(storage_file_ctx, {
    id :: helpers:file_id(),
    handle = undefined :: undefined | storage_file_manager:handle(),
    stat  = undefined :: undefined | #statbuf{}
}).

-type ctx() :: #storage_file_ctx{}.

-export_type([ctx/0]).

%% API
-export([new/3, get_child_ctx/2, get_children_ctxs_batch_const/3]).
-export([get_stat_buf/1, get_handle_const/1, get_file_id_const/1]).


%%%-------------------------------------------------------------------
%%% @doc
%%% Returns initialized #storage_file_ctx{}.
%%% @end
%%%-------------------------------------------------------------------
-spec new(file_meta:name(), od_space:id(), storage:id()) -> ctx().
new(CanonicalPath, SpaceId, StorageId) ->
    FileName = filename:basename(CanonicalPath),
    {ok, Storage} = storage:get(StorageId),
    SFMHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceId,
        undefined, Storage, CanonicalPath, undefined),

    #storage_file_ctx{
        id = FileName,
        handle = SFMHandle
    }.


%%%-------------------------------------------------------------------
%%% @doc
%%% Returns file_if from storage_file_ctx.
%%% @end
%%%-------------------------------------------------------------------
-spec get_file_id_const(ctx()) -> helpers:file_id().
get_file_id_const(#storage_file_ctx{id = FileId}) ->
    FileId.


%%%-------------------------------------------------------------------
%%% @doc
%%% Returns children's storage_file_ctxs,
%%% @end
%%%-------------------------------------------------------------------
-spec get_children_ctxs_batch_const(ctx(), non_neg_integer(),
    non_neg_integer()) -> [ctx()].
get_children_ctxs_batch_const(StorageFileCtx, Offset, BatchSize) ->
    SFMHandle = storage_file_ctx:get_handle_const(StorageFileCtx),
    {ok, ChildrenIds} = storage_file_manager:readdir(SFMHandle, Offset, BatchSize),
    lists:filtermap(fun(ChildId) ->
        case file_meta:is_hidden(ChildId) of
            false ->
                BaseName = filename:basename(ChildId),
                {true, storage_file_ctx:get_child_ctx(StorageFileCtx, BaseName)};
            _ -> false
        end
    end, ChildrenIds).


%%%-------------------------------------------------------------------
%%% @doc
%%% Returns storage_file_ctx of child with given name.
%%% @end
%%%-------------------------------------------------------------------
-spec get_child_ctx(ctx(), file_meta:name()) -> ctx().
get_child_ctx(#storage_file_ctx{handle=ParentHandle}, ChildName) ->
    #storage_file_ctx{
        id = ChildName,
        handle = storage_file_manager:get_child_handle(ParentHandle, ChildName)
    }.


%%%-------------------------------------------------------------------
%%% @doc
%%% Returns #statbuf of file associated with StorageFileCtx.
%%% @end
%%%-------------------------------------------------------------------
-spec get_stat_buf(ctx()) -> {#statbuf{}, ctx()} | {error, term()}.
get_stat_buf(StorageFileCtx = #storage_file_ctx{stat=undefined, handle=SFMHandle}) ->
    case storage_file_manager:stat(SFMHandle) of
        {ok, StatBuf} ->
            {StatBuf, StorageFileCtx#storage_file_ctx{stat=StatBuf}};
        Error ->
            Error
    end;
get_stat_buf(StorageFileCtx = #storage_file_ctx{stat=StatBuf}) ->
    {StatBuf, StorageFileCtx}.


%%%-------------------------------------------------------------------
%%% @doc
%%% Returns cached SFMHandle.
%%% @end
%%%-------------------------------------------------------------------
-spec get_handle_const(ctx()) -> storage_file_manager:handle().
get_handle_const(#storage_file_ctx{handle = SFMHandle}) ->
    SFMHandle.