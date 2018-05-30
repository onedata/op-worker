%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains definition and functions to operate on
%%% storage_file_ctx, a utility data structure used by storage sync.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_file_ctx).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-record(storage_file_ctx, {
    id :: helpers:file_id(),
    canonical_path :: helpers:file_id(),
    space_id :: od_space:id(),
    storage_id :: storage:id(),
    handle = undefined :: undefined | storage_file_manager:handle(),
    stat = undefined :: undefined | #statbuf{},
    xattr = undefined :: undefined | binary()
}).

-type ctx() :: #storage_file_ctx{}.

-export_type([ctx/0]).

%% API
-export([new/3, get_child_ctx/2, get_children_ctxs_batch/3, reset/1]).
-export([get_stat_buf/1, get_handle/1, get_file_id_const/1,
    get_storage_doc/1, get_nfs4_acl/1, get_space_id_const/1
]).


%%-------------------------------------------------------------------
%% @doc
%% Returns initialized #storage_file_ctx{}.
%% @end
%%-------------------------------------------------------------------
-spec new(file_meta:name(), od_space:id(), storage:id()) -> ctx().
new(CanonicalPath, SpaceId, StorageId) ->
    FileName = filename:basename(CanonicalPath),
    Ctx = #storage_file_ctx{
        id = FileName,
        canonical_path = CanonicalPath,
        space_id = SpaceId,
        storage_id = StorageId
    },
    set_sfm_handle(Ctx).

%%-------------------------------------------------------------------
%% @doc
%% Resets handle field.
%% @end
%%-------------------------------------------------------------------
-spec reset(ctx()) -> ctx().
reset(Ctx = #storage_file_ctx{}) ->
    Ctx#storage_file_ctx{
        handle = undefined,
        xattr = undefined
    }.

%%-------------------------------------------------------------------
%% @doc
%% Returns file id from storage_file_ctx.
%% @end
%%-------------------------------------------------------------------
-spec get_file_id_const(ctx()) -> helpers:file_id().
get_file_id_const(#storage_file_ctx{id = FileId}) ->
    FileId.

%%-------------------------------------------------------------------
%% @doc
%% Returns children's storage_file_ctxs,
%% @end
%%-------------------------------------------------------------------
-spec get_children_ctxs_batch(ctx(), non_neg_integer(),
    non_neg_integer()) -> {ChildrenCtxs :: [ctx()], NewParentCtx :: ctx()}.
get_children_ctxs_batch(StorageFileCtx, Offset, BatchSize) ->
    {SFMHandle, StorageFileCtx2} = storage_file_ctx:get_handle(StorageFileCtx),
    case storage_file_manager:readdir(SFMHandle, Offset, BatchSize) of
        {error, ?ENOENT} ->
            {[], StorageFileCtx2};
        {error, ?EACCES} ->
            {[], StorageFileCtx2};
        {ok, ChildrenIds} ->
            lists:foldr(fun(ChildId, {ChildrenCtxs, ParentCtx0}) ->
                BaseName = filename:basename(ChildId),
                {ChildCtx, ParentCtx} = storage_file_ctx:get_child_ctx(ParentCtx0, BaseName),
                {[ChildCtx | ChildrenCtxs], ParentCtx}
            end, {[], StorageFileCtx2}, ChildrenIds)
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns storage_file_ctx of child with given name.
%% @end
%%-------------------------------------------------------------------
-spec get_child_ctx(ctx(), file_meta:name()) -> {ChildCtx :: ctx(), ParentCtx :: ctx()}.
get_child_ctx(ParentCtx = #storage_file_ctx{handle = undefined}, ChildName) ->
    get_child_ctx(set_sfm_handle(ParentCtx), ChildName);
get_child_ctx(ParentCtx = #storage_file_ctx{
    canonical_path = ParentCanonicalPath,
    handle = ParentHandle
}, ChildName) ->
    ChildCtx = ParentCtx#storage_file_ctx{
        id = ChildName,
        canonical_path = filename:join([ParentCanonicalPath, ChildName]),
        stat = undefined,
        handle = storage_file_manager:get_child_handle(ParentHandle, ChildName)
    },
    {ChildCtx, ParentCtx}.

%%-------------------------------------------------------------------
%% @doc
%% Returns #statbuf of file associated with StorageFileCtx.
%% @end
%%-------------------------------------------------------------------
-spec get_stat_buf(ctx()) -> {#statbuf{}, ctx()}.
get_stat_buf(StorageFileCtx = #storage_file_ctx{stat = undefined, handle = undefined}) ->
    get_stat_buf(set_sfm_handle(StorageFileCtx));
get_stat_buf(StorageFileCtx = #storage_file_ctx{stat = undefined, handle = SFMHandle}) ->
    case storage_file_manager:stat(SFMHandle) of
        {ok, StatBuf} ->
            {StatBuf, StorageFileCtx#storage_file_ctx{stat = StatBuf}};
        {error, ?ENOENT} ->
            throw(?ENOENT)
    end;
get_stat_buf(StorageFileCtx = #storage_file_ctx{stat = StatBuf}) ->
    {StatBuf, StorageFileCtx}.

%%-------------------------------------------------------------------
%% @doc
%% Returns cached SFMHandle.
%% @end
%%-------------------------------------------------------------------
-spec get_handle(ctx()) -> {storage_file_manager:handle(), ctx()}.
get_handle(StorageFileCtx = #storage_file_ctx{handle = undefined}) ->
    StorageFileCtx2 = #storage_file_ctx{handle = SFMHandle} =
        set_sfm_handle(StorageFileCtx),
    {SFMHandle, StorageFileCtx2};
get_handle(StorageFileCtx = #storage_file_ctx{handle = SFMHandle}) ->
    {SFMHandle, StorageFileCtx}.

%%-------------------------------------------------------------------
%% @doc
%% Returns #storage{} record for storage associated with given context.
%% @end
%%-------------------------------------------------------------------
-spec get_storage_doc(ctx()) -> {storage:doc(), ctx()}.
get_storage_doc(StorageFileCtx = #storage_file_ctx{handle = undefined}) ->
    get_storage_doc(set_sfm_handle(StorageFileCtx));
get_storage_doc(StorageFileCtx = #storage_file_ctx{
    handle = #sfm_handle{
        storage = StorageDoc = #document{}
}}) ->
    {StorageDoc, StorageFileCtx}.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Getter for #storage_file_ctx field.
%% @end
%%-------------------------------------------------------------------
-spec get_space_id_const(ctx()) -> od_space:id().
get_space_id_const(#storage_file_ctx{space_id = SpaceId}) ->
    SpaceId.

%%-------------------------------------------------------------------
%% @doc
%% Returns binary representation of nfs4 acl.
%% @end
%%-------------------------------------------------------------------
-spec get_nfs4_acl(ctx()) -> {binary(), ctx()}.
get_nfs4_acl(StorageFileCtx) ->
    get_xattr(StorageFileCtx, <<"system.nfs4_acl">>).

%%-------------------------------------------------------------------
%% @doc
%% Returns binary representation of given xattr.
%% @end
%%-------------------------------------------------------------------
-spec get_xattr(ctx(), binary()) -> {binary(), ctx()}.
get_xattr(StorageFileCtx = #storage_file_ctx{
    xattr = undefined,
    handle = undefined
}, XattrName) ->
    get_xattr(set_sfm_handle(StorageFileCtx), XattrName);
get_xattr(StorageFileCtx = #storage_file_ctx{
    xattr = Xattr,
    handle = undefined
}, _XattrName) ->
    {Xattr, StorageFileCtx};
get_xattr(StorageFileCtx = #storage_file_ctx{handle = SFMHandle}, XattrName) ->
    case storage_file_manager:getxattr(SFMHandle, XattrName) of
        {ok, Xattr} ->
            {Xattr, StorageFileCtx#storage_file_ctx{xattr = Xattr}};
        {error, ?ENOTSUP} ->
            throw(?ENOTSUP);
        {error, ?ENOENT} ->
            throw(?ENOENT)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Acquires SFMHandle and sets handle field of #storage_file_ctx.
%% @end
%%-------------------------------------------------------------------
-spec set_sfm_handle(ctx()) -> ctx().
set_sfm_handle(Ctx = #storage_file_ctx{
    canonical_path = CanonicalPath,
    space_id = SpaceId,
    storage_id = StorageId
}) ->
    {ok, Storage} = storage:get(StorageId),
    SFMHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceId,
        undefined, Storage, CanonicalPath, undefined),
    Ctx#storage_file_ctx{handle = SFMHandle}.
