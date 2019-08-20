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
-include("modules/storage_sync/storage_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

-record(storage_file_ctx, {
    name :: helpers:file_id(),
    storage_file_id :: helpers:file_id(),
    canonical_path :: file_meta:name(),
    space_id :: od_space:id(),
    storage_id :: storage:id(),
    handle = undefined :: undefined | storage_file_manager:handle(),
    stat = undefined :: undefined | #statbuf{},
    stat_timestamp :: undefined | non_neg_integer(),
    xattr = undefined :: undefined | binary(),
    sync_mode :: space_strategy:sync_mode(),
    mounted_in_root :: boolean()
}).

-type ctx() :: #storage_file_ctx{}.

-export_type([ctx/0]).

%% API
-export([new/4, get_child_ctx/2, get_children_ctxs_batch/3, reset/1]).
-export([get_stat_buf/1, get_handle/1, get_file_name_const/1,
    get_storage_doc/1, get_nfs4_acl/1, get_space_id_const/1,
    get_stat_timestamp_const/1, get_canonical_path_const/1, get_storage_file_id_const/1, get_parent_ctx_const/1]).


%%-------------------------------------------------------------------
%% @doc
%% Returns initialized #storage_file_ctx{}.
%% @end
%%-------------------------------------------------------------------
-spec new(file_meta:name(), od_space:id(), storage:id(), space_strategy:sync_mode()) -> ctx().
new(CanonicalPath, SpaceId, StorageId, SyncMode) ->
    FileName = filename:basename(CanonicalPath),
    MiRStorages = space_storage:get_mounted_in_root(SpaceId),
    StorageFileId = filename_mapping:to_storage_path(SpaceId, StorageId, CanonicalPath),
    Ctx = #storage_file_ctx{
        name = FileName,
        storage_file_id = StorageFileId,
        canonical_path = CanonicalPath,
        space_id = SpaceId,
        storage_id = StorageId,
        sync_mode = SyncMode,
        mounted_in_root = lists:member(StorageId, MiRStorages)
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
-spec get_file_name_const(ctx()) -> helpers:file_id().
get_file_name_const(#storage_file_ctx{name = FileName}) ->
    FileName.

-spec get_storage_file_id_const(ctx()) -> helpers:file_id().
get_storage_file_id_const(#storage_file_ctx{storage_file_id = StorageFileId}) ->
    StorageFileId.

%%-------------------------------------------------------------------
%% @doc
%% Returns children's storage_file_ctxs,
%% @end
%%-------------------------------------------------------------------
-spec get_children_ctxs_batch(ctx(), non_neg_integer(),
    non_neg_integer()) -> {ChildrenCtxs :: [ctx()], NewParentCtx :: ctx()}.
get_children_ctxs_batch(StorageFileCtx = #storage_file_ctx{sync_mode = ?STORAGE_SYNC_POSIX_MODE}, Offset, BatchSize) ->
    {SFMHandle, StorageFileCtx2} = get_handle(StorageFileCtx),
    case storage_file_manager:readdir(SFMHandle, Offset, BatchSize) of
        {error, ?ENOENT} ->
            {[], StorageFileCtx2};
        {error, ?EACCES} ->
            {[], StorageFileCtx2};
        {ok, ChildrenIds} ->
            lists:foldr(fun(ChildId, {ChildrenCtxs, ParentCtx0}) ->
                BaseName = filename:basename(ChildId),
                {ChildCtx, ParentCtx} = get_child_ctx(ParentCtx0, BaseName),
                {[ChildCtx | ChildrenCtxs], ParentCtx}
            end, {[], StorageFileCtx2}, ChildrenIds)
    end;
get_children_ctxs_batch(StorageFileCtx = #storage_file_ctx{
    sync_mode = ?STORAGE_SYNC_OBJECT_MODE,
    space_id = SpaceId,
    storage_id = StorageId,
    mounted_in_root = MiR
}, Marker, BatchSize) ->
    {SFMHandle, StorageFileCtx2} = get_handle(StorageFileCtx),
    case storage_file_manager:listobjects(SFMHandle, Marker, 0, BatchSize) of
        {error, ?ENOENT} ->
            {[], StorageFileCtx2};
        {error, ?EACCES} ->
            {[], StorageFileCtx2};
        {ok, ChildrenIds} ->
            lists:foldr(fun(ChildId, {ChildrenCtxs, ParentCtx0}) ->
                CanonicalPath = case MiR of
                    true ->
                        filename:join([<<"/">>, SpaceId, ChildId]);
                    false ->
                        filename:join([<<"/">>, ChildId])
                end,
                ChildCtx = new(CanonicalPath, SpaceId, StorageId, ?STORAGE_SYNC_OBJECT_MODE),
                {[ChildCtx | ChildrenCtxs], ParentCtx0}
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
    space_id = SpaceId,
    storage_id = StorageId,
    canonical_path = ParentCanonicalPath,
    handle = ParentHandle,
    sync_mode = SyncMode,
    mounted_in_root = MountedInRoot
}, ChildName) ->
    ChildCanonicalPath = filename:join([ParentCanonicalPath, ChildName]),
    ChildCtx = #storage_file_ctx{
        name = ChildName,
        storage_file_id = filename_mapping:to_storage_path(SpaceId, StorageId, ChildCanonicalPath),
        canonical_path = ChildCanonicalPath,
        space_id = SpaceId,
        storage_id = StorageId,
        handle = storage_file_manager:get_child_handle(ParentHandle, ChildName),
        stat = undefined,
        stat_timestamp = undefined,
        xattr = undefined,
        sync_mode = SyncMode,
        mounted_in_root = MountedInRoot
    },
    {ChildCtx, ParentCtx}.

-spec get_parent_ctx_const(ctx()) -> ParentCtx :: ctx().
get_parent_ctx_const(#storage_file_ctx{
    storage_file_id = ChildStorageFileId,
    canonical_path = ChildCanonicalPath,
    space_id = SpaceId,
    storage_id = StorageId,
    sync_mode = SyncMode,
    mounted_in_root = MountedInRoot
}) ->
    {_, ParentCanonicalPath} = fslogic_path:basename_and_parent(ChildCanonicalPath),
    {_, ParentStorageFileId} = fslogic_path:basename_and_parent(ChildStorageFileId),
    {ParentName, _} = fslogic_path:basename_and_parent(ParentStorageFileId),
    #storage_file_ctx{
        name = ParentName,
        storage_file_id = ParentStorageFileId,
        canonical_path = ParentCanonicalPath,
        space_id = SpaceId,
        storage_id = StorageId,
        stat = undefined,
        stat_timestamp = undefined,
        xattr = undefined,
        sync_mode = SyncMode,
        mounted_in_root = MountedInRoot
    }.

%%-------------------------------------------------------------------
%% @doc
%% Returns #statbuf of file associated with StorageFileCtx.
%% @end
%%-------------------------------------------------------------------
-spec get_stat_buf(ctx()) -> {#statbuf{}, ctx()}.
get_stat_buf(StorageFileCtx = #storage_file_ctx{stat = undefined, handle = undefined}) ->
    get_stat_buf(set_sfm_handle(StorageFileCtx));
get_stat_buf(StorageFileCtx = #storage_file_ctx{stat = undefined, handle = SFMHandle}) ->
    Timestamp = time_utils:system_time_seconds(),
    case storage_file_manager:stat(SFMHandle) of
        {ok, StatBuf} ->
            {StatBuf, StorageFileCtx#storage_file_ctx{
                stat = StatBuf,
                stat_timestamp = Timestamp
            }};
        {error, ?ENOENT} ->
            throw(?ENOENT)
    end;
get_stat_buf(StorageFileCtx = #storage_file_ctx{stat = StatBuf}) ->
    {StatBuf, StorageFileCtx}.

%%-------------------------------------------------------------------
%% @doc
%% Returns timestamp of last stat performed on storage.
%% @end
%%-------------------------------------------------------------------
-spec get_stat_timestamp_const(ctx()) -> non_neg_integer().
get_stat_timestamp_const(#storage_file_ctx{stat_timestamp = StatTimestamp}) ->
    StatTimestamp.

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
    StorageFileCtx2 = set_sfm_handle(StorageFileCtx),
    get_storage_doc(StorageFileCtx2);
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

-spec get_canonical_path_const(ctx()) -> od_space:id().
get_canonical_path_const(#storage_file_ctx{canonical_path = CanonicalPath}) ->
    CanonicalPath.

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
            throw(?ENOENT);
        {error, ?ENODATA} ->
            throw(?ENODATA);
        {error, 'Function not implemented'} ->
            throw(?ENOTSUP)
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
