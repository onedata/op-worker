%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Opaque data structure storing information about file on storage, working as a cache.
%%% Its lifetime is limited by the time of request.
%%% If effort of computing something is significant,
%%% the value is cached and the further calls will use it. Therefore some of the
%%% functions (those without '_const' suffix) return updated version of context
%%% together with the result.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_file_ctx).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").

-record(storage_file_ctx, {
    name :: helpers:file_id(),
    storage_file_id :: helpers:file_id(),
    space_id :: od_space:id(),
    storage_id :: storage:id(),
    storage :: undefined | storage:data(),
    stat :: undefined | helpers:stat(),
    % field used to store timestamp of cached stat structure
    stat_timestamp :: undefined | non_neg_integer(),
    xattr :: undefined | binary()
}).

-type ctx() :: #storage_file_ctx{}.

-export_type([ctx/0]).

%% API
-export([new/3, new/4, new_with_stat/4, new_with_stat/5, set_stat/2]).
-export([
    get_file_name_const/1, get_storage_file_id_const/1,
    get_storage_id_const/1, get_space_id_const/1,
    get_child_ctx_const/2, get_parent_ctx_const/1,
    get_stat_timestamp_const/1, get_handle_const/1
]).
-export([stat/1, get_nfs4_acl/1, get_storage/1]).

-define(NOW(), global_clock:timestamp_seconds()).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new(file_meta:name(), od_space:id(), storage:id()) -> ctx().
new(StorageFileId, SpaceId, StorageId) ->
    FileName = filename:basename(StorageFileId),
    new(StorageFileId, FileName, SpaceId, StorageId).

new(StorageFileId, FileName, SpaceId, StorageId) ->
    #storage_file_ctx{
        storage_file_id = StorageFileId,
        name = FileName,
        space_id = SpaceId,
        storage_id = StorageId
    }.

-spec new_with_stat(file_meta:name(), od_space:id(), storage:id(), helpers:stat()) -> ctx().
new_with_stat(StorageFileId, SpaceId, StorageId, Stat) ->
    FileName = filename:basename(StorageFileId),
    new_with_stat(StorageFileId, FileName, SpaceId, StorageId, Stat).

new_with_stat(StorageFileId, FileName, SpaceId, StorageId, Stat) ->
    #storage_file_ctx{
        storage_file_id = StorageFileId,
        name = FileName,
        space_id = SpaceId,
        storage_id = StorageId,
        stat = Stat,
        stat_timestamp = ?NOW()
    }.

-spec get_file_name_const(ctx()) -> helpers:file_id().
get_file_name_const(#storage_file_ctx{name = FileName}) ->
    FileName.

-spec get_storage_file_id_const(ctx()) -> helpers:file_id().
get_storage_file_id_const(#storage_file_ctx{storage_file_id = StorageFileId}) ->
    StorageFileId.

-spec get_storage_id_const(ctx()) -> storage:id().
get_storage_id_const(#storage_file_ctx{storage_id = StorageId}) ->
    StorageId.

-spec get_space_id_const(ctx()) -> od_space:id().
get_space_id_const(#storage_file_ctx{space_id = SpaceId}) ->
    SpaceId.

%%-------------------------------------------------------------------
%% @doc
%% Returns #storage_file_ctx of child with given name.
%% @end
%%-------------------------------------------------------------------
-spec get_child_ctx_const(ctx(), file_meta:name()) -> ChildCtx :: ctx().
get_child_ctx_const(#storage_file_ctx{
    storage_file_id = ParentStorageFileId,
    space_id = SpaceId,
    storage_id = StorageId
}, ChildName) ->
    #storage_file_ctx{
        name = ChildName,
        storage_file_id = filename:join([ParentStorageFileId, ChildName]),
        space_id = SpaceId,
        storage_id = StorageId,
        stat = undefined,
        stat_timestamp = undefined,
        xattr = undefined
    }.

%%-------------------------------------------------------------------
%% @doc
%% Returns #storage_file_ctx of file's parent.
%% @end
%%-------------------------------------------------------------------
-spec get_parent_ctx_const(ctx()) -> ParentCtx :: ctx().
get_parent_ctx_const(#storage_file_ctx{
    storage_file_id = ChildStorageFileId,
    space_id = SpaceId,
    storage_id = StorageId
}) ->
    {_, ParentStorageFileId} = filepath_utils:basename_and_parent_dir(ChildStorageFileId),
    {ParentName, _} = filepath_utils:basename_and_parent_dir(ParentStorageFileId),
    #storage_file_ctx{
        name = ParentName,
        storage_file_id = ParentStorageFileId,
        space_id = SpaceId,
        storage_id = StorageId,
        stat = undefined,
        stat_timestamp = undefined,
        xattr = undefined
    }.

%%-------------------------------------------------------------------
%% @doc
%% Returns timestamp of last stat performed on storage.
%% @end
%%-------------------------------------------------------------------
-spec get_stat_timestamp_const(ctx()) -> non_neg_integer().
get_stat_timestamp_const(#storage_file_ctx{stat_timestamp = StatTimestamp}) ->
    StatTimestamp.

-spec get_handle_const(ctx()) -> storage_driver:handle().
get_handle_const(#storage_file_ctx{
    storage_file_id = StorageFileId,
    space_id = SpaceId,
    storage_id = StorageId
}) ->
    storage_driver:new_handle(?ROOT_SESS_ID, SpaceId, undefined, StorageId, StorageFileId).

-spec set_stat(ctx(), helpers:stat()) -> ctx().
set_stat(StorageFileCtx, Statbuf) ->
    StorageFileCtx#storage_file_ctx{
        stat = Statbuf,
        stat_timestamp = ?NOW()
    }.

-spec stat(ctx()) -> {helpers:stat(), ctx()}.
stat(StorageFileCtx = #storage_file_ctx{stat = undefined}) ->
    SDHandle = get_handle_const(StorageFileCtx),
    case storage_driver:stat(SDHandle) of
        {ok, StatBuf} ->
            {StatBuf, StorageFileCtx#storage_file_ctx{
                stat = StatBuf,
                stat_timestamp = ?NOW()
            }};
        {error, ?ENOENT} ->
            throw(?ENOENT);
        {error, ?ENOTSUP} ->
            throw(?ENOTSUP)
    end;
stat(StorageFileCtx = #storage_file_ctx{stat = StatBuf}) ->
    {StatBuf, StorageFileCtx}.

%%-------------------------------------------------------------------
%% @doc
%% Returns binary representation of nfs4 acl.
%% @end
%%-------------------------------------------------------------------
-spec get_nfs4_acl(ctx()) -> {binary(), ctx()}.
get_nfs4_acl(StorageFileCtx) ->
    get_xattr(StorageFileCtx, <<"system.nfs4_acl">>).

-spec get_storage(ctx()) -> {storage:data(), ctx()}.
get_storage(StorageFileCtx = #storage_file_ctx{storage = undefined}) ->
    StorageId = get_storage_id_const(StorageFileCtx),
    {ok, Storage} = storage:get(StorageId),
    {Storage, StorageFileCtx#storage_file_ctx{storage = Storage}};
get_storage(StorageFileCtx = #storage_file_ctx{storage = Storage}) ->
    {Storage, StorageFileCtx}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns binary representation of given xattr.
%% @end
%%-------------------------------------------------------------------
-spec get_xattr(ctx(), binary()) -> {binary(), ctx()}.
get_xattr(StorageFileCtx = #storage_file_ctx{xattr = undefined}, XattrName) ->
    SDHandle = get_handle_const(StorageFileCtx),
    case storage_driver:getxattr(SDHandle, XattrName) of
        {ok, Xattr} ->
            {Xattr, StorageFileCtx#storage_file_ctx{xattr = Xattr}};
        {error, ?ENOTSUP} ->
            throw(?ENOTSUP);
        {error, ?ENOSYS} ->
            throw(?ENOTSUP);
        {error, ?ENOENT} ->
            throw(?ENOENT);
        {error, ?ENODATA} ->
            throw(?ENODATA);
        {error, 'Function not implemented'} ->
            throw(?ENOTSUP)
    end;
get_xattr(StorageFileCtx = #storage_file_ctx{xattr = Xattr}, _XattrName) ->
    {Xattr, StorageFileCtx}.
