%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Module with helper functions for storage_sync.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_hash).
-author("Jakub Kudzia").


-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

-type hash() :: binary() | undefined.

-export_type([hash/0]).

%% API
-export([children_attrs_hash_has_changed/4, hash/1, compute_file_attrs_hash/2]).

%%-------------------------------------------------------------------
%% @doc
%% Checks whether hash of children attrs has changed since last
%% synchronization.
%% @end
%%-------------------------------------------------------------------
-spec children_attrs_hash_has_changed(hash(), non_neg_integer(), non_neg_integer(), storage_sync_info:doc() | undefined) ->
    boolean().
children_attrs_hash_has_changed(_CurrentChildrenAttrsHash, _Offset, _BatchSize, undefined) ->
    true;
children_attrs_hash_has_changed(CurrentChildrenAttrsHash, Offset, BatchSize, SSIDoc) ->
    PreviousHash = storage_sync_info:get_batch_hash(Offset, BatchSize, SSIDoc),
    case {PreviousHash, CurrentChildrenAttrsHash} of
        {Hash, Hash} -> false;
        {undefined, <<"">>} -> false;
        _ -> true
    end.

%%-------------------------------------------------------------------
%% @doc
%% Counts hash of attributes of file associated with passed context.
%% If file has been remove, it will return empty hash.
%% @end
%%-------------------------------------------------------------------
-spec compute_file_attrs_hash(storage_file_ctx:ctx(), boolean()) ->
    {hash(), storage_file_ctx:ctx()}.
compute_file_attrs_hash(StorageFileCtx, SyncAcl) ->
    try
       count_file_attrs_hash(StorageFileCtx, SyncAcl)
    catch
        throw:?ENOENT ->
            {<<"">>, StorageFileCtx}
    end.

%%-------------------------------------------------------------------
%% @doc
%% Wrapper for crypto:hash function
%% @end
%%-------------------------------------------------------------------
-spec hash([term()]) -> hash().
hash(Args) ->
    Arg = str_utils:join_binary([str_utils:to_binary(A) || A <- Args], <<"">>),
    crypto:hash(md5, Arg).

%%===================================================================
%% Internal functions
%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Counts hash of attributes of file associated with passed context.
%% @end
%%-------------------------------------------------------------------
-spec count_file_attrs_hash(storage_file_ctx:ctx(), boolean()) -> {hash(), storage_file_ctx:ctx()}.
count_file_attrs_hash(StorageFileCtx, SyncAcl) ->
    {StatBuf, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    #statbuf{
        st_mode = StMode,
        st_size = StSize,
        st_atime = StAtime,
        st_mtime = STMtime,
        st_ctime = STCtime
    }= StatBuf,

    {Xattr, StorageFileCtx3} = maybe_get_nfs4_acl(StorageFileCtx2, SyncAcl),

    case file_meta:type(StMode) of
        ?DIRECTORY_TYPE ->
            %% don't count hash for directory as it will be scanned anyway
            {<<"">>, StorageFileCtx3};
        ?REGULAR_FILE_TYPE ->
            FileId = storage_file_ctx:get_file_name_const(StorageFileCtx2),
            {hash([FileId, StMode, StSize, StAtime, STMtime, STCtime, Xattr]), StorageFileCtx3}
    end.

-spec maybe_get_nfs4_acl(storage_file_ctx:ctx(), boolean()) ->
    {binary(), storage_file_ctx:ctx()}.
maybe_get_nfs4_acl(StorageFileCtx, false) ->
    {<<"">>, StorageFileCtx};
maybe_get_nfs4_acl(StorageFileCtx, true) ->
    try
        storage_file_ctx:get_nfs4_acl(StorageFileCtx)
    catch
        throw:?ENOTSUP ->
            {<<"">>, StorageFileCtx};
        throw:?ENOENT ->
            {<<"">>, StorageFileCtx};
        throw:?ENODATA ->
            {<<"">>, StorageFileCtx}
    end.
