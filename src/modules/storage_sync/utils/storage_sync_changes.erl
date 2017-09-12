%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Module with helper functions for storage_sync.
%%% Functions in this module are responsible for detecting changes of
%%% synchronized files.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_changes).
-author("Jakub Kudzia").


-include("modules/fslogic/fslogic_common.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-type hash() :: binary() | undefined.

-export_type([hash/0]).

%% API
-export([children_attrs_hash_has_changed/3, mtime_has_changed/2,
    count_files_attrs_hash/1]).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether mtime has changed since last synchronization.
%% @end
%%-------------------------------------------------------------------
-spec mtime_has_changed(#file_meta{}, storage_file_ctx:ctx()) -> boolean().
mtime_has_changed(#file_meta{storage_sync_info = StSyncInfo}, StorageFileCtx) ->
    {#statbuf{st_mtime = StMtime}, _} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    case StSyncInfo#storage_sync_info.last_synchronized_mtime of
        StMtime -> false;
        _ -> true
    end.



%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether hash of children attrs has changed since last
%% synchronization.
%% @end
%%-------------------------------------------------------------------
-spec children_attrs_hash_has_changed(#file_meta{}, hash(), non_neg_integer()) ->
    boolean().
children_attrs_hash_has_changed(#file_meta{storage_sync_info = StSyncInfo},
    CurrentChildrenAttrsHash, Key
) ->
    PreviousHashes = StSyncInfo#storage_sync_info.children_attrs_hashes,
    PreviousHash = maps:get(Key, PreviousHashes, undefined),
    case {PreviousHash, CurrentChildrenAttrsHash} of
        {Hash, Hash} -> false;
        {undefined, <<"">>} -> false;
        CurrentChildrenAttrsHash -> false;
        _ -> true
    end.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Counts hash of attributes of files associated with passed contexts.
%% @end
%%-------------------------------------------------------------------
-spec count_files_attrs_hash([storage_file_ctx:ctx()]) ->
    {hash(), [storage_file_ctx:ctx()]}.
count_files_attrs_hash([]) ->
    {undefined, []};
count_files_attrs_hash(StorageFileCtxs) ->
    try
        lists:foldr(fun(StorageFileCtx, {Hash0, StorageFileCtxs0}) ->
            {FileHash, StorageFileCtx2} = count_file_attrs_hash(StorageFileCtx),
            {hash([Hash0, FileHash]), [StorageFileCtx2 | StorageFileCtxs0]}
        end, {<<"">>, []}, StorageFileCtxs)
    catch
        error:_ ->
            {undefined, StorageFileCtxs}
    end.

%%===================================================================
%% Internal functions
%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Counts hash of attributes of file associated with passed context.
%% @end
%%-------------------------------------------------------------------
-spec count_file_attrs_hash(storage_file_ctx:ctx()) -> {hash(), storage_file_ctx:ctx()}.
count_file_attrs_hash(StorageFileCtx) ->
    {StatBuf, StorageFileCtx2} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    #statbuf{
        st_mode = StMode,
        st_size = StSize,
        st_atime = StAtime,
        st_mtime = STMtime,
        st_ctime = STCtime
    }= StatBuf,

    {Xattr, StorageFileCtx3} = try
        storage_file_ctx:get_nfs4_acl(StorageFileCtx2)
    catch
        throw:?ENOTSUP ->
            {<<"">>, StorageFileCtx2};
        throw:?ENOENT ->
            {<<"">>, StorageFileCtx2}
    end,

    case file_meta:type(StMode) of
        ?DIRECTORY_TYPE ->
            %% don't count hash for directory as it will be scanned anyway
            {<<"">>, StorageFileCtx3};
        ?REGULAR_FILE_TYPE ->
            {hash([StMode, StSize, StAtime, STMtime, STCtime, Xattr]), StorageFileCtx3}
    end.



%%-------------------------------------------------------------------
%% @private
%% @doc
%% Wrapper for crypto:hash function
%% @end
%%-------------------------------------------------------------------
-spec hash([term()]) -> hash().
hash(Args) ->
    Arg = str_utils:join_binary([str_utils:to_binary(A) || A <- Args], <<"">>),
    crypto:hash(md5, Arg).
