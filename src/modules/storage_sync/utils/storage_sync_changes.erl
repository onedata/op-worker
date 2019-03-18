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
-include("modules/storage_file_manager/helpers/helpers.hrl").
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
-spec mtime_has_changed(storage_sync_info:doc() | undefined, storage_file_ctx:ctx()) -> boolean().
mtime_has_changed(undefined, _StorageFileCtx) ->
    true;
mtime_has_changed(#document{
    value = #storage_sync_info{mtime = LastMtime}
}, StorageFileCtx) ->
    {#statbuf{st_mtime = StMtime}, _} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    case LastMtime of
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
-spec children_attrs_hash_has_changed(storage_sync_info:doc() | undefined, hash(), non_neg_integer()) ->
    boolean().
children_attrs_hash_has_changed(undefined, _CurrentChildrenAttrsHash, _Key) ->
    true;
children_attrs_hash_has_changed(#document{
    value = #storage_sync_info{children_attrs_hashes = PreviousHashes}
}, CurrentChildrenAttrsHash, Key) ->
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
        {H, Ctxs} = lists:foldr(fun(StorageFileCtx, {Hash0, StorageFileCtxs0}) ->
            {FileHash, StorageFileCtx2} = count_file_attrs_hash_safe(StorageFileCtx),
            {[FileHash | Hash0], [StorageFileCtx2 | StorageFileCtxs0]}
        end, {[], []}, StorageFileCtxs),
        {hash(H), Ctxs}
    catch
        Error:Reason ->
            ?error_stacktrace("count_files_attrs_hash failed due to: ~p:~p", [Error, Reason]),
            {undefined, StorageFileCtxs}
    end.

%%===================================================================
%% Internal functions
%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Counts hash of attributes of file associated with passed context.
%% If file has been remove, it will return empty hash.
%% @end
%%-------------------------------------------------------------------
-spec count_file_attrs_hash_safe(storage_file_ctx:ctx()) -> {hash(), storage_file_ctx:ctx()}.
count_file_attrs_hash_safe(StorageFileCtx) ->
    try
       count_file_attrs_hash(StorageFileCtx)
    catch
        throw:?ENOENT ->
            {<<"">>, StorageFileCtx}
    end.

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
            {<<"">>, StorageFileCtx2};
        throw:?ENODATA ->
            {<<"">>, StorageFileCtx2}
    end,

    case file_meta:type(StMode) of
        ?DIRECTORY_TYPE ->
            %% don't count hash for directory as it will be scanned anyway
            {<<"">>, StorageFileCtx3};
        ?REGULAR_FILE_TYPE ->
            FileId = storage_file_ctx:get_file_id_const(StorageFileCtx2),
            {hash([FileId, StMode, StSize, StAtime, STMtime, STCtime, Xattr]), StorageFileCtx3}
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
