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
-module(storage_sync_changes).  %todo maybe rename this module
-author("Jakub Kudzia").


-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

-type hash() :: binary() | undefined.

-export_type([hash/0]).

%% API
-export([children_attrs_hash_has_changed/3, mtime_has_changed/2,
    compute_files_attrs_hash/2, hash/1, compute_file_attrs_hash/2]).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether mtime has changed since last synchronization.
%% @end
%%-------------------------------------------------------------------
-spec mtime_has_changed(storage_sync_info:doc() | undefined, storage_file_ctx:ctx()) ->
    {boolean(), storage_file_ctx:ctx()}.
mtime_has_changed(undefined, StorageFileCtx) ->
%%    ?alert("CHANGED 1"),
    {true, StorageFileCtx};
mtime_has_changed(#document{
    value = #storage_sync_info{mtime = LastMtime}
}, StorageFileCtx) ->
    {#statbuf{st_mtime = StMtime}, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
%%    ?alert("CHANGED 2 ~p ~p", [StMtime, LastMtime]),
    {not (LastMtime =:= StMtime), StorageFileCtx2}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether hash of children attrs has changed since last
%% synchronization.
%% @end
%%-------------------------------------------------------------------
-spec children_attrs_hash_has_changed(storage_sync_info:doc() | undefined, hash(), binary()) ->
    boolean().
children_attrs_hash_has_changed(undefined, _CurrentChildrenAttrsHash, _Key) ->
    true;
children_attrs_hash_has_changed(#document{
    value = #storage_sync_info{children_attrs_hashes = PreviousHashes}
}, CurrentChildrenAttrsHash, Key) ->
    PreviousHash = maps:get(Key, PreviousHashes, undefined),
%%    ?alert("PREVIOUS HASH: ~p", [PreviousHash]),
%%    ?alert("CurrentChildrenAttrsHash: ~p", [CurrentChildrenAttrsHash]),

    case {PreviousHash, CurrentChildrenAttrsHash} of
        {Hash, Hash} -> false;
        {undefined, <<"">>} -> false;
        _ -> true
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Counts hash of attributes of files associated with passed contexts.
%% @end
%%-------------------------------------------------------------------
-spec compute_files_attrs_hash([storage_file_ctx:ctx()], boolean()) ->
    {hash(), [storage_file_ctx:ctx()]}.
compute_files_attrs_hash([], _SyncAcl) ->
    {undefined, []};
compute_files_attrs_hash(StorageFileCtxs, SyncAcl) ->
    try
        {H, Ctxs} = lists:foldr(fun(StorageFileCtx, {Hash0, StorageFileCtxs0}) ->
            {FileHash, StorageFileCtx2} = compute_file_attrs_hash(StorageFileCtx, SyncAcl),
            {[FileHash | Hash0], [StorageFileCtx2 | StorageFileCtxs0]}
        end, {[], []}, StorageFileCtxs),
        {hash(H), Ctxs}
    catch
        Error:Reason ->
            ?error_stacktrace("count_files_attrs_hash failed due to: ~p:~p", [Error, Reason]),
            {undefined, StorageFileCtxs}
    end.

%%-------------------------------------------------------------------
%% @private
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
%%            ?emergency("FileId = ~p", [FileId]),
%%            ?emergency("StMode = ~p", [StMode]),
%%            ?emergency("StSize = ~p", [StSize]),
%%            ?emergency("StAtime = ~p", [StAtime]),
%%            ?emergency("STMtime = ~p", [STMtime]),
%%            ?emergency("STCtime = ~p", [STCtime]),
%%            ?emergency("Xattr = ~p", [Xattr]),
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
