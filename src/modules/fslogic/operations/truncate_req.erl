%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests for truncating file.
%%% @end
%%%--------------------------------------------------------------------
-module(truncate_req).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([truncate/3, truncate_insecure/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv truncate_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec truncate(user_ctx:ctx(), file_ctx:ctx(), Size :: non_neg_integer()) ->
    fslogic_worker:fuse_response().
truncate(UserCtx, FileCtx, Size) ->
    check_permissions:execute(
        [traverse_ancestors, ?write_object],
        [UserCtx, FileCtx, Size, true],
        fun truncate_insecure/4).

%%--------------------------------------------------------------------
%% @doc
%% Truncates file on storage and returns only if operation is complete.
%% Does not change file size in #file_meta model. Model's size should be
%% changed by write events.
%% @end
%%--------------------------------------------------------------------
-spec truncate_insecure(user_ctx:ctx(), file_ctx:ctx(),
    Size :: non_neg_integer(), UpdateTimes :: boolean()) ->
    fslogic_worker:fuse_response().
truncate_insecure(UserCtx, FileCtx, Size, UpdateTimes) ->
    FileCtx2 = update_quota(FileCtx, Size),

    FileCtx5 = case file_ctx:get_extended_direct_io_const(FileCtx2) of
        true ->
            FileCtx2;
        _ ->
            SessId = user_ctx:get_session_id(UserCtx),
            {SFMHandle, FileCtx3} = storage_file_manager:new_handle(SessId, FileCtx2),
            case storage_file_manager:open(SFMHandle, write) of
                {ok, Handle} ->
                    {CurrentSize, _} = file_ctx:get_file_size(FileCtx3),
                    case storage_file_manager:truncate(Handle, Size, CurrentSize) of
                        ok ->
                            ok;
                        Error = {error, ?EBUSY} ->
                            {Path, FileCtx4} = file_ctx:get_canonical_path(FileCtx3),
                            {StorageFileId, _} = file_ctx:get_storage_file_id(FileCtx4),
                            ?warning_stacktrace("truncate of file ~p with file_id ~p returned ~p",
                                [Path, StorageFileId, Error])
                    end,
                    ok = storage_file_manager:release(Handle),
                    ok = file_popularity:update_size(FileCtx3, Size);
                {error, ?ENOENT} ->
                    ok
            end,
            FileCtx3
    end,

    case UpdateTimes of
        true ->
            fslogic_times:update_mtime_ctime(FileCtx5);
        false ->
            ok
    end,
    #fuse_response{status = #status{code = ?OK}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates space quota.
%% @end
%%--------------------------------------------------------------------
-spec update_quota(file_ctx:ctx(), file_meta:size()) -> NewFileCtx :: file_ctx:ctx().
update_quota(FileCtx, Size) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {OldSize, FileCtx2} = file_ctx:get_local_storage_file_size(FileCtx),
    ok = space_quota:assert_write(SpaceId, Size - OldSize),
    FileCtx2.
