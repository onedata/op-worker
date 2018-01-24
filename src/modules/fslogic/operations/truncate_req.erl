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

%% API
-export([truncate/3, truncate/4, truncate_insecure/4, truncate_insecure/5]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv truncate(UserCtx, FileCtx, Size, OnStorage)
%% @end
%%--------------------------------------------------------------------
-spec truncate(user_ctx:ctx(), file_ctx:ctx(), Size :: non_neg_integer()) ->
    fslogic_worker:fuse_response().
truncate(UserCtx, FileCtx, Size) ->
    truncate(UserCtx, FileCtx, Size, true).

%%--------------------------------------------------------------------
%% @equiv truncate_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec truncate(user_ctx:ctx(), file_ctx:ctx(), Size :: non_neg_integer(),
    OnStorage :: boolean()) -> fslogic_worker:fuse_response().
truncate(UserCtx, FileCtx, Size, OnStorage) ->
    check_permissions:execute(
        [traverse_ancestors, ?write_object],
        [UserCtx, FileCtx, Size, true, OnStorage],
        fun truncate_insecure/5).

%%--------------------------------------------------------------------
%% @equiv truncate_insecure(UserCtx, FileCtx, Size, UpdateTimes, true)
%% @end
%%--------------------------------------------------------------------
-spec truncate_insecure(user_ctx:ctx(), file_ctx:ctx(),
    Size :: non_neg_integer(), UpdateTimes :: boolean()) ->
    fslogic_worker:fuse_response().
truncate_insecure(UserCtx, FileCtx, Size, UpdateTimes) ->
    truncate_insecure(UserCtx, FileCtx, Size, UpdateTimes, true).

%%--------------------------------------------------------------------
%% @doc
%% Truncates file on storage and returns only if operation is complete.
%% Does not change file size in #file_meta model. Model's size should be
%% changed by write events.
%% @end
%%--------------------------------------------------------------------
-spec truncate_insecure(user_ctx:ctx(), file_ctx:ctx(),
    Size :: non_neg_integer(), UpdateTimes :: boolean(),
    OnStorage :: boolean()) -> fslogic_worker:fuse_response().
truncate_insecure(UserCtx, FileCtx, Size, UpdateTimes, OnStorage) ->
    FileCtx2 = update_quota(FileCtx, Size),

    FileCtx4 = case OnStorage of
        true ->
            SessId = user_ctx:get_session_id(UserCtx),
            {SFMHandle, FileCtx3} = storage_file_manager:new_handle(SessId, FileCtx2),
            case storage_file_manager:open(SFMHandle, write) of
                {ok, Handle} ->
                    ok = storage_file_manager:truncate(Handle, Size),
                    ok = storage_file_manager:release(Handle),
                    ok = file_popularity:update_size(FileCtx3, Size);
                {error, ?ENOENT} ->
                    ok
            end,
            FileCtx3;
        _ ->
            FileCtx2
    end,

    case UpdateTimes of
        true ->
            fslogic_times:update_mtime_ctime(FileCtx4);
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
