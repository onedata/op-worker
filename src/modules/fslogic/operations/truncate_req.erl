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
truncate(UserCtx, FileCtx0, Size) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_object]
    ),
    FileCtx2 = file_ctx:assert_not_readonly_storage(FileCtx1),
    truncate_insecure(UserCtx, FileCtx2, Size, true).


%%--------------------------------------------------------------------
%% @doc
%% Truncates file on storage and returns only if operation is complete.
%% Model's size should be
%% changed by write events.
%% @end
%%--------------------------------------------------------------------
-spec truncate_insecure(user_ctx:ctx(), file_ctx:ctx(),
    Size :: non_neg_integer(), UpdateTimes :: boolean()) ->
    fslogic_worker:fuse_response().
truncate_insecure(UserCtx, FileCtx0, Size, UpdateTimes) ->
    FileCtx1 = update_quota(FileCtx0, Size),
    SessId = user_ctx:get_session_id(UserCtx),
    NextStep = case file_ctx:is_readonly_storage(FileCtx1) of
        {true, FileCtx2} ->
            {continue, FileCtx2};
        {false, FileCtx2} ->
            {SDHandle, FileCtx3} = storage_driver:new_handle(SessId, FileCtx2),
            case storage_driver:open(SDHandle, write) of
                {ok, Handle} ->
                    {CurrentSize, _} = file_ctx:get_file_size(FileCtx3),
                    case storage_driver:truncate(Handle, Size, CurrentSize) of
                        ok ->
                            ok;
                        Error = {error, ?EBUSY} ->
                            log_warning(storage_driver, truncate, Error, FileCtx3)
                    end,
                    case storage_driver:release(Handle) of
                        ok -> ok;
                        Error2 = {error, ?EDOM} ->
                            log_warning(storage_driver, release, Error2, FileCtx3)
                    end,
                    {continue, FileCtx3};
                {error, ?ENOENT} ->
                    case sd_utils:create_deferred(FileCtx3, user_ctx:new(?ROOT_SESS_ID), false, true) of
                        {#document{}, FileCtx5} ->
                            FinalAns = truncate_insecure(UserCtx, FileCtx5, Size, UpdateTimes),
                            {return, FinalAns};
                        Error3 = {error, _} ->
                            log_warning(storage_driver, truncate, Error3, FileCtx3),
                            {continue, FileCtx3}
                    end
            end
    end,

    case NextStep of
        {continue, FileCtx4} ->
            case file_popularity:update_size(FileCtx4, Size) of
                ok -> ok;
                {error, not_found} -> ok
            end,
            case UpdateTimes of
                true ->
                    fslogic_times:update_mtime_ctime(FileCtx4);
                false ->
                    ok
            end,
            #fuse_response{status = #status{code = ?OK}};
        {return, Ans} ->
            Ans
    end.


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


-spec log_warning(atom(), atom(), {error, term()}, file_ctx:ctx()) -> ok.
log_warning(Module, Function, Error, FileCtx) ->
    {Path, FileCtx2} = file_ctx:get_canonical_path(FileCtx),
    {StorageFileId, FileCtx3} = file_ctx:get_storage_file_id(FileCtx2),
    Guid = file_ctx:get_guid_const(FileCtx3),
    ?warning("~p:~p on file {~p, ~p} with file_id ~p returned ~p",
        [Module, Function, Path, Guid, StorageFileId, Error]).