%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules is used to create archive with plain layout.
%%% @end
%%%-------------------------------------------------------------------
-module(plain_archive).
-author("Jakub Kudzia").

-include_lib("ctool/include/logging.hrl").

%% API
-export([archive_file/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec archive_file(file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx()) -> {ok, file_ctx:ctx()} | {error, term()}.
archive_file(FileCtx, TargetParentCtx, UserCtx) ->
    try
        archive_file_insecure(FileCtx, TargetParentCtx, UserCtx)
    catch
        Class:Reason ->
            Guid = file_ctx:get_logical_guid_const(FileCtx),
            ?error_stacktrace("Unexpected error ~p:~p occured during archivisation of file ~s.", [Class, Reason, Guid]),
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec archive_file_insecure(file_ctx:ctx(), file_ctx:ctx(), user_ctx:ctx()) -> {ok, file_ctx:ctx()} | error.
archive_file_insecure(FileCtx, TargetParentCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    {FileName, FileCtx2} = file_ctx:get_aliased_name(FileCtx, UserCtx),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx2),
    TargetParentGuid = file_ctx:get_logical_guid_const(TargetParentCtx),

    {ok, CopyGuid, _} = file_copy:copy(SessionId, FileGuid, TargetParentGuid, FileName, false),

    CopyCtx = file_ctx:new_by_guid(CopyGuid),
    {FileSize, CopyCtx2} = file_ctx:get_local_storage_file_size(CopyCtx),
    {SDHandle, CopyCtx3} = storage_driver:new_handle(SessionId, CopyCtx2),
    ok = storage_driver:flushbuffer(SDHandle, FileSize),
    {ok, CopyCtx3}.
