%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules is used to create archive with plain layout.
%%%
%%% If archive is created with create_nested_archives=true,
%%% archives for nested datasets are also created and symlinks to these
%%% nested archives are created in parent archives.
%%% If create_nested_archives=false, files are simply copied.
%%%
%%%-------------------------------------------------------------------
%%% Example
%%%-------------------------------------------------------------------
%%% Following file structure
%%%
%%% Dir1(DS1)
%%% |--- f.txt (DS2)
%%% |--- f2.txt (DS3)
%%% |--- f3.txt
%%% |--- Dir1.1(DS4)
%%%      |--- hello.txt
%%%
%%% will have the following archive structure, in case of create_nested_archives=true:
%%%
%%% .__onedata_archive
%%% |--- dataset_DS1
%%% |    |--- archive_123
%%% |         |--- Dir1
%%% |              |---  f.txt  (SL -> dataset_DS2/archive_1234/f.txt)
%%% |              |---  f2.txt (SL -> dataset_DS3/archive_1235/f2.txt)
%%% |              |---  f3.txt
%%% |              |---  Dir1.1 (SL -> dataset_DS4/archive_1236/Dir1.1)
%%% |
%%% |--- dataset_DS2
%%% |    |--- archive_1234
%%% |         |--- f.txt
%%% |
%%% |--- dataset_DS3
%%% |    |--- archive_1235
%%% |         |--- f2.txt
%%% |
%%% |--- dataset_DS4
%%%      |--- archive_1236
%%%           |--- Dir1.1
%%%                |--- hello.txt
%%%
%%% If create_nested_archives=false, the structure will be as follows:
%%%
%%% .__onedata_archive
%%% |--- dataset_DS1
%%%      |--- archive_123
%%%           |--- Dir1
%%%                |--- f.txt
%%%                |--- f2.txt
%%%                |--- f3.txt
%%%                |--- Dir1.1
%%%                      |--- hello.txt
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
