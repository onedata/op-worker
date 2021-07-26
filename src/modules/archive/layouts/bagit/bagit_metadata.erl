%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper for bagit_archive module.
%%% It contains utility functions for archiving files' custom metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(bagit_metadata).
-author("Jakub Kudzia").

-include("modules/dataset/bagit.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/fslogic_common.hrl").


%% API
-export([init/2, add_entry/4]).

% TODO VFS-7831 handle metadata files bigger than 1GB
-define(MAX_METADATA_FILE_SIZE, 1073741824). % 1 GB
-define(CRITICAL_SECTION(FileGuid, Fun), critical_section:run({?MODULE, FileGuid}, Fun)).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init(file_ctx:ctx(), user_ctx:ctx()) -> ok.
init(ArchiveDirCtx, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    ParentGuid = file_ctx:get_logical_guid_const(ArchiveDirCtx),
    {ok, {_Guid, Handle}} = lfm:create_and_open(SessionId, ParentGuid, ?METADATA_FILE_NAME, ?DEFAULT_FILE_MODE, write),
    {ok, NewHandle} = dump(Handle, #{}),
    ok = lfm:release(NewHandle).


-spec add_entry(file_ctx:ctx(), user_ctx:ctx(), file_meta:path(), json_utils:json_term()) -> ok.
add_entry(ArchiveDirCtx, UserCtx, FilePath, MetadataJson) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    {MetadataFileCtx, _} = files_tree:get_child(ArchiveDirCtx, ?METADATA_FILE_NAME, UserCtx),
    MetadataFileGuid = file_ctx:get_logical_guid_const(MetadataFileCtx),
    ?CRITICAL_SECTION(MetadataFileGuid, fun() ->
        case lfm:open(SessionId, ?FILE_REF(MetadataFileGuid), rdwr) of
            {ok, Handle} ->
                {ok, NewHandle, CurrentJson} = load(Handle),
                UpdatedJson = CurrentJson#{FilePath => MetadataJson},
                {ok, NewHandle2} = dump(NewHandle, UpdatedJson),
                lfm:release(NewHandle2);
            Error ->
                Error
        end
    end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec load(lfm:handle()) -> {ok, lfm:handle(), json_utils:json_term()}.
load(Handle) ->
    case lfm:check_size_and_read(Handle, 0, ?MAX_METADATA_FILE_SIZE) of
        {ok, NewHandle, Content} ->
            Json = json_utils:decode(Content),
            {ok, NewHandle, Json};
        Error ->
            Error
    end.


-spec dump(lfm:handle(), json_utils:json_term()) -> {ok, lfm:handle()}.
dump(Handle, MetadataJson) ->
    case lfm:write(Handle, 0, json_utils:encode(MetadataJson)) of
        {ok, NewHandle, _} ->
            lfm:fsync(NewHandle),
            {ok, NewHandle};
        Error ->
            Error
    end.