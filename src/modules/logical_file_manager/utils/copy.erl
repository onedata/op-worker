%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Implementation of copy.
%%% @end
%%%--------------------------------------------------------------------
-module(copy).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([copy/3]).

-define(COPY_BUFFER_SIZE, application:get_env(?APP_NAME, rename_file_chunk_size, 8388608)). % 8*1024*1024
-define(COPY_LS_SIZE, application:get_env(?APP_NAME, ls_chunk_size, 5000)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Checks file type and executes type specific copy function.
%%--------------------------------------------------------------------
-spec copy(session:id(), SourceEntry :: {guid, fslogic_worker:file_guid()}, TargetPath :: file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | {error, term()}.
copy(SessId, SourceEntry, TargetPath) ->
    try
        case logical_file_manager:stat(SessId, SourceEntry) of
            {ok, #file_attr{type = ?DIRECTORY_TYPE} = Attr} ->
                copy_dir(SessId, Attr, TargetPath);
            {ok, Attr} ->
                copy_file(SessId, Attr, TargetPath)
        end
    catch
        _:{badmatch, Error}  ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Checks permissions and copies directory.
%%--------------------------------------------------------------------
-spec copy_dir(session:id(), SourceEntry :: {guid, fslogic_worker:file_guid()},
    LogicalTargetPath :: file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | {error, term()}.
copy_dir(SessId, #file_attr{uuid = SourceGuid, mode = Mode}, LogicalTargetPath) ->
    {ok, TargetGuid} = logical_file_manager:mkdir(SessId, LogicalTargetPath),
    ok = copy_children(SessId, SourceGuid, LogicalTargetPath, 0),
    ok = copy_metadata(SessId, SourceGuid, TargetGuid, Mode),
    {ok, TargetGuid}.

%%--------------------------------------------------------------------
%% @doc Checks permissions and copies file.
%%--------------------------------------------------------------------
-spec copy_file(session:id(), SourceEntry :: {guid, fslogic_worker:file_guid()},
    LogicalTargetPath :: file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | {error, term()}.
copy_file(SessId, #file_attr{uuid = SourceGuid, mode = Mode}, LogicalTargetPath) ->
    {ok, TargetGuid} = logical_file_manager:create(SessId, LogicalTargetPath),
    {ok, SourceHandle} = logical_file_manager:open(SessId, {guid, SourceGuid}, read),
    {ok, TargetHandle} = logical_file_manager:open(SessId, {guid, TargetGuid}, write),
    {ok, _NewSourceHandle, _NewTargetHandle} = copy_file_content(SourceHandle, TargetHandle, 0),
    ok = copy_metadata(SessId, SourceGuid, TargetGuid, Mode),
    {ok, TargetGuid}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Copy file content from source to handle
%% @end
%%--------------------------------------------------------------------
-spec copy_file_content(lfm_files:handle(), lfm_files:handle(), non_neg_integer()) ->
    {ok, lfm_files:handle(), lfm_files:handle()} | {error, term()}.
copy_file_content(SourceHandle, TargetHandle, Offset) ->
    case logical_file_manager:read(SourceHandle, Offset, ?COPY_BUFFER_SIZE) of
        {ok, NewSourceHandle, <<>>} ->
            {ok, NewSourceHandle, TargetHandle};
        {ok, NewSourceHandle, Data} ->
            case logical_file_manager:write(TargetHandle, Offset, Data) of
                {ok, NewTargetHandle, N} ->
                    copy_file_content(NewSourceHandle, NewTargetHandle, Offset + N);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Copy children of file
%% @end
%%--------------------------------------------------------------------
-spec copy_children(session:id(), fslogic_worker:file_guid(), file_meta:path(), non_neg_integer()) ->
    ok | {error, term()}.
copy_children(SessId, ParentGuid, TargetPath, Offset) ->
    case logical_file_manager:ls(SessId, {guid, ParentGuid}, Offset, ?COPY_LS_SIZE) of
        {ok, []} ->
            ok;
        {ok, Children} ->
            lists:foreach(fun({ChildGuid, ChildName}) ->
                {ok, _} = copy(SessId, {guid, ChildGuid}, filename:join(TargetPath, ChildName))
            end, Children),
            ok;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Copy metadata of file
%% @end
%%--------------------------------------------------------------------
-spec copy_metadata(session:id(), fslogic_worker:file_guid(),
    fslogic_worker:file_guid(), file_meta:posix_permissions()) -> ok.
copy_metadata(SessId, SourceGuid, TargetGuid, Mode) ->
    {ok, Xattrs} = logical_file_manager:list_xattr(SessId, {guid, SourceGuid}, false, true),
    lists:foreach(fun
        (?ACL_KEY) ->
            ok;
        (?CDMI_COMPLETION_STATUS_KEY) ->
            ok;
        (XattrName) ->
            {ok, Xattr} = logical_file_manager:get_xattr(SessId, {guid, SourceGuid}, XattrName, false),
            ok = logical_file_manager:set_xattr(SessId, {guid, TargetGuid}, Xattr)
    end, Xattrs),
    case lists:member(?ACL_KEY, Xattrs) of
        true ->
            {ok, Xattr} = logical_file_manager:get_xattr(SessId, {guid, SourceGuid}, ?ACL_KEY, false),
            ok = logical_file_manager:set_xattr(SessId, {guid, TargetGuid}, Xattr);
        false ->
            ok = logical_file_manager:set_perms(SessId, {guid, TargetGuid}, Mode)
    end.