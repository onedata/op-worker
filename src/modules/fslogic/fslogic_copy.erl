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
-module(fslogic_copy).
-author("Tomasz Lichon").

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([copy/3]).

-define(COPY_BUFFER_SIZE, 33554432). % 32*1024*1024
-define(COPY_LS_SIZE, 1000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Checks file type and executes type specific copy function.
%%--------------------------------------------------------------------
-spec copy(CTX :: fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    LogicalTargetPath :: file_meta:path()) ->
    #provider_response{} | no_return().
copy(CTX, SourceEntry, LogicalTargetPath) ->
    case file_meta:get(SourceEntry) of
        {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}} = SourceDoc} ->
            copy_dir(CTX, SourceDoc, LogicalTargetPath);
        {ok, #document{value = #file_meta{type = _}} = SourceDoc} ->
            copy_file(CTX, SourceDoc, LogicalTargetPath)
    end.

%%--------------------------------------------------------------------
%% @doc Checks permissions and copies directory.
%%--------------------------------------------------------------------
-spec copy_dir(CTX :: fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    LogicalTargetPath :: file_meta:path()) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?traverse_container, 2},
    {?list_container, 2}, {?read_metadata, 2}, {?read_attributes, 2}, {?read_acl, 2}]).
copy_dir(CTX = #fslogic_ctx{session_id = SessId}, #document{key = SourceUuid, value = #file_meta{mode = Mode}}, LogicalTargetPath) ->
    SourceGuid = fslogic_uuid:uuid_to_guid(SourceUuid),
    {ok, TargetGuid} = logical_file_manager:mkdir(SessId, LogicalTargetPath),
    ok = copy_children(CTX, SourceGuid, LogicalTargetPath, 0),
    ok = copy_metadata(SessId, SourceGuid, TargetGuid, Mode),
    #provider_response{status = #status{code = ?OK}, provider_response = #file_copied{new_uuid = TargetGuid}}.

%%--------------------------------------------------------------------
%% @doc Checks permissions and copies file.
%%--------------------------------------------------------------------
-spec copy_file(CTX :: fslogic_worker:ctx(), SourceEntry :: fslogic_worker:file(),
    LogicalTargetPath :: file_meta:path()) ->
    #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?read_object, 2},
    {?read_metadata, 2}, {?read_attributes, 2}, {?read_acl, 2}]).
copy_file(#fslogic_ctx{session_id = SessId}, #document{key = SourceUuid, value = #file_meta{mode = Mode}}, LogicalTargetPath) ->
    SourceGuid = fslogic_uuid:uuid_to_guid(SourceUuid),
    {ok, TargetGuid} = logical_file_manager:create(SessId, LogicalTargetPath),
    {ok, SourceHandle} = logical_file_manager:open(SessId, {guid, SourceGuid}, read),
    {ok, TargetHandle} = logical_file_manager:open(SessId, {guid, TargetGuid}, write),
    {ok, _NewSourceHandle, _NewTargetHandle} = copy_file_content(SourceHandle, TargetHandle, 0),
    ok = copy_metadata(SessId, SourceGuid, TargetGuid, Mode),
    #provider_response{status = #status{code = ?OK}, provider_response = #file_copied{new_uuid = TargetGuid}}.

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
%% Copy file content from source to handle
%% @end
%%--------------------------------------------------------------------
-spec copy_children(fslogic_worker:ctx(), fslogic_worker:file_guid(), file_meta:path(), non_neg_integer()) ->
    ok | {error, term()}.
copy_children(CTX = #fslogic_ctx{session_id = SessId}, ParentGuid, TargetPath, Offset) ->
    case logical_file_manager:ls(SessId, {guid, ParentGuid}, Offset, ?COPY_LS_SIZE) of
        {ok, []} ->
            ok;
        {ok, Children} ->
            lists:foreach(fun({ChildGuid, ChildName}) ->
                #provider_response{status = #status{code = ?OK}} = copy(CTX,
                    {uuid, fslogic_uuid:guid_to_uuid(ChildGuid)}, filename:join(TargetPath, ChildName))
            end, Children);
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%%
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