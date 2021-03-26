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
-module(file_copy).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/auth/acl.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([copy/4]).

-define(COPY_BUFFER_SIZE,
    application:get_env(?APP_NAME, rename_file_chunk_size, 8388608)). % 8*1024*1024
-define(COPY_LS_SIZE, application:get_env(?APP_NAME, ls_batch_size, 5000)).

-type child_entry() :: {
    OldGuid :: fslogic_worker:file_guid(),
    NewGuid :: fslogic_worker:file_guid(),
    NewParentGuid :: fslogic_worker:file_guid(),
    NewName :: file_meta:name()
}.

%%%===================================================================
%%% API
%%%===================================================================

-spec copy(session:id(), SourceGuid :: fslogic_worker:file_guid(),
    TargetParentGuid :: fslogic_worker:file_guid(),
    TargetName :: file_meta:name()) ->
    {ok, NewFileGuid :: fslogic_worker:file_guid(),
        [child_entry()]} | {error, term()}.
copy(_SessId, SourceGuid, SourceGuid, _TargetName) ->
    % attempt to copy file to itself
    {error, ?EINVAL};
copy(SessId, SourceGuid, TargetParentGuid, TargetName) ->
    {ok, SourcePath} = lfm:get_file_path(SessId, SourceGuid),
    {ok, TargetParentPath} = lfm:get_file_path(SessId, TargetParentGuid),
    SourcePathTokens = filepath_utils:split(SourcePath),
    TargetParentPathTokens = filepath_utils:split(TargetParentPath),
    case SourcePathTokens -- TargetParentPathTokens of
        [] ->
            % attempt to copy file to itself
            {error, ?EINVAL};
        _ ->
        copy_internal(SessId, SourceGuid, TargetParentGuid, TargetName)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec copy_internal(session:id(), SourceGuid :: fslogic_worker:file_guid(),
    TargetParentGuid :: fslogic_worker:file_guid(),
    TargetName :: file_meta:name()) ->
    {ok, NewFileGuid :: fslogic_worker:file_guid(),
        [child_entry()]} | {error, term()}.
copy_internal(SessId, SourceGuid, TargetParentGuid, TargetName) ->
    try
        case lfm:stat(SessId, {guid, SourceGuid}) of
            {ok, #file_attr{type = ?DIRECTORY_TYPE} = Attr} ->
                copy_dir(SessId, Attr, TargetParentGuid, TargetName);
            {ok, Attr} ->
                copy_file(SessId, Attr, TargetParentGuid, TargetName)
        end
    catch
        _:{badmatch, Error}  ->
            Error
    end.


-spec copy_dir(session:id(), #file_attr{},
    TargetParentGuid :: fslogic_worker:file_guid(),
    TargetName :: file_meta:name()) ->
    {ok, NewFileGuid :: fslogic_worker:file_guid(), [child_entry()]}.
copy_dir(SessId, #file_attr{guid = SourceGuid, mode = Mode}, TargetParentGuid, TargetName) ->
    {ok, TargetGuid} = lfm:mkdir(
        SessId, TargetParentGuid, TargetName, Mode),
    {ok, ChildEntries} = copy_children(SessId, SourceGuid, TargetGuid),
    ok = copy_metadata(SessId, SourceGuid, TargetGuid),
    {ok, TargetGuid, ChildEntries}.


-spec copy_file(session:id(), #file_attr{},
    TargetParentGuid :: fslogic_worker:file_guid(),
    TargetName :: file_meta:name()) ->
    {ok, NewFileGuid :: fslogic_worker:file_guid(), [child_entry()]}.
copy_file(SessId, #file_attr{guid = SourceGuid, mode = Mode}, TargetParentGuid, TargetName) ->
    {ok, {TargetGuid, TargetHandle}} = lfm:create_and_open(
        SessId, TargetParentGuid, TargetName, Mode, write),
    try
        {ok, SourceHandle} =
            lfm:open(SessId, {guid, SourceGuid}, read),
        try
            {ok, _NewSourceHandle, _NewTargetHandle} =
                copy_file_content(SourceHandle, TargetHandle, 0),
            ok = copy_metadata(SessId, SourceGuid, TargetGuid),
            ok = lfm:fsync(TargetHandle)
        after
            lfm:release(SourceHandle)
        end
    after
        lfm:release(TargetHandle)
    end,
    {ok, TargetGuid, []}.


-spec copy_file_content(lfm:handle(), lfm:handle(), non_neg_integer()) ->
    {ok, lfm:handle(), lfm:handle()} | {error, term()}.
copy_file_content(SourceHandle, TargetHandle, Offset) ->
    case lfm:check_size_and_read(SourceHandle, Offset, ?COPY_BUFFER_SIZE) of
        {ok, NewSourceHandle, <<>>} ->
            {ok, NewSourceHandle, TargetHandle};
        {ok, NewSourceHandle, Data} ->
            case lfm:write(TargetHandle, Offset, Data) of
                {ok, NewTargetHandle, N} ->
                    copy_file_content(NewSourceHandle, NewTargetHandle, Offset + N);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


-spec copy_children(session:id(), file_id:file_guid(), file_id:file_guid()) ->
    {ok, [child_entry()]} | {error, term()}.
copy_children(SessId, ParentGuid, TargetParentGuid) ->
    copy_children(SessId, ParentGuid, TargetParentGuid, ?INITIAL_LS_TOKEN, []).


-spec copy_children(session:id(), file_id:file_guid(), file_id:file_guid(), file_meta:list_token(), [child_entry()]) ->
    {ok, [child_entry()]} | {error, term()}.
copy_children(SessId, ParentGuid, TargetParentGuid, Token, ChildEntriesAcc) ->
    case lfm:get_children(SessId, {guid, ParentGuid}, #{token => Token, size => ?COPY_LS_SIZE}) of
        {ok, Children, ListExtendedInfo} ->
            % TODO VFS-6265 fix usage of file names from lfm:get_children as they contain
            % collision suffix which normally shouldn't be there
            ChildEntries = lists:foldl(fun({ChildGuid, ChildName}, ChildrenEntries) ->
                {ok, NewChildGuid, NewChildrenEntries} =
                    copy_internal(SessId, ChildGuid, TargetParentGuid, ChildName),
                [
                    {ChildGuid, NewChildGuid, TargetParentGuid, ChildName} |
                        NewChildrenEntries ++ ChildrenEntries
                ]
            end, [], Children),
            AllChildEntries = ChildEntriesAcc ++ ChildEntries,
            case maps:get(is_last, ListExtendedInfo) of
                true ->
                    {ok, AllChildEntries};
                false ->
                    NewToken = maps:get(token, ListExtendedInfo),
                    copy_children(SessId, ParentGuid, TargetParentGuid, NewToken, AllChildEntries)
            end;
        Error ->
            Error
    end.


-spec copy_metadata(session:id(), fslogic_worker:file_guid(),
    fslogic_worker:file_guid()) -> ok.
copy_metadata(SessId, SourceGuid, TargetGuid) ->
    {ok, Xattrs} = lfm:list_xattr(
        SessId, {guid, SourceGuid}, false, true
    ),
    lists:foreach(fun
        (?ACL_KEY) ->
            ok;
        (?CDMI_COMPLETION_STATUS_KEY) ->
            ok;
        (XattrName) ->
            {ok, Xattr} = lfm:get_xattr(
                SessId, {guid, SourceGuid}, XattrName, false),
            ok = lfm:set_xattr(SessId, {guid, TargetGuid}, Xattr)
    end, Xattrs),

    {ok, Acl} = lfm:get_acl(SessId, {guid, SourceGuid}),
    lfm:set_acl(SessId, {guid, TargetGuid}, Acl).
