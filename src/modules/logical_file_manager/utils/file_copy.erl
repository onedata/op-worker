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
-include("modules/fslogic/acl.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([copy/4, copy/5]).

-define(COPY_BUFFER_SIZE,
    op_worker:get_env(rename_file_chunk_size, 52428800)). % 50*1024*1024

-type child_entry() :: {
    OldGuid :: fslogic_worker:file_guid(),
    NewGuid :: fslogic_worker:file_guid(),
    NewParentGuid :: fslogic_worker:file_guid(),
    NewName :: file_meta:name()
}.

% Callback called after each successful write operation; 
% the number in the function argument is the number of successfully written bytes.
-type callback() :: fun((non_neg_integer()) -> continue | abort).

-type options() :: #{
    % when enabled whole subtree will be copied
    recursive => boolean(),
    % when enabled existing target files will be overwritten instead of returning an error
    overwrite => boolean(),
    % callback called after each successful write operation
    on_write_callback => callback()
}.

-export_type([options/0]).

-define(DEFAULT_RECURSIVE_OPT, true).
-define(DEFAULT_OVERWRITE_OPT, false).
-define(DEFAULT_WRITE_CALLBACK_OPT, fun(_) -> continue end).

%%%===================================================================
%%% API
%%%===================================================================

-spec copy(session:id(), SourceGuid :: fslogic_worker:file_guid(),
    TargetParentGuid :: fslogic_worker:file_guid(),
    TargetName :: file_meta:name()) ->
    {ok, NewFileGuid :: fslogic_worker:file_guid(),
        [child_entry()]} | {error, term()}.
copy(SessId, SourceGuid, TargetParentGuid, TargetName) ->
    copy(SessId, SourceGuid, TargetParentGuid, TargetName, #{}).


-spec copy(session:id(), fslogic_worker:file_guid(), fslogic_worker:file_guid(), 
    file_meta:name(), options()) ->
    {ok, fslogic_worker:file_guid(), [child_entry()]} | {error, term()}.
copy(_SessId, SourceGuid, SourceGuid, _TargetName, _Options) ->
    % attempt to copy file to itself
    {error, ?EINVAL};
copy(SessId, SourceGuid, TargetParentGuid, TargetName, Options) ->
    {ok, SourcePath} = lfm:get_file_path(SessId, SourceGuid),
    {ok, TargetParentPath} = lfm:get_file_path(SessId, TargetParentGuid),
    Recursive = maps:get(recursive, Options, ?DEFAULT_RECURSIVE_OPT),
    case filepath_utils:is_equal_or_descendant(TargetParentPath, SourcePath) of
        {true, _} when Recursive ->
            % attempt to copy file to itself
            {error, ?EINVAL};
        _ ->
            copy_internal(SessId, SourceGuid, TargetParentGuid, TargetName, Options)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec copy_internal(session:id(), SourceGuid :: fslogic_worker:file_guid(),
    TargetParentGuid :: fslogic_worker:file_guid(),
    TargetName :: file_meta:name(), options()
) ->
    {ok, NewFileGuid :: fslogic_worker:file_guid(),
        [child_entry()]} | {error, term()}.
copy_internal(SessId, SourceGuid, TargetParentGuid, TargetName, Options) ->
    try
        case lfm:stat(SessId, ?FILE_REF(SourceGuid)) of
            {ok, #file_attr{type = ?DIRECTORY_TYPE} = Attr} ->
                copy_dir(SessId, Attr, TargetParentGuid, TargetName, Options);
            {ok, #file_attr{type = ?REGULAR_FILE_TYPE} = Attr} ->
                copy_file(SessId, Attr, TargetParentGuid, TargetName, Options);
            {ok, #file_attr{type = ?SYMLINK_TYPE} = Attr} ->
                copy_symlink(SessId, Attr, TargetParentGuid, TargetName);
            {error, _} = Error ->
                Error
        end
    catch
        _:{badmatch, Error2}  ->
            Error2
    end.


-spec copy_dir(session:id(), #file_attr{},
    TargetParentGuid :: fslogic_worker:file_guid(),
    TargetName :: file_meta:name(), options()
) ->
    {ok, NewFileGuid :: fslogic_worker:file_guid(), [child_entry()]}.
copy_dir(SessId, #file_attr{guid = SourceGuid, mode = Mode}, TargetParentGuid, TargetName, Options) ->
    % copy dir with default perms as it should be possible to copy its children even without the write permission
    {ok, TargetGuid} = case 
        lfm:mkdir(SessId, TargetParentGuid, TargetName, ?DEFAULT_DIR_MODE) 
    of
        {ok, TG} -> 
            {ok, TG};
        {error, eexist} = Error ->
            case maps:get(overwrite, Options, ?DEFAULT_OVERWRITE_OPT) of
                true ->
                    lfm:resolve_guid_by_relative_path(SessId, TargetParentGuid, TargetName);
                false -> Error
            end;
        Error ->
            Error
    end,
    ChildEntries2 = case maps:get(recursive, Options, ?DEFAULT_RECURSIVE_OPT) of
        true ->
            {ok, ChildEntries} = copy_children(SessId, SourceGuid, TargetGuid, Options),
            ChildEntries;
        false ->
            []
    end,
    ok = copy_metadata(SessId, SourceGuid, TargetGuid, Mode),
    {ok, TargetGuid, ChildEntries2}.


-spec copy_file(session:id(), #file_attr{},
    TargetParentGuid :: fslogic_worker:file_guid(),
    TargetName :: file_meta:name(), options()) ->
    {ok, NewFileGuid :: fslogic_worker:file_guid(), [child_entry()]}.
copy_file(SessId, #file_attr{guid = SourceGuid, mode = Mode}, TargetParentGuid, TargetName, Options) ->
    {ok, {TargetGuid, TargetHandle}} = case 
        lfm:create_and_open(SessId, TargetParentGuid, TargetName, Mode, write) 
    of
        {ok, {TG, TH}} -> 
            {ok, {TG, TH}};
        {error, eexist} = Error ->
            case maps:get(overwrite, Options, ?DEFAULT_OVERWRITE_OPT) of
                true -> 
                    {ok, Guid} = lfm:resolve_guid_by_relative_path(SessId, TargetParentGuid, TargetName),
                    ok = lfm:truncate(SessId, #file_ref{guid = Guid}, 0),
                    {ok, H} = lfm:open(SessId, ?FILE_REF(Guid), write),
                    {ok, {Guid, H}};
                false -> Error
            end;
        Error -> 
            Error
    end,
    try
        {ok, SourceHandle} = lfm:open(SessId, ?FILE_REF(SourceGuid), read),
        try
            BufferSize = get_buffer_size(TargetGuid),
            {ok, _NewSourceHandle, _NewTargetHandle} = copy_file_content(
                SourceHandle, TargetHandle, 0, BufferSize, 
                maps:get(on_write_callback, Options, ?DEFAULT_WRITE_CALLBACK_OPT)
            ),
            ok = copy_metadata(SessId, SourceGuid, TargetGuid, Mode),
            ok = lfm:fsync(TargetHandle)
        after
            lfm:release(SourceHandle)
        end
    after
        lfm:release(TargetHandle)
    end,
    {ok, TargetGuid, []}.


-spec copy_symlink(session:id(), #file_attr{},
    TargetParentGuid :: fslogic_worker:file_guid(),
    TargetName :: file_meta:name()) ->
    {ok, NewFileGuid :: fslogic_worker:file_guid(), [child_entry()]}.
copy_symlink(SessId, #file_attr{guid = SourceGuid}, TargetParentGuid, TargetName) ->
    {ok, SymlinkValue} = lfm:read_symlink(SessId, ?FILE_REF(SourceGuid)),
    {ok, #file_attr{guid = CopyGuid}} =
        lfm:make_symlink(SessId, ?FILE_REF(TargetParentGuid), TargetName, SymlinkValue),
    {ok, CopyGuid, []}.


-spec copy_file_content(lfm:handle(), lfm:handle(), non_neg_integer(), non_neg_integer(), callback()) ->
    {ok, lfm:handle(), lfm:handle()} | {error, term()}.
copy_file_content(SourceHandle, TargetHandle, Offset, BufferSize, Callback) ->
    case lfm:check_size_and_read(SourceHandle, Offset, ?COPY_BUFFER_SIZE) of
        {ok, NewSourceHandle, <<>>} ->
            {ok, NewSourceHandle, TargetHandle};
        {ok, NewSourceHandle, Data} ->
            case lfm:write(TargetHandle, Offset, Data) of
                {ok, NewTargetHandle, N} ->
                    case Callback(byte_size(Data)) of
                        continue ->
                            copy_file_content(
                                NewSourceHandle, NewTargetHandle, Offset + N, BufferSize, Callback);
                        abort ->
                            {ok, NewSourceHandle, NewTargetHandle}
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


-spec copy_children(session:id(), file_id:file_guid(), file_id:file_guid(), options()) ->
    {ok, [child_entry()]} | {error, term()}.
copy_children(SessId, ParentGuid, TargetParentGuid, Options) ->
    copy_children(SessId, ParentGuid, TargetParentGuid, #{tune_for_large_continuous_listing => true}, [], Options).


-spec copy_children(session:id(), file_id:file_guid(), file_id:file_guid(), file_listing:options(), 
    [child_entry()], options()) -> {ok, [child_entry()]} | {error, term()}.
copy_children(SessId, ParentGuid, TargetParentGuid, ListingOpts, ChildEntriesAcc, Options) ->
    case lfm:get_children_attrs(SessId, ?FILE_REF(ParentGuid), ListingOpts, [?attr_guid, ?attr_name]) of
        {ok, Children, ListingPaginationToken} ->
            % TODO VFS-6265 fix usage of file names from lfm:get_children as they contain
            % collision suffix which normally shouldn't be there
            ChildEntries = lists:foldl(fun(#file_attr{guid = ChildGuid, name = ChildName}, ChildrenEntries) ->
                {ok, NewChildGuid, NewChildrenEntries} =
                    copy_internal(SessId, ChildGuid, TargetParentGuid, ChildName, Options),
                [
                    {ChildGuid, NewChildGuid, TargetParentGuid, ChildName} |
                        NewChildrenEntries ++ ChildrenEntries
                ]
            end, [], Children),
            AllChildEntries = ChildEntriesAcc ++ ChildEntries,
            case file_listing:is_finished(ListingPaginationToken) of
                true ->
                    {ok, AllChildEntries};
                false ->
                    NextPageListingOpts = #{pagination_token => ListingPaginationToken},
                    copy_children(SessId, ParentGuid, TargetParentGuid, NextPageListingOpts, 
                        AllChildEntries, Options)
            end;
        Error ->
            Error
    end.


-spec copy_metadata(session:id(), fslogic_worker:file_guid(),
    fslogic_worker:file_guid(), file_meta:posix_permissions()) -> ok.
copy_metadata(SessId, SourceGuid, TargetGuid, Mode) ->
    {ok, Xattrs} = lfm:list_xattr(SessId, ?FILE_REF(SourceGuid), false, true),

    lists:foreach(fun
        (?ACL_KEY) ->
            ok;
        (?CDMI_COMPLETION_STATUS_KEY) ->
            ok;
        (?ARCHIVISATION_METADATA_CHECKSUM_KEY) -> 
            ok;
        (?ARCHIVISATION_CONTENT_CHECKSUM_KEY) ->
            ok;
        (?ARCHIVISATION_CHILDREN_COUNT_KEY) ->
            ok;
        (XattrName) ->
            {ok, Xattr} = lfm:get_xattr(
                SessId, ?FILE_REF(SourceGuid), XattrName, false),
            ok = lfm:set_xattr(SessId, ?FILE_REF(TargetGuid), Xattr)
    end, Xattrs),

    {ok, Acl} = lfm:get_acl(SessId, ?FILE_REF(SourceGuid)),
    lfm:set_acl(SessId, ?FILE_REF(TargetGuid), Acl),
    lfm:set_perms(SessId, ?FILE_REF(TargetGuid), Mode).


-spec get_buffer_size(file_id:file_guid()) -> non_neg_integer().
get_buffer_size(FileGuid) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    case space_logic:is_supported(?ROOT_SESS_ID, SpaceId, oneprovider:get_id()) of
        true ->
            {SDHandle, _FileCtx2} = storage_driver:new_handle(?ROOT_SESS_ID, file_ctx:new_by_guid(FileGuid)),
            case storage_driver:blocksize_for_path(SDHandle) of
                {ok, 0} -> ?COPY_BUFFER_SIZE; % on imported storage blockSize can be equal to 0
                {ok, Size} -> Size;
                _ -> ?COPY_BUFFER_SIZE
            end;
        false ->
            ?COPY_BUFFER_SIZE
    end.