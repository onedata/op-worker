%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module offers a high level API for operating on logical filesystem.
%%% When passing a file in arguments, one can use one of the following:
%%% {uuid, FileUUIDBin} - preferred and fast. UUIDs are returned from 'ls' function.
%%% {path, BinaryFilePath} - slower than by uuid (path has to be resolved). Discouraged, but there are cases when this is useful.
%%% {handle, BinaryHandle} - fastest, but not possible in all functions. The handle can be obtained from open function.
%%%
%%% This module is merely a convenient wrapper that calls functions from lfm_xxx modules.
%%% @end
%%%-------------------------------------------------------------------
-module(logical_file_manager).


% TODO Issues connected with logical_files_manager
% 1) User context lib - a simple library is needed to set and retrieve the user context. If not context is set, it should crash
% with an error. 'root' atom can be used to set root context.
% 2) Structure of file handle - what should it contain:
%   - file uuid
%   - expiration time
%   - connected session
%   - session type
%   - open mode
%   - ??
% 3) How to get rid of rubbish like not closed opens - check expired handles if a session connected with them has ended?
% 4) All errors should be listed in one hrl -> errors.hrl
% 4) Common types should be listed in one hrl -> types.hrl
% 5) chown is no longer featured in lfm as it is not needed by higher layers
% 6) Blocks related functions should go to another module (synchronize, mark_as_truncated etc).


-define(run(F),
    try
        F()
    catch
        _:{badmatch, {error, {not_found, file_meta}}} ->
            {error, ?ENOENT};
        _:___Reason ->
            ?error_stacktrace("logical_file_manager generic error: ~p", [___Reason]),
            {error, ___Reason}
    end).


-include("types.hrl").
-include("errors.hrl").
-include("modules/fslogic/lfm_internal.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

-type handle() :: #lfm_handle{}.

-export_type([handle/0]).

%% Functions operating on directories
-export([mkdir/2, ls/4, get_children_count/2]).
%% Functions operating on directories or files
-export([exists/1, mv/2, cp/2]).
%% Functions operating on files
-export([create/3, open/3, write/3, read/3, truncate/2, truncate/3, get_block_map/1, unlink/1, unlink/2]).
%% Functions concerning file permissions
-export([set_perms/2, check_perms/2, set_acl/2, get_acl/1]).
%% Functions concerning file attributes
-export([stat/1, stat/2, set_xattr/3, get_xattr/2, remove_xattr/2, list_xattr/1]).
%% Functions concerning symbolic links
-export([create_symlink/2, read_symlink/1, remove_symlink/1]).
%% Functions concerning file shares
-export([create_share/2, get_share/1, remove_share/1]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates a directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec mkdir(SessId :: session:id(), Path :: file_path()) -> {ok, file_uuid()} | error_reply().
mkdir(SessId, Path) ->
    lfm_dirs:mkdir(Path).


%%--------------------------------------------------------------------
%% @doc
%% Lists some contents of a directory.
%% Returns up to Limit of entries, starting with Offset-th entry.
%%
%% @end
%%--------------------------------------------------------------------
-spec ls(SessId :: session:id(), FileKey :: file_id_or_path(), Limit :: integer(), Offset :: integer()) -> {ok, [{file_uuid(), file_name()}]} | error_reply().
ls(SessId, FileKey, Limit, Offset) ->
    lfm_dirs:ls(FileKey, Limit, Offset).


%%--------------------------------------------------------------------
%% @doc
%% Returns number of children of a directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_children_count(SessId :: session:id(), FileKey :: file_id_or_path()) -> {ok, integer()} | error_reply().
get_children_count(SessId, FileKey) ->
    lfm_dirs:get_children_count(FileKey).


%%--------------------------------------------------------------------
%% @doc
%% Checks if a file or directory exists.
%%
%% @end
%%--------------------------------------------------------------------
-spec exists(FileKey :: file_key()) -> {ok, boolean()} | error_reply().
exists(FileKey) ->
    lfm_files:exists(FileKey).


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location.
%%
%% @end
%%--------------------------------------------------------------------
-spec mv(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
mv(FileKeyFrom, PathTo) ->
    lfm_files:mv(FileKeyFrom, PathTo).


%%--------------------------------------------------------------------
%% @doc
%% Copies a file or directory to given location.
%%
%% @end
%%--------------------------------------------------------------------
-spec cp(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
cp(PathFrom, PathTo) ->
    lfm_files:cp(PathFrom, PathTo).


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec unlink(handle()) -> ok | error_reply().
unlink(#lfm_handle{fslogic_ctx = #fslogic_ctx{session_id = SessId}, file_uuid = UUID}) ->
    unlink(SessId, {uuid, UUID}).

-spec unlink(session:id(), fslogic_worker:file()) -> ok | error_reply().
unlink(SessId, FileEntry) ->
    ?run(fun() ->
        CTX = fslogic_context:new(SessId),
        lfm_files:unlink(CTX, ensure_uuid(CTX, FileEntry))
    end).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file.
%%
%% @end
%%--------------------------------------------------------------------
-spec create(SessId :: session:id(), Path :: file_path(), Mode :: file_meta:posix_permissions()) ->
    {ok, file_uuid()} | error_reply().
create(SessId, Path, Mode) ->
    try
        CTX = fslogic_context:new(SessId),
        {ok, Tokens} = fslogic_path:verify_file_path(Path),
        Entry = fslogic_path:get_canonical_file_entry(CTX, Tokens),
        {ok, CanonicalPath} = file_meta:gen_path(Entry),
        lfm_files:create(CTX, CanonicalPath, Mode)
    catch
        _:Reason ->
            ?error_stacktrace("Create error for file ~p: ~p", [Path, Reason]),
            {error, Reason}
    end .


%%--------------------------------------------------------------------
%% @doc
%% Opens a file in selected mode and returns a file handle used to read or write.
%%
%% @end
%%--------------------------------------------------------------------
-spec open(session:id(), FileKey :: file_id_or_path(), OpenType :: open_mode()) -> {ok, handle()} | error_reply().
open(SessId, FileKey, OpenType) ->
    CTX = fslogic_context:new(SessId),
    lfm_files:open(CTX, ensure_uuid(CTX, FileKey), OpenType).


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file. Returns number of written bytes.
%%
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: handle(), Offset :: integer(), Buffer :: binary()) ->
    {ok, NewHandle :: handle(), integer()} | error_reply().
write(FileHandle, Offset, Buffer) ->
    Size = size(Buffer),
    try lfm_files:write(FileHandle, Offset, Buffer) of
        {error, Reason} ->
            {error, Reason};
        {ok, _, Size} = Ret1 ->
            Ret1;
        {ok, _, 0} = Ret2 ->
            Ret2;
        {ok, NewHandle, Written} ->
            case write(NewHandle, Offset + Written, binary:part(Buffer, Written, Size - Written)) of
                {ok, NewHandle1, Written1} ->
                    {ok, NewHandle1, Written + Written1};
                {error, Reason1} ->
                    {error, Reason1}
            end
    catch
        _:Error ->
            ?error_stacktrace("Write error for file ~p: ~p", [FileHandle, Error]),
            {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: handle(), binary()} | error_reply().
read(FileHandle, Offset, MaxSize) ->
    try lfm_files:read(FileHandle, Offset, MaxSize) of
        {error, Reason} ->
            {error, Reason};
        {ok, NewHandle, Bytes} = Ret1 ->
            case size(Bytes) of
                MaxSize ->
                    Ret1;
                0 ->
                    Ret1;
                Size ->
                    case lfm_files:read(NewHandle, Offset + Size, MaxSize - Size) of
                        {ok, NewHandle1, Bytes1} ->
                            {ok, NewHandle1, <<Bytes/binary, Bytes1/binary>>};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
    catch
        _:Error ->
            ?error_stacktrace("Read error for file ~p: ~p", [FileHandle, Error]),
            {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Truncates a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec truncate(FileHandle :: handle(), Size :: non_neg_integer()) -> ok | error_reply().
truncate(#lfm_handle{file_uuid = FileUUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}, Size) ->
    truncate(SessId, {uuid, FileUUID}, Size).

-spec truncate(SessId :: session:id(), FileKey :: file_id_or_path(), Size :: non_neg_integer()) -> ok | error_reply().
truncate(SessId, FileKey, Size) ->
    try
        CTX = fslogic_context:new(SessId),
        {uuid, FileUUID} = ensure_uuid(CTX, FileKey),
        lfm_files:truncate(CTX, FileUUID, Size)
    catch
        _:Reason ->
            ?error_stacktrace("truncate error for file ~p: ~p", [FileKey, Reason]),
            {error, Reason}
    end .


%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%%
%% @end
%%--------------------------------------------------------------------

-spec get_block_map(FileHandle :: handle()) -> {ok, [block_range()]} | error_reply().
get_block_map(#lfm_handle{file_uuid = FileUUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}) ->
    get_block_map(SessId, {uuid, FileUUID}).

-spec get_block_map(SessId :: session:id(), FileKey :: file_id_or_path()) -> {ok, [block_range()]} | error_reply().
get_block_map(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    lfm_files:get_block_map(CTX, ensure_uuid(CTX, FileKey)).


%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec set_perms(FileKey :: file_key(), NewPerms :: perms_octal()) -> ok | error_reply().
set_perms(Path, NewPerms) ->
    lfm_perms:set_perms(Path, NewPerms).


%%--------------------------------------------------------------------
%% @doc
%% Checks if current user has given permissions for given file.
%%
%% @end
%%--------------------------------------------------------------------
-spec check_perms(FileKey :: file_key(), PermsType :: permission_type()) -> {ok, boolean()} | error_reply().
check_perms(Path, PermType) ->
    lfm_perms:check_perms(Path, PermType).


%%--------------------------------------------------------------------
%% @doc
%% Returns file's Access Control List.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_acl(FileKey :: file_key()) -> {ok, [access_control_entity()]} | error_reply().
get_acl(Path) ->
    lfm_perms:get_acl(Path).


%%--------------------------------------------------------------------
%% @doc
%% Updates file's Access Control List.
%%
%% @end
%%--------------------------------------------------------------------
-spec set_acl(FileKey :: file_key(), EntityList :: [access_control_entity()]) -> ok | error_reply().
set_acl(Path, EntityList) ->
    lfm_perms:set_acl(Path, EntityList).


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes.
%%
%% @end
%%--------------------------------------------------------------------
-spec stat(handle()) -> {ok, file_attributes()} | error_reply().
stat(#lfm_handle{file_uuid = UUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}) ->
    stat(SessId, {uuid, UUID}).

-spec stat(session:id(), file_key()) -> {ok, file_attributes()} | error_reply().
stat(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    lfm_attrs:stat(CTX, FileKey).


%%--------------------------------------------------------------------
%% @doc
%% Returns file's extended attribute by key.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_xattr(FileKey :: file_key(), Key :: xattr_key()) -> {ok, xattr_value()} | error_reply().
get_xattr(Path, Key) ->
    lfm_attrs:get_xattr(Path, Key).


%%--------------------------------------------------------------------
%% @doc
%% Updates file's extended attribute by key.
%%
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(FileKey :: file_key(), Key :: xattr_key(), Value :: xattr_value()) -> ok |  error_reply().
set_xattr(Path, Key, Value) ->
    lfm_attrs:set_xattr(Path, Key, Value).


%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%%
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(FileKey :: file_key(), Key :: xattr_key()) -> ok |  error_reply().
remove_xattr(Path, Key) ->
    lfm_attrs:remove_xattr(Path, Key).


%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes of a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(FileKey :: file_key()) -> {ok, [{Key :: xattr_key(), Value :: xattr_value()}]} | error_reply().
list_xattr(Path) ->
    lfm_attrs:list_xattr(Path).


%%--------------------------------------------------------------------
%% @doc
%% Creates a symbolic link.
%%
%% @end
%%--------------------------------------------------------------------
-spec create_symlink(Path :: binary(), TargetFileKey :: file_key()) -> {ok, file_uuid()} | error_reply().
create_symlink(Path, TargetFileKey) ->
    lfm_links:create_symlink(Path, TargetFileKey).


%%--------------------------------------------------------------------
%% @doc
%% Returns the symbolic link's target file.
%%
%% @end
%%--------------------------------------------------------------------
-spec read_symlink(FileKey :: file_key()) -> {ok, {file_uuid(), file_name()}} | error_reply().
read_symlink(FileKey) ->
    lfm_links:read_symlink(FileKey).


%%--------------------------------------------------------------------
%% @doc
%% Removes a symbolic link.
%%
%% @end
%%--------------------------------------------------------------------
-spec remove_symlink(FileKey :: file_key()) -> ok | error_reply().
remove_symlink(FileKey) ->
    lfm_links:remove_symlink(FileKey).


%%--------------------------------------------------------------------
%% @doc
%% Creates a share for given file. File can be shared with anyone or
%% only specified group of users.
%%
%% @end
%%--------------------------------------------------------------------
-spec create_share(FileKey :: file_key(), ShareWith :: all | [{user, user_id()} | {group, group_id()}]) ->
    {ok, ShareID :: share_id()} | error_reply().
create_share(Path, ShareWith) ->
    lfm_shares:create_share(Path, ShareWith).


%%--------------------------------------------------------------------
%% @doc
%% Returns shared file by share_id.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_share(ShareID :: share_id()) -> {ok, {file_uuid(), file_name()}} | error_reply().
get_share(ShareID) ->
    lfm_shares:get_share(ShareID).


%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareID.
%%
%% @end
%%--------------------------------------------------------------------
-spec remove_share(ShareID :: share_id()) -> ok | error_reply().
remove_share(ShareID) ->
    lfm_shares:remove_share(ShareID).


%%--------------------------------------------------------------------
%% @doc
%% Converts given file entry to UUID.
%% @end
%%--------------------------------------------------------------------
-spec ensure_uuid(fslogic_worker:ctx(), fslogic_worker:file()) -> {uuid, file_uuid()}.
ensure_uuid(_CTX, {uuid, UUID}) ->
    {uuid, UUID};
ensure_uuid(_CTX, #document{key = UUID}) ->
    {uuid, UUID};
ensure_uuid(CTX, {path, Path}) ->
    {uuid, fslogic_path:to_uuid(CTX, Path)}.

