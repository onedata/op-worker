%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides high level file system operations that
%% use logical names of files.
%% @end
%% ===================================================================

-module(logical_files_manager).

-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/fslogic/fslogic_available_blocks.hrl").
-include("oneprovider_modules/dao/dao_vfs.hrl").
-include("oneprovider_modules/dao/dao_share.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include_lib("oneprovider_modules/dao/dao_types.hrl").
-include_lib("ctool/include/logging.hrl").

%% ====================================================================
%% API
%% ====================================================================
%% Logical file organization management (only db is used)

-export([mkdir/1, rmdir/1, mv/2, chown/2, ls/3, ls_chunked/1, ls_chunked/3,
    getfileattr/1, get_xattr/2, set_xattr/3, remove_xattr/2, list_xattr/1, get_acl/1, set_acl/2,
    rmlink/1, read_link/1, create_symlink/2]).
%% File access (db and helper are used)
-export([rmdir_recursive/1, cp/2, read/4, read/3, write/4, write/3, write_file_chunk/2, create/1, truncate/2, delete/1, exists/1, error_to_string/1]).
-export([change_file_perm/3, check_file_perm/2]).
-export([get_file_children_count/1]).

%% Block synchronization
-export([synchronize/3, mark_as_modified/5, mark_as_truncated/4]).
-export([get_file_block_map/1]).

%% File sharing
-export([get_file_by_uuid/1, get_file_uuid/1, get_file_full_name_by_uuid/1, get_file_name_by_uuid/1, get_file_user_dependent_name_by_uuid/1]).
-export([create_standard_share/1, create_share/2, get_share/1, remove_share/1]).

-export([sync_from_remote/2]).

%% ====================================================================
%% Test API
%% ====================================================================
-ifdef(TEST).
%% eunit
-export([cache_size/2]).
%% ct
-export([getfilelocation/1]).
-export([doUploadTest/4]).
-endif.

%% ====================================================================
%% API functions
%% ====================================================================

%% ====================================================================
%% Logical file organization management (only db is used)
%% ====================================================================

sync_from_remote(Path, ProviderId) ->
    FullFileName = fslogic_path:get_full_file_name(Path),
    {ok, #fileattributes{size = Size}} = getfileattr(FullFileName),
    SyncReq = #synchronizefileblock{logical_name = FullFileName, offset = 0, size = Size},
    Request = #fusemessage{message_type = "synchronizefileblock", input = fuse_messages_pb:encode_synchronizefileblock(SyncReq)},
    {GRUID, AccessToken} = fslogic_context:get_gr_auth(),
    #atom{value = Value} = provider_proxy:reroute_pull_message(ProviderId, {GRUID, AccessToken}, ?CLUSTER_FUSE_ID, Request),
    Value.

%% read_link/1
%% ====================================================================
%% @doc Reads symbolic link from DB.
%% @end
-spec read_link(Path :: path()) -> {ok, LinkValue :: string()} | {error | logical_file_system_error, Reason :: any()}.
%% ====================================================================
read_link(Path) ->
    Record = #getlink{file_logic_name = Path},
    {Status, TmpAns} = contact_fslogic(Record),
    case Status of
        ok ->
            Response = TmpAns#linkinfo.answer,
            case Response of
                ?VOK ->
                    {ok, TmpAns#linkinfo.file_logic_name};
                _ ->
                    {logical_file_system_error, Response}
            end;
        _ ->
            {Status, TmpAns}
    end.


%% create_symlink/2
%% ====================================================================
%% @doc Creates symbolic link in DB.
%% @end
-spec create_symlink(LinkValue :: string(), LinkFilePath :: path()) -> ok | {error | logical_file_system_error, Reason :: any()}.
%% ====================================================================
create_symlink(LinkValue, LinkFile) ->
    Record = #createlink{from_file_logic_name = LinkFile, to_file_logic_name = LinkValue},
    {Status, TmpAns} = contact_fslogic(Record),
    case Status of
        ok ->
            Response = TmpAns#atom.value,
            case Response of
                ?VOK -> ok;
                _ ->
                    {logical_file_system_error, Response}
            end;
        _ ->
            {Status, TmpAns}
    end.


%% mkdir/1
%% ====================================================================
%% @doc Creates directory (in db)
%% @end
-spec mkdir(DirName :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
mkdir(DirName) ->
    {ModeStatus, NewFileLogicMode} = get_mode(DirName),
    case ModeStatus of
        ok ->
            Record = #createdir{dir_logic_name = DirName, mode = NewFileLogicMode},
            {Status, TmpAns} = contact_fslogic(Record),
            case Status of
                ok ->
                    Response = TmpAns#atom.value,
                    case Response of
                        ?VOK -> ok;
                        ?VEEXIST -> {error, dir_exists};
                        _ -> {logical_file_system_error, Response}
                    end;
                _ -> {Status, TmpAns}
            end;
        _ -> {error, cannot_get_file_mode}
    end.

%% rmlink/1
%% ====================================================================
%% @doc Deletes link (in db)
%% @end
-spec rmlink(LnkName :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
rmlink(LnkName) ->
    delete_special(LnkName).

%% rmdir/1
%% ====================================================================
%% @doc Deletes directory (in db)
%% @end
-spec rmdir(DirName :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
rmdir(DirName) ->
    delete_special(DirName).

%% mv/2
%% ====================================================================
%% @doc Moves directory (in db)
%% @end
-spec mv(From :: string(), To :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
mv(From, To) ->
    Record = #renamefile{from_file_logic_name = From, to_file_logic_name = To},
    {Status, TmpAns} = contact_fslogic(Record),
    case Status of
        ok ->
            Response = TmpAns#atom.value,
            case Response of
                ?VOK -> clear_cache(From);
                _ ->
                    clear_cache(From),
                    {logical_file_system_error, Response}
            end;
        _ ->
            clear_cache(From),
            {Status, TmpAns}
    end.

%% ls_chunked/1
%% ====================================================================
%% @doc @equiv ls_chunked(Path, 0, 10, all, [])
%% @end
-spec ls_chunked(string()) -> Result when
    Result :: {ok, [#dir_entry{}]} | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
ls_chunked(Path) ->
    ls_chunked(Path, 0, 10, all, []).

%% ls_chunked/3
%% ====================================================================
%% @doc @equiv ls_chunked(Path, From, 10, To - From + 1, [])
%% @end
-spec ls_chunked(string(), integer(), integer()) -> Result when
    Result :: {ok, [#dir_entry{}]} | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
ls_chunked(Path, From, To) ->
    ls_chunked(Path, From, 10, To - From + 1, []).

%% chown/2
%% ====================================================================
%% @doc Changes owner of file (in db)
%% @end
-spec chown(FileName :: string(), Uid :: non_neg_integer()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
chown(FileName, Uid) ->
    Record = #changefileowner{file_logic_name = FileName, uid = Uid},
    {Status, TmpAns} = contact_fslogic(Record),
    case Status of
        ok ->
            Response = TmpAns#atom.value,
            case Response of
                ?VOK -> ok;
                _ -> {logical_file_system_error, Response}
            end;
        _ -> {Status, TmpAns}
    end.

%% ls/3
%% ====================================================================
%% @doc Lists directory (uses data from db)
%% @end
-spec ls(DirName :: string(), ChildrenNum :: integer(), Offset :: integer()) -> Result when
    Result :: {ok, FilesList} | {ErrorGeneral, ErrorDetail},
    FilesList :: list(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
ls(DirName, ChildrenNum, Offset) ->
    Record = #getfilechildren{dir_logic_name = DirName, children_num = ChildrenNum, offset = Offset},
    {Status, TmpAns} = contact_fslogic(Record),
    case Status of
        ok ->
            Response = TmpAns#filechildren.answer,
            case Response of
                ?VOK ->
                    DirEnt = lists:map(
                        fun(#filechildren_direntry{name = Name, type = Type}) ->
                            #dir_entry{name = Name, type = Type}
                        end, TmpAns#filechildren.entry),
                    {ok, DirEnt};
                _ -> {logical_file_system_error, Response}
            end;
        _ -> {Status, TmpAns}
    end.

%% getfileattr/1
%% ====================================================================
%% @doc Returns file attributes
%% @end
-spec getfileattr(FileName :: string()) -> Result when
    Result :: {ok, Attributes} | {ErrorGeneral, ErrorDetail},
    Attributes :: term(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
getfileattr({uuid, UUID}) ->
    getfileattr(getfileattr, UUID);

getfileattr(FileName) ->
    Record = #getfileattr{file_logic_name = FileName},
    getfileattr(internal_call, Record).

%% getfileattr/2
%% ====================================================================
%% @doc Returns file attributes
%% @end
-spec getfileattr(Message :: atom(), Value :: term()) -> Result when
    Result :: {ok, Attributes} | {ErrorGeneral, ErrorDetail},
    Attributes :: term(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
getfileattr(Message, Value) ->
    {Status, TmpAns} = contact_fslogic(Message, Value),
    case Status of
        ok ->
            ?debug("getfileattr: ~p", [TmpAns]),
            Response = TmpAns#fileattr.answer,
            case Response of
                ?VOK -> {ok, #fileattributes{
                    mode = TmpAns#fileattr.mode,
                    uid = TmpAns#fileattr.uid,
                    gid = TmpAns#fileattr.gid,
                    atime = TmpAns#fileattr.atime,
                    mtime = TmpAns#fileattr.mtime,
                    ctime = TmpAns#fileattr.ctime,
                    type = TmpAns#fileattr.type,
                    size = TmpAns#fileattr.size,
                    uname = TmpAns#fileattr.uname,
                    gname = TmpAns#fileattr.gname,
                    links = TmpAns#fileattr.links,
                    has_acl = TmpAns#fileattr.has_acl
                }};
                _ -> {logical_file_system_error, Response}
            end;
        _ -> {Status, TmpAns}
    end.

%% get_xattr/2
%% ====================================================================
%% @doc Gets file's extended attribute by name.
%% @end
-spec get_xattr(FullFileName :: string(), Name :: binary()) ->
    {ok, binary()} | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
get_xattr(FullFileName, Name) ->
    {Status, TmpAns} = contact_fslogic(#getxattr{file_logic_name = FullFileName, name = Name}),
    case Status of
        ok ->
            case TmpAns#xattr.answer of
                ?VOK -> {ok, TmpAns#xattr.value};
                Error -> {logical_file_system_error, Error}
            end;
        _ -> {Status, TmpAns}
    end.

%% set_xattr/3
%% ====================================================================
%% @doc Sets file's extended attribute as {Name, Value}.
%% @end
-spec set_xattr(FullFileName :: string(), Name :: binary(), Value :: binary()) ->
    ok | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
set_xattr(FullFileName, Name, Value) ->
    {Status, TmpAns} = contact_fslogic(#setxattr{file_logic_name = FullFileName, name = Name, value = Value}),
    case Status of
        ok ->
            case TmpAns#atom.value of
                ?VOK -> ok;
                Error -> {logical_file_system_error, Error}
            end;
        _ -> {Status, TmpAns}
    end.

%% remove_xattr/2
%% ====================================================================
%% @doc Removes file's extended attribute with given Name.
%% @end
-spec remove_xattr(FullFileName :: string(), Name :: binary()) ->
    ok | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
remove_xattr(FullFileName, Name) ->
    {Status, TmpAns} = contact_fslogic(#removexattr{file_logic_name = FullFileName, name = Name}),
    case Status of
        ok ->
            case TmpAns#atom.value of
                ?VOK -> ok;
                Error -> {logical_file_system_error, Error}
            end;
        _ -> {Status, TmpAns}
    end.

%% list_xattr/1
%% ====================================================================
%% @doc Gets file's extended attribute list.
%% @end
-spec list_xattr(FullFileName :: string()) ->
    {ok, list()} | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
list_xattr(FullFileName) ->
    {Status, TmpAns} = contact_fslogic(#listxattr{file_logic_name = FullFileName}),
    case Status of
        ok ->
            case TmpAns#xattrlist.answer of
                ?VOK ->
                    {ok, [{Name, Value} || #xattrlist_xattrentry{name = Name, value = Value} <- TmpAns#xattrlist.attrs]};
                Error -> {logical_file_system_error, Error}
            end;
        _ -> {Status, TmpAns}
    end.

%% get_acl/1
%% ====================================================================
%% @doc Gets file's access controll list.
%% @end
-spec get_acl(FullFileName :: string()) ->
    {ok, list(#accesscontrolentity{})} | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
get_acl(FullFileName) ->
    {Status, TmpAns} = contact_fslogic(#getacl{file_logic_name = FullFileName}),
    case Status of
        ok ->
            case TmpAns#acl.answer of
                ?VOK -> {ok, TmpAns#acl.entities};
                Error -> {logical_file_system_error, Error}
            end;
        _ -> {Status, TmpAns}
    end.

%% set_acl/2
%% ====================================================================
%% @doc Sets file's access controll list.
%% @end
-spec set_acl(FullFileName :: string(), EntitiyList :: list(#accesscontrolentity{})) ->
    ok | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
set_acl(FullFileName, EntitiyList) ->
    {Status, TmpAns} = contact_fslogic(#setacl{file_logic_name = FullFileName, entities = EntitiyList}),
    case Status of
        ok ->
            case TmpAns#atom.value of
                ?VOK -> ok;
                Error -> {logical_file_system_error, Error}
            end;
        _ -> {Status, TmpAns}
    end.

%% ====================================================================
%% File access (db and helper are used)
%% ====================================================================

%% rmdir_recursive/1
%% ====================================================================
%% @doc Removes given dir with all files and subdirectories.
%% @end
-spec rmdir_recursive(DirPath :: string()) -> Result when
    Result :: ok | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
rmdir_recursive(DirPath) ->
    case fslogic_path:is_space_dir(DirPath) of
        true -> {logical_file_system_error, ?VEACCES};
        false ->
            case ls_chunked(DirPath) of
                {ok, Childs} ->
                    lists:foreach(
                        fun(#dir_entry{name = Name, type = ?REG_TYPE_PROT}) ->
                            logical_files_manager:delete(filename:join(DirPath, Name));
                            (#dir_entry{name = Name, type = ?DIR_TYPE_PROT}) ->
                                rmdir_recursive(filename:join(DirPath, Name))
                        end,
                        Childs),
                    logical_files_manager:rmdir(DirPath);
                Error -> Error
            end
    end.

%% cp/2
%% ====================================================================
%% @doc Copies file or directory
%% @end
-spec cp(From :: string(), To :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
cp(From, To) ->
    {ok, #fileattributes{type = Type, has_acl = HasAcl}} = getfileattr(From),
    case Type of
        ?DIR_TYPE_PROT ->
            case mkdir(To) of
                ok ->
                    case {case HasAcl of true -> copy_file_acl(From, To); false -> ok end, copy_file_xattr(From, To)} of
                        {ok, ok} ->
                            case ls_chunked(From) of
                                {ok, ChildList} ->
                                    AnswerList = lists:map(fun(#dir_entry{name = Name}) ->
                                        cp(filename:join(From, Name), filename:join(To, Name)) end, ChildList),
                                    case lists:filter(fun(ok) -> false; (_) -> true end, AnswerList) of
                                        [] -> ok;
                                        [Error | _] ->
                                            rmdir_recursive(To),
                                            Error
                                    end;
                                Error ->
                                    rmdir(To),
                                    Error
                            end;
                        {ok, Error} ->
                            rmdir_recursive(To),
                            Error;
                        {Error, _} ->
                            rmdir_recursive(To),
                            Error
                    end;
                Error ->
                    Error
            end;
        ?REG_TYPE_PROT ->
            case create(To) of
                ok ->
                    case copy_file_content(From, To, 0, ?default_copy_buffer_size) of
                        ok ->
                            Ans = {case HasAcl of true -> copy_file_acl(From, To); false ->
                                ok end, copy_file_xattr(From, To)},
                            case Ans of
                                {ok, ok} -> ok;
                                {ok, Error} ->
                                    delete(To),
                                    Error;
                                {Error, _} ->
                                    delete(To),
                                    Error
                            end;
                        Error ->
                            delete(To),
                            Error
                    end;
                Error -> Error
            end;
        ?LNK_TYPE_PROT ->
            case read_link(From) of
                {ok, Value} -> create_symlink(Value, To);
                Error -> Error
            end
    end.

%% read/3
%% ====================================================================
%% @equiv read(File, Offset, Size, generate_events)
-spec read(File :: term(), Offset :: integer(), Size :: integer()) -> Result when
    Result :: {ok, Bytes} | {ErrorGeneral, ErrorDetail},
    Bytes :: binary(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
read(File, Offset, Size) ->
    read(File, Offset, Size, generate_events).


%% read/4
%% ====================================================================
%% @doc Reads file (uses logical name of file). First it gets information
%% about storage helper and file id at helper. Next it uses storage helper
%% to read data from file.
%% File can be string (path) or {uuid, UUID}.
%% @end
-spec read(File :: term(), Offset :: integer(), Size :: integer(),
    EventPolicy :: generate_events | no_events) -> Result when
    Result :: {ok, Bytes} | {ErrorGeneral, ErrorDetail},
    Bytes :: binary(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
read(File, Offset, Size, EventPolicy) ->
    {Response, Response2} = getfilelocation(File),
    synchronize(File, Offset, Size),
    case Response of
        ok ->
            {Storage_helper_info, FileId} = Response2,
            Res = storage_files_manager:read(Storage_helper_info, FileId, Offset, Size),
            case Res of
                {ok, Bytes} ->
                    case {event_production_enabled("read_event"), EventPolicy} of
                        {true, generate_events} ->
                            % TODO: add filePath
                            ReadEvent = [{"type", "read_event"}, {"user_dn", fslogic_context:get_user_dn()},
                                {"bytes", byte_size(Bytes)}, {"blocks", [{Offset, byte_size(Bytes)}]}],
                            gen_server:call(?Dispatcher_Name, {cluster_rengine, 1, {event_arrived, ReadEvent}});
                        _ ->
                            ok
                    end;
                _ ->
                    ok
            end,
            Res;
        _ -> {Response, Response2}
    end.


%% write_file_chunk/2
%% ====================================================================
%% @doc Write file chunk beggining at offset 0, consecutive calls cache
%% file size and appends data at the end
%% First it gets information about storage helper and file id at helper.
%% Next it uses storage helper to write data to file.
%% @end
-spec write_file_chunk(File :: term(), Buf :: binary()) -> Result when
    Result :: BytesWritten | {ErrorGeneral, ErrorDetail},
    BytesWritten :: integer(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
write_file_chunk({uuid, Uuid}, Buf) ->
    case get_file_path_from_cache({uuid, Uuid}) of
        {ok, FullFilePath} -> write_file_chunk(FullFilePath, Buf);
        Error -> Error
    end;
write_file_chunk(FilePath, Buf) ->
    case write_enabled(fslogic_context:get_user_dn()) of
        true ->
            case getfilelocation(FilePath) of
                {ok, {Storage_helper_info, FileId}} ->
                    Offset = cache_size(FilePath, byte_size(Buf)),
                    Res = storage_files_manager:write(Storage_helper_info, FileId, Offset, Buf),
                    case {is_integer(Res), event_production_enabled("write_event")} of
                        {true, true} ->
                            {ok, FullFileName} = get_file_path_from_cache(FilePath),
                            WriteEvent = [{"type", "write_event"}, {"user_dn", fslogic_context:get_user_dn()},
                                {"bytes", Res}, {"blocks", [{Offset, Res}]}, {"filePath", FullFileName}],
                            gen_server:call(?Dispatcher_Name, {cluster_rengine, 1, {event_arrived, WriteEvent}}),
                            WriteEventStats = [{"type", "write_for_stats"}, {"user_dn", fslogic_context:get_user_dn()},
                                {"bytes", Res}, {"blocks", [{Offset, Res}]}, {"filePath", FullFileName}],
                            gen_server:call(?Dispatcher_Name, {cluster_rengine, 1, {event_arrived, WriteEventStats}}),
                            WriteEventAvailableBlocks = [{"type", "write_for_available_blocks"}, {"user_dn", fslogic_context:get_user_dn()},
                                {"fuse_id", ?CLUSTER_FUSE_ID}, {"sequence_number", 0},
                                {"bytes", Res}, {"blocks", [{Offset, Res}]}, {"filePath", FullFileName}],
                            gen_server:call(?Dispatcher_Name, {cluster_rengine, 1, {event_arrived, WriteEventAvailableBlocks}});
                        _ ->
                            ok
                    end,
                    Res;
                Err -> Err
            end;
        _ ->
            {error, quota_exceeded}
    end.

%% write/3
%% ====================================================================
%% @equiv write(File, Offset, Buf, generate_events)
-spec write(File :: term(), Offset :: integer(), Buf :: binary()) -> Result when
    Result :: BytesWritten | {ErrorGeneral, ErrorDetail},
    BytesWritten :: integer(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
write(File, Offset, Buf) ->
    write(File, Offset, Buf, generate_events).

%% write/4
%% ====================================================================
%% @doc Writes data to file (uses logical name of file). First it gets
%% information about storage helper and file id at helper. Next it uses
%% storage helper to write data to file.
%% @end
-spec write(File :: term(), Offset :: integer(), Buf :: binary(),
    EventPolicy :: generate_events | no_events) -> Result when
    Result :: BytesWritten | {ErrorGeneral, ErrorDetail},
    BytesWritten :: integer(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
write({uuid, Uuid}, Offset, Buf, EventPolicy) ->
    case get_file_path_from_cache({uuid, Uuid}) of
        {ok, FullFilePath} -> write(FullFilePath, Offset, Buf, EventPolicy);
        Error -> Error
    end;
write(FilePath, Offset, Buf, EventPolicy) ->
    case write_enabled(fslogic_context:get_user_dn()) of
        true ->
            case getfilelocation(FilePath) of
                {ok, {Storage_helper_info, FileId}} ->
                    Res = storage_files_manager:write(Storage_helper_info, FileId, Offset, Buf),

                    case {is_integer(Res), event_production_enabled("write_event"), EventPolicy} of
                        {true, true, generate_events} ->
                            {ok, FullFileName} = get_file_path_from_cache(FilePath),
                            WriteEvent = [{"type", "write_event"}, {"user_dn", fslogic_context:get_user_dn()},
                                {"count", Res}, {"blocks", [{Offset, Res}]}, {"filePath", FullFileName}],
                            gen_server:call(?Dispatcher_Name, {cluster_rengine, 1, {event_arrived, WriteEvent}}),
                            WriteEventStats = [{"type", "write_for_stats"}, {"user_dn", fslogic_context:get_user_dn()},
                                {"bytes", Res}, {"blocks", [{Offset, Res}]}, {"filePath", FullFileName}],
                            gen_server:call(?Dispatcher_Name, {cluster_rengine, 1, {event_arrived, WriteEventStats}}),
                            WriteEventAvailableBlocks = [{"type", "write_for_available_blocks"}, {"user_dn", fslogic_context:get_user_dn()},
                                {"fuse_id", ?CLUSTER_FUSE_ID}, {"sequence_number", 0},
                                {"bytes", Res}, {"blocks", [{Offset, Res}]}, {"filePath", FullFileName}],
                            gen_server:call(?Dispatcher_Name, {cluster_rengine, 1, {event_arrived, WriteEventAvailableBlocks}});
                        _ ->
                            ok
                    end,
                    Res;
                Err -> Err
            end;
        _ ->
            {error, quota_exceeded}
    end.

%% create/1
%% ====================================================================
%% @doc Creates file (uses logical name of file). First it creates file
%% in db and gets information about storage helper and file id at helper.
%% Next it uses storage helper to create file on storage.
%% @end
-spec create(File :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
create(File) ->
    {ModeStatus, NewFileLogicMode} = get_mode(File),
    case ModeStatus of
        ok ->
            Record = #getnewfilelocation{file_logic_name = File, mode = NewFileLogicMode},
            {Status, TmpAns} = contact_fslogic(Record),
            case Status of
                ok ->
                    Response = TmpAns#filelocation.answer,
                    case Response of
                        ?VOK ->
                            Storage_helper_info = #storage_helper_info{name = TmpAns#filelocation.storage_helper_name, init_args = TmpAns#filelocation.storage_helper_args},
                            case storage_files_manager:create(Storage_helper_info, TmpAns#filelocation.file_id) of
                                ok ->
                                    Record2 = #createfileack{file_logic_name = File},
                                    {Status2, TmpAns2} = contact_fslogic(Record2),
                                    case Status of
                                        ok ->
                                            Response2 = TmpAns2#atom.value,
                                            case Response2 of
                                                ?VOK ->
                                                    ok;
                                                _ ->
                                                    {logical_file_system_error, {cannot_confirm_file_creation, Response2}}
                                            end;
                                        _ -> {Status2, TmpAns2}
                                    end;
                                {wrong_mknod_return_code, -17} ->
                                    {error, file_exists};
                                StorageBadAns ->
                                    StorageBadAns
                            end;
                        ?VEEXIST -> {error, file_exists};
                        _ -> {logical_file_system_error, Response}
                    end;
                _ -> {Status, TmpAns}
            end;
        _ -> {error, cannot_get_file_mode}
    end.

%% truncate/2
%% ====================================================================
%% @doc Truncates file (uses logical name of file). First it gets
%% information about storage helper and file id at helper.
%% Next it uses storage helper to truncate file on storage.
%% @end
-spec truncate(File :: string(), Size :: integer()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
truncate({uuid, Uuid}, Size) ->
    case get_file_path_from_cache({uuid, Uuid}) of
        {ok, FullFilePath} -> truncate(FullFilePath, Size);
        Error -> Error
    end;
truncate(FilePath, Size) ->
    case getfilelocation(FilePath) of
        {ok, {Storage_helper_info, FileId}} ->
            Res = storage_files_manager:truncate(Storage_helper_info, FileId, Size),
            case {Res, event_production_enabled("truncate_event")} of
                {ok, true} ->
                    {ok, FullFileName} = get_file_path_from_cache(FilePath),
                    TruncateEvent = [{"type", "truncate_event"}, {"user_dn", fslogic_context:get_user_dn()}, {"filePath", FullFileName}],
                    gen_server:call(?Dispatcher_Name, {cluster_rengine, 1, {event_arrived, TruncateEvent}}),
                    TruncateEventAvailableBlocks = [{"type", "truncate_for_available_blocks"}, {"user_dn", fslogic_context:get_user_dn()},
                        {"fuse_id", ?CLUSTER_FUSE_ID}, {"sequence_number", 0}, {"filePath", FullFileName}, {"newSize", Size}],
                    gen_server:call(?Dispatcher_Name, {cluster_rengine, 1, {event_arrived, TruncateEventAvailableBlocks}});
                _ ->
                    ok
            end,
            Res;
        Err -> Err
    end.

%% delete/1
%% ====================================================================
%% @doc Deletes file (uses logical name of file). First it gets
%% information about storage helper and file id at helper. Next it uses
%% storage helper to delete file from storage. Afterwards it deletes
%% information about file from db.
%% @end
-spec delete(File :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
delete(File) ->
    {Response, Response2} = getfilelocation(File),
    case Response of
        ok ->
            {Storage_helper_info, FileId} = Response2,
            TmpAns2 = storage_files_manager:delete(Storage_helper_info, FileId),

            TmpAns2_2 = case TmpAns2 of
                            {wrong_getatt_return_code, -2} -> ok;
                            _ -> TmpAns2
                        end,

            case TmpAns2_2 of
                ok ->
                    Record2 = #deletefile{file_logic_name = File},
                    {Status3, TmpAns3} = contact_fslogic(Record2),
                    case Status3 of
                        ok ->
                            Response3 = TmpAns3#atom.value,
                            case Response3 of
                                ?VOK ->
                                    case event_production_enabled("rm_event") of
                                        true ->
                                            RmEvent = [{"type", "rm_event"}, {"user_dn", fslogic_context:get_user_dn()}],
                                            gen_server:call(?Dispatcher_Name, {cluster_rengine, 1, {event_arrived, RmEvent}});
                                        _ ->
                                            ok
                                    end,
                                    clear_cache(File);
                                _ ->
                                    clear_cache(File),
                                    {logical_file_system_error, Response3}
                            end;
                        _ ->
                            clear_cache(File),
                            {Status3, TmpAns3}
                    end;
                _ ->
                    clear_cache(File),
                    TmpAns2_2
            end;
        _ ->
            clear_cache(File),
            {Response, Response2}
    end.

%% change_file_perm/3
%% ====================================================================
%% @doc Changes file's permissions in db and at storage (if the file is regular).
%% @end
-spec change_file_perm(FileName :: string(), NewPerms :: integer(), IsRegular :: boolean()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
change_file_perm(FileName, NewPerms, IsRegular) ->
    Record = #changefileperms{file_logic_name = FileName, perms = NewPerms},
    {Status, TmpAns} = contact_fslogic(Record),
    case Status of
        ok ->
            Response = TmpAns#atom.value,
            case Response of
                ?VOK ->
                    case IsRegular of
                        false ->
                            ok;
                        true ->
                            {LocStatus, Response2} = getfilelocation(FileName),
                            case LocStatus of
                                ok ->
                                    {Storage_helper_info, FileId} = Response2,
                                    storage_files_manager:chmod(Storage_helper_info, FileId, NewPerms);
                                _ -> {LocStatus, Response2}
                            end
                    end;
                _ -> {logical_file_system_error, Response}
            end;
        _ -> {Status, TmpAns}
    end.

%% check_file_perms/2
%% ====================================================================
%% @doc Checks permissions to open the file in chosen mode.
%% @end
-spec check_file_perm(FileName :: string(), Type :: root | owner | delete | read | write | execute | rdwr | '') -> Result when
    Result :: boolean() | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
check_file_perm(FileName, Type) ->
    Record = #checkfileperms{file_logic_name = FileName, type = atom_to_list(Type)},
    {Status, TmpAns} = contact_fslogic(Record),
    case Status of
        ok ->
            Response = TmpAns#atom.value,
            case Response of
                ?VOK -> true;
                _ -> false
            end;
        _ -> {Status, TmpAns}
    end.

%% exists/1
%% ====================================================================
%% @doc Checks if file exists.
%% @end
-spec exists(File :: string()) -> Result when
    Result :: boolean() | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
exists(FileName) ->
    {FileNameFindingAns, File} = fslogic_path:get_full_file_name(FileName),
    case FileNameFindingAns of
        ok ->
            {Status, TmpAns} = fslogic_objects:get_file(1, File, ?CLUSTER_FUSE_ID),
            case {Status, TmpAns} of
                {ok, _} -> true;
                {error, file_not_found} -> false;
                _ -> {Status, TmpAns}
            end;
        _ -> {full_name_finding_error, File}
    end.

%% get_file_children_count/1
%% ====================================================================
%% @doc Counts first level childrens of directory.
%% @end
-spec get_file_children_count(DirName :: string()) -> Result when
    Result :: {ok, non_neg_integer()} | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
get_file_children_count(DirName) ->
    {Status, TmpAns} = contact_fslogic(#getfilechildrencount{dir_logic_name = DirName}),
    case Status of
        ok ->
            case TmpAns#filechildrencount.answer of
                ?VOK -> {ok, TmpAns#filechildrencount.count};
                Error -> {logical_file_system_error, Error}
            end;
        _ -> {Status, TmpAns}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% contact_fslogic/1
%% ====================================================================
%% @doc Sends request to and receives answer from fslogic
%% @end
-spec contact_fslogic(Record :: record()) -> Result when
    Result :: {ok, FSLogicAns} | {ErrorGeneral, ErrorDetail},
    FSLogicAns :: record(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
contact_fslogic(Record) ->
    contact_fslogic(internal_call, Record).

%% contact_fslogic/2
%% ====================================================================
%% @doc Sends request to and receives answer from fslogic
%% @end
-spec contact_fslogic(Message :: atom(), Value :: term()) -> Result when
    Result :: {ok, FSLogicAns} | {ErrorGeneral, ErrorDetail},
    FSLogicAns :: record(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
contact_fslogic(Message, Value) ->
    MsgId = case get(files_manager_msg_id) of
                ID when is_integer(ID) ->
                    put(files_manager_msg_id, ID + 1);
                _ -> put(files_manager_msg_id, 0)
            end,

    Timeout = case Value of
                  #renamefile{} ->
                      timer:minutes(10);
                  _ ->
                      timer:seconds(7)
              end,

    try
        CallAns = case Message of
                      internal_call ->
                          gen_server:call(?Dispatcher_Name, {fslogic, 1, self(), MsgId,
                              #worker_request{access_token = fslogic_context:get_gr_auth(), subject = fslogic_context:get_user_dn(),
                                  request = {internal_call, Value}}});
                      _ -> gen_server:call(?Dispatcher_Name, {fslogic, 1, self(), MsgId, {Message, Value}})
                  end,

        case CallAns of
            ok ->
                receive
                    {worker_answer, MsgId, Resp} -> {ok, Resp}
                after Timeout ->
                    ?error("Logical files manager: error during contact with fslogic, timeout"),
                    {error, timeout}
                end;
            _ ->
                ?error("Logical files manager: error during contact with fslogic, call ans: ~p", [CallAns]),
                {error, CallAns}
        end
    catch
        E1:E2 ->
            ?error("Logical files manager: error during contact with fslogic: ~p:~p", [E1, E2]),
            {error, dispatcher_error}
    end.

%% get_file_by_uuid/1
%% ====================================================================
%% @doc Gets file record on the basis of uuid.
%% @end
-spec get_file_by_uuid(UUID :: string()) -> Result when
    Result :: {ok, File} | {ErrorGeneral, ErrorDetail},
    File :: term(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
get_file_by_uuid(UUID) ->
    dao_lib:apply(dao_vfs, get_file, [{uuid, UUID}], 1).

%% get_file_uuid/1
%% ====================================================================
%% @doc Gets uuid on the basis of filepath.
%% @end
-spec get_file_uuid(Filepath :: string()) -> Result when
    Result :: {ok, Uuid} | {ErrorGeneral, ErrorDetail},
    Uuid :: uuid(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
get_file_uuid(FileName) ->
    {Status, TmpAns} = contact_fslogic(#getfileuuid{file_logic_name = FileName}),
    case Status of
        ok ->
            case TmpAns#fileuuid.answer of
                ?VOK -> {ok, TmpAns#fileuuid.uuid};
                Error -> {logical_file_system_error, Error}
            end;
        _ -> {Status, TmpAns}
    end.

%% get_file_user_dependent_name_by_uuid/1
%% ====================================================================
%% @doc Gets file full name relative to user's dir on the basis of uuid.
%% @end
-spec get_file_user_dependent_name_by_uuid(UUID :: string()) -> Result when
    Result :: {ok, FullPath} | {ErrorGeneral, ErrorDetail},
    FullPath :: string(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
get_file_user_dependent_name_by_uuid(UUID) ->
    case get_file_full_name_by_uuid(UUID) of
        {ok, FullPath} ->
            case fslogic_objects:get_user() of
                {ok, UserDoc} ->
                    Login = user_logic:get_login(UserDoc),
                    case string:str(FullPath, Login ++ "/") of
                        1 -> {ok, string:sub_string(FullPath, length(Login ++ "/") + 1)};
                        _ -> {ok, FullPath}
                    end;
                {ErrorGeneral, ErrorDetail} ->
                    {ErrorGeneral, ErrorDetail}
            end;
        {ErrorGeneral, ErrorDetail} ->
            {ErrorGeneral, ErrorDetail}
    end.

%% get_file_name_by_uuid/1
%% ====================================================================
%% @doc Gets file name on the basis of uuid.
%% @end
-spec get_file_name_by_uuid(UUID :: string()) -> Result when
    Result :: {ok, Name} | {ErrorGeneral, ErrorDetail},
    Name :: term(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
get_file_name_by_uuid(UUID) ->
    case get_file_by_uuid(UUID) of
        {ok, #db_document{record = FileRec}} -> {ok, FileRec#file.name};
        _ -> {error, {get_file_by_uuid, UUID}}
    end.

%% get_file_full_name_by_uuid/1
%% ====================================================================
%% @doc Gets file full name (with root of the user's system) on the basis of uuid.
%% @end
-spec get_file_full_name_by_uuid(UUID :: string()) -> Result when
    Result :: {ok, FullPath} | {ErrorGeneral, ErrorDetail},
    FullPath :: string(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
get_file_full_name_by_uuid(UUID) ->
    get_full_path(UUID, "").

%% get_full_path/1
%% ====================================================================
%% @doc Gets file full path (with root of the user's system) on the basis of uuid.
%% @end
-spec get_full_path(UUID :: string(), TmpPath :: string()) -> Result when
    Result :: {ok, FullPath} | {ErrorGeneral, ErrorDetail},
    FullPath :: string(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
get_full_path("", TmpPath) ->
    {ok, TmpPath};

get_full_path(UUID, TmpPath) ->
    case get_file_by_uuid(UUID) of
        {ok, #db_document{record = FileRec}} ->
            case TmpPath of
                "" -> get_full_path(FileRec#file.parent, FileRec#file.name);
                _ -> get_full_path(FileRec#file.parent, FileRec#file.name ++ "/" ++ TmpPath)
            end;
        _ -> {error, {get_file_by_uuid, UUID}}
    end.

%% create_standard_share/1
%% ====================================================================
%% @doc Creates standard share info (share with all) for file (file path is
%% an argument).
%% @end
-spec create_standard_share(File :: string()) -> Result when
    Result :: {ok, Share_info} | {ErrorGeneral, ErrorDetail},
    Share_info :: term(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
create_standard_share(File) ->
    create_share(File, all).

%% create_share/2
%% ====================================================================
%% @doc Creates share info for file (file path is an argument).
%% @end
-spec create_share(File :: string(), Share_With :: term()) -> Result when
    Result :: {ok, Share_info} | {ErrorGeneral, ErrorDetail},
    Share_info :: term(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
create_share(File, Share_With) ->
    {Status, FullName} = fslogic_path:get_full_file_name(File),
    {Status2, UID} = fslogic_context:get_user_id(),
    case {Status, Status2} of
        {ok, ok} ->
            case fslogic_objects:get_file(1, FullName, ?CLUSTER_FUSE_ID) of
                {ok, #db_document{uuid = FUuid}} ->
                    Share_info = #share_desc{file = FUuid, user = UID, share_with = Share_With},
                    add_share(Share_info);
                Other -> Other
            end;
        {_, error} ->
            {Status2, UID};
        _ ->
            {Status, FullName}
    end.

%% add_share/1
%% ====================================================================
%% @doc Adds info about share to db.
%% @end
-spec add_share(Share_info :: term()) -> Result when
    Result :: {ok, Share_uuid} | {ErrorGeneral, ErrorDetail},
    Share_uuid :: term(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
add_share(Share_info) ->
    {Status, Ans} = get_share({file_uuid, Share_info#share_desc.file}),
    Found = case {Status, Ans} of
                {error, share_not_found} -> false;
                {ok, OneAns} when is_record(OneAns, db_document) ->
                    Sh_Inf = OneAns#db_document.record,
                    case Share_info#share_desc.share_with =:= Sh_Inf#share_desc.share_with of
                        true -> {true, OneAns};
                        _ -> false
                    end;
                {ok, _} ->
                    Check = fun(Sh_doc, TmpAns) ->
                        case TmpAns of
                            false ->
                                Sh_Inf = Sh_doc#db_document.record,
                                case Share_info#share_desc.share_with =:= Sh_Inf#share_desc.share_with of
                                    true -> {true, Sh_doc};
                                    _ -> false
                                end;
                            true -> TmpAns
                        end
                    end,
                    lists:foldl(Check, false, Ans);
                _ -> error
            end,
    case Found of
        {true, ExistingShare} -> {exists, ExistingShare};
        false ->
            dao_lib:apply(dao_share, save_file_share, [Share_info], 1);
        _ -> {Status, Ans}
    end.

%% get_share/1
%% ====================================================================
%% @doc Gets info about share from db.
%% @end
-spec get_share(Key :: {file, File :: uuid()} |
{user, User :: uuid()} |
{uuid, UUID :: uuid()}) -> Result when
    Result :: {ok, Share_doc} | {ErrorGeneral, ErrorDetail},
    Share_doc :: term(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
get_share({file, File}) ->
    {Status, FullName} = fslogic_path:get_full_file_name(File),
    case Status of
        ok ->
            case fslogic_objects:get_file(1, FullName, ?CLUSTER_FUSE_ID) of
                {ok, #db_document{uuid = FUuid}} ->
                    GetAns = get_share({file_uuid, FUuid}),
                    GetAns;
                Other ->
                    Other
            end;
        _ ->
            {Status, FullName}
    end;

get_share({file_uuid, File}) ->
    dao_lib:apply(dao_share, get_file_share, [{file, File}], 1);

get_share(Key) ->
    dao_lib:apply(dao_share, get_file_share, [Key], 1).

%% remove_share/1
%% ====================================================================
%% @doc Removes info about share from db.
%% @end
-spec remove_share(Key :: {file, File :: uuid()} |
{user, User :: uuid()} |
{uuid, UUID :: uuid()}) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
remove_share({file, File}) ->
    {Status, FullName} = fslogic_path:get_full_file_name(File),
    case Status of
        ok ->
            case fslogic_objects:get_file(1, FullName, ?CLUSTER_FUSE_ID) of
                {ok, #db_document{uuid = FUuid}} ->
                    dao_lib:apply(dao_share, remove_file_share, [{file, FUuid}], 1);
                Other -> Other
            end;
        _ ->
            {Status, FullName}
    end;

remove_share(Key) ->
    dao_lib:apply(dao_share, remove_file_share, [Key], 1).

%% getfilelocation/1
%% ====================================================================
%% @doc Gets file location from fslogic or from cache.
%% File can be string (path) or {uuid, UUID}.
%% @end
-spec getfilelocation(File :: term()) -> Result when
    Result :: {ok, {Helper, Id}} | {ErrorGeneral, ErrorDetail},
    Helper :: term(),
    Id :: term(),
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
getfilelocation(File) ->
    CachedLocation =
        case get(File) of
            {Location, ValidTo} ->
                {Megaseconds, Seconds, _Microseconds} = os:timestamp(),
                Time = 1000000 * Megaseconds + Seconds,
                case Time < ValidTo of
                    true -> Location;
                    false -> []
                end;
            _ -> undefined
        end,
    case CachedLocation of
        undefined ->
            {Status, TmpAns} = case File of
                                   {uuid, UUID} -> contact_fslogic(getfilelocation_uuid, UUID);
                                   _ -> contact_fslogic(#getfilelocation{file_logic_name = File})
                               end,
            case Status of
                ok ->
                    Response = TmpAns#filelocation.answer,
                    case Response of
                        ?VOK ->
                            Storage_helper_info = #storage_helper_info{name = TmpAns#filelocation.storage_helper_name, init_args = TmpAns#filelocation.storage_helper_args},
                            {Megaseconds2, Seconds2, _Microseconds2} = os:timestamp(),
                            Time2 = 1000000 * Megaseconds2 + Seconds2,
                            put(File, {{Storage_helper_info, TmpAns#filelocation.file_id}, Time2 + TmpAns#filelocation.validity}),
                            {ok, {Storage_helper_info, TmpAns#filelocation.file_id}};
                        _ -> {logical_file_system_error, Response}
                    end;
                _ -> {Status, TmpAns}
            end;
        _ ->
            ?debug("Reading file location from cache: ~p", [CachedLocation]),
            {ok, CachedLocation}
    end.

%% synchronize/3
%% ====================================================================
%% @doc Synchronize given byte range with other providers
%% @end
-spec synchronize(File :: file(), Offset :: non_neg_integer(), Size :: non_neg_integer()) ->
    ok | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
synchronize(File, Offset, Size) ->
    {Status, TmpAns} =
        case File of
            {uuid, Uuid} ->
                case logical_files_manager:get_file_full_name_by_uuid(Uuid) of %todo cache this value somehow
                    {ok, Name} ->
                        contact_fslogic(#synchronizefileblock{logical_name = Name, offset = Offset, size = Size});
                    Error_ -> Error_
                end;
            _ -> contact_fslogic(#synchronizefileblock{logical_name = File, offset = Offset, size = Size})
        end,
    case Status of
        ok ->
            case TmpAns#atom.value of
                ?VOK -> ok;
                Error -> {logical_file_system_error, Error}
            end;
        _ -> {Status, TmpAns}
    end.

%% mark_as_modified/5
%% ====================================================================
%% @doc Mark given byte range as modified, so other providers would know that they need to synchronize their data
%% @end
-spec mark_as_modified(FullFileName :: string(), FuseId :: string(), SequenceNumber :: non_neg_integer(),
    Offset :: non_neg_integer(), Size :: non_neg_integer()) -> ok | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
mark_as_modified(_, undefined, _, _, _) ->
    ok;
mark_as_modified(_, _, undefined, _, _) ->
    ok;
mark_as_modified(FullFileName, FuseId, SequenceNumber, Offset, Size) ->
    {Status, TmpAns} = contact_fslogic(#fileblockmodified{logical_name = FullFileName, fuse_id = FuseId,
        sequence_number = SequenceNumber, offset = Offset, size = Size}),
    case Status of
        ok ->
            case TmpAns#atom.value of
                ?VOK -> ok;
                Error -> {logical_file_system_error, Error}
            end;
        _ -> {Status, TmpAns}
    end.

%% mark_as_truncated/4
%% ====================================================================
%% @doc truncate given byte range in remote location, so other providers would know that they need to synchronize their data.
%% @end
-spec mark_as_truncated(FullFileName :: string(), FuseId :: string(), SequenceNumber :: non_neg_integer(),
    Size :: non_neg_integer()) ->  ok | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
mark_as_truncated(_, undefined, _, _) ->
    ok;
mark_as_truncated(_, _, undefined, _) ->
    ok;
mark_as_truncated(FullFileName, FuseId, SequenceNumber, Size) ->
    {Status, TmpAns} = contact_fslogic(#filetruncated{logical_name = FullFileName, fuse_id = FuseId,
        sequence_number = SequenceNumber, size = Size}),
    case Status of
        ok ->
            case TmpAns#atom.value of
                ?VOK -> ok;
                Error -> {logical_file_system_error, Error}
            end;
        _ -> {Status, TmpAns}
    end.


%% get_file_block_map/1
%% ====================================================================
%% @doc Gets list of available_blocks for each provider supporting space.
%% The result is a proplist [{ProviderId, BlockList}]
%% @end
-spec get_file_block_map(FullFileName :: string()) ->
    {ok, [{ProviderId :: binary(), BlockList :: [#block_range{}]}]} | {ErrorGeneral :: atom(), ErrorDetail :: term()}.
%% ====================================================================
get_file_block_map(FullFileName) ->
    {Status, TmpAns} = contact_fslogic(#getfileblockmap{logical_name = FullFileName}),
    case Status of
        ok ->
            case TmpAns#fileblockmap.answer of
                ?VOK ->
                    BlockMap = TmpAns#fileblockmap.block_map,
                    ProtobufProplist = lists:map(
                        fun(#fileblockmap_blockmapentity{provider_id = Id, ranges = Ranges}) ->
                            {Id, Ranges}
                        end, BlockMap),
                    FinalProplist = lists:map(
                        fun({Id, RangeList}) ->
                            {utils:ensure_binary(Id), [#block_range{from = From, to = To} || #fileblockmap_blockmapentity_blockrange{from = From, to = To} <- RangeList]}
                        end, ProtobufProplist),
                    {ok, FinalProplist};
                Error -> {logical_file_system_error, Error}
            end;
        _ -> {Status, TmpAns}
    end.


%% cache_size/2
%% ====================================================================
%% @doc Gets and updates size of file.
%% @end
-spec cache_size(File :: string(), BuffSize :: integer()) -> Result when
    Result :: integer().
%% ====================================================================
cache_size(File, BuffSize) ->
    OldSize =
        case get({File, size}) of
            Size when is_integer(Size) ->
                ?debug("Reading file size from cache, size: ~p", [Size]),
                Size;
            _ -> 0
        end,
    put({File, size}, OldSize + BuffSize),
    OldSize.

%% error_to_string/1
%% ====================================================================
%% @doc Translates error to text message.
%% @end
-spec error_to_string(Error :: term()) -> Result when
    Result :: string().
%% ====================================================================
error_to_string(Error) ->
    case Error of
        {logical_file_system_error, _} -> "Cannot get data from db";
        {error, timeout} -> "Conection between cluster machines error (timeout)";
        {error, worker_not_found} -> "File management module is down";
        {error, file_not_found} -> "File not found in DB";
        {error, file_exists} -> "Cannot create file - file already exists";
        {error, invalid_data} -> "DB invalid response";
        {error, share_not_found} -> "File sharing info not found in DB";
        {error, remove_file_share_error} -> "File sharing info cacnot be removed from DB";
        {error, unsupported_record} -> "Data cannot be stored in DB";
        {error, {get_file_by_uuid, _}} -> "Cannot find information about file in DB";
        {error, 'NIF_not_loaded'} -> "Data access library not loaded";
        {error, not_regular_file} -> "Cannot access to file at storage (not a regular file)";
        {wrong_unlink_return_code, _} -> "Error during file operation at storage system";
        {wrong_read_return_code, _} -> "Error during file operation at storage system";
        {wrong_write_return_code, _} -> "Error during file operation at storage system";
        {wrong_release_return_code, _} -> "Error during file operation at storage system";
        {wrong_open_return_code, _} -> "Error during file operation at storage system";
        {wrong_truncate_return_code, _} -> "Error during file operation at storage system";
        {wrong_mknod_return_code, _} -> "Error during file operation at storage system";
        {full_name_finding_error, _} -> "Error during translation of file name to DB internal form";
        {error, cannot_get_file_mode} -> "Cannot get file mode for new file/dir";
        _ -> "Unknown error"
    end.

-ifdef(TEST).
%% doUploadTest/4
%% ====================================================================
%% @doc Tests upload speed
%% @end
-spec doUploadTest(File :: string(), WriteFunNum :: integer(), Size :: integer(), Times :: integer()) -> Result when
    Result :: {BytesWritten, WriteTime},
    BytesWritten :: integer(),
    WriteTime :: integer().
%% ====================================================================
doUploadTest(File, WriteFunNum, Size, Times) ->
    Write = fun(Buf, TmpAns) ->
        write_file_chunk(File, Buf) + TmpAns
    end,

    Write2 = fun(Buf, TmpAns) ->
        write_file_chunk(File, Buf) + TmpAns
    end,

    WriteFun = case WriteFunNum of
                   1 -> Write;
                   _ -> Write2
               end,

    Bufs = generateData(Times, Size),
    ok = create(File),

    {Megaseconds, Seconds, Microseconds} = erlang:now(),
    BytesWritten = lists:foldl(WriteFun, 0, Bufs),
    {Megaseconds2, Seconds2, Microseconds2} = erlang:now(),
    WriteTime = 1000000 * 1000000 * (Megaseconds2 - Megaseconds) + 1000000 * (Seconds2 - Seconds) + Microseconds2 - Microseconds,
    {BytesWritten, WriteTime}.

%% generateData/2
%% ====================================================================
%% @doc Generates data for upload test
%% @end
-spec generateData(Size :: integer(), BufSize :: integer()) -> Result when
    Result :: list().
%% ====================================================================
generateData(1, BufSize) -> [list_to_binary(generateRandomData(BufSize))];
generateData(Size, BufSize) -> [list_to_binary(generateRandomData(BufSize)) | generateData(Size - 1, BufSize)].

%% generateRandomData/1
%% ====================================================================
%% @doc Generates list of random bytes
%% @end
-spec generateRandomData(Size :: integer()) -> Result when
    Result :: list().
%% ====================================================================
generateRandomData(1) -> [random:uniform(255)];
generateRandomData(Size) -> [random:uniform(255) | generateRandomData(Size - 1)].
-endif.

%% get_mode/1
%% ====================================================================
%% @doc Gets mode for a newly created file.
%% @end
-spec get_mode(FileName :: string()) -> Result when
    Result :: {ok, integer()} | {error, undefined}.
%% ====================================================================
get_mode(FileName) ->
    TmpAns = case string:tokens(FileName, "/") of
                 [?SPACES_BASE_DIR_NAME | _] ->
                     application:get_env(?APP_Name, new_group_file_logic_mode);
                 _ ->
                     application:get_env(?APP_Name, new_file_logic_mode)
             end,
    case TmpAns of
        undefined -> {error, undefined};
        _ -> TmpAns
    end.

%% event_production_enabled/1
%% ====================================================================
%% @doc Returns true if event of type EventName should be produced.
%% @end
-spec event_production_enabled(EventName :: string()) -> boolean().
%% ====================================================================
event_production_enabled(EventName) ->
    case ets:lookup(?LFM_EVENT_PRODUCTION_ENABLED_ETS, EventName) of
        [{_Key, _Value}] -> true;
        _ -> false
    end.

%% write_enabled/1
%% ====================================================================
%% @doc Returns true if quota for user of given dn has not been exceeded and therefore writing is enabled.
%% @end
-spec write_enabled(UserDn :: string()) -> boolean().
write_enabled(UserDn) ->
    case ets:lookup(?WRITE_DISABLED_USERS, UserDn) of
        [{_Key, _Value}] -> false;
        _ -> true
    end.

%% clear_cache/1
%% ====================================================================
%% @doc Clears caches connected with file.
%% @end
-spec clear_cache(File :: string()) -> ok.
clear_cache(File) ->
    erase(File),
    erase({File, size}),
    ok.


%% delete_special/1
%% ====================================================================
%% @doc Removes special (not regular) file from DB.
%% @end
-spec delete_special(Path :: path()) -> ok | {error | logical_file_system_error, Reason :: any()}.
%% ====================================================================
delete_special(Path) ->
    Record = #deletefile{file_logic_name = Path},
    {Status, TmpAns} = contact_fslogic(Record),
    case Status of
        ok ->
            Response = TmpAns#atom.value,
            case Response of
                ?VOK -> ok;
                _ -> {logical_file_system_error, Response}
            end;
        _ -> {Status, TmpAns}
    end.

%% ls_chunked/5
%% ====================================================================
%% @doc List the given directory, calling itself recursively if there is more to fetch.
%% The arguments are dir path, child offset, chunk size for db queries, number of childs to read and actual Reslt list
%% @end
-spec ls_chunked(Path :: string(), Offset :: integer(), ChunkSize :: integer(), HowManyChilds :: all | integer(), Result :: list()) -> Result when
    Result :: [#dir_entry{}] | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
ls_chunked(Path, Offset, ChunkSize, all, Result) ->
    case logical_files_manager:ls(Path, ChunkSize, Offset) of
        {ok, FileList} ->
            case length(FileList) of
                ChunkSize -> ls_chunked(Path, Offset + ChunkSize, ChunkSize * 10, all, Result ++ FileList);
                _ -> {ok, Result ++ FileList}
            end;
        Error -> Error
    end;
ls_chunked(_Path, _Offset, _ChunkSize, HowManyChilds, Result) when HowManyChilds =< 0 ->
    {ok, Result};
ls_chunked(Path, Offset, ChunkSize, HowManyChilds, Result) ->
    case logical_files_manager:ls(Path, min(HowManyChilds, ChunkSize), Offset) of
        {ok, FileList} ->
            case length(FileList) of
                ChunkSize ->
                    ls_chunked(Path, Offset + ChunkSize, ChunkSize * 10, HowManyChilds - ChunkSize, Result ++ FileList);
                _ -> {ok, Result ++ FileList}
            end;
        Error -> Error
    end.

%% copy_file_content/4
%% ====================================================================
%% @doc Copies file content beginning at offset, with given buffer size
%% @end
-spec copy_file_content(From :: string(), To :: string(), Offset :: integer(), BufferSize :: integer()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
copy_file_content(From, To, Offset, BufferSize) ->
    case read(From, Offset, BufferSize) of
        {ok, Data} ->
            case byte_size(Data) < BufferSize of
                true ->
                    case write_file_chunk(To, Data) of
                        Written when is_integer(Written) -> ok;
                        Error -> Error
                    end;
                false ->
                    case write_file_chunk(To, Data) of
                        Written when is_integer(Written) ->
                            copy_file_content(From, To, Offset + Written, BufferSize);
                        Error -> Error
                    end
            end;
        Error -> Error
    end.

%% copy_file_acl/2
%% ====================================================================
%% @doc Copies file access contol list
%% @end
-spec copy_file_acl(From :: string(), To :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
copy_file_acl(From, To) ->
    case get_acl(From) of
        {ok, Acl} ->
            set_acl(To, Acl);
        Error -> Error
    end.

%% copy_file_xattr/4
%% ====================================================================
%% @doc Copies file extended attributes
%% @end
-spec copy_file_xattr(From :: string(), To :: string()) -> Result when
    Result :: ok | {ErrorGeneral, ErrorDetail},
    ErrorGeneral :: atom(),
    ErrorDetail :: term().
%% ====================================================================
copy_file_xattr(From, To) ->
    case list_xattr(From) of
        {ok, XattrList} ->
            lists:foreach(fun({Key, Value}) -> set_xattr(To, Key, Value) end, XattrList),
            ok;
        Error -> Error
    end.

get_file_path_from_cache({uuid, Uuid}) ->
    case get({path_of, Uuid}) of
        undefined ->
            case logical_files_manager:get_file_full_name_by_uuid(Uuid) of
                {ok, FullFilePath} ->
                    put({path_of, Uuid}, FullFilePath),
                    {ok, FullFilePath};
                Error -> Error
            end;
        FullFilePath -> {ok, FullFilePath}
    end;
get_file_path_from_cache(FileShortName) ->
    case string:tokens(FileShortName, "/") of
        [?SPACES_BASE_DIR_NAME | _] -> {ok, FileShortName};
        _ ->
            case get({path_of, FileShortName}) of
                undefined ->
                    case fslogic_path:get_full_file_name(FileShortName) of
                        {ok, FullFileName} ->
                            put({path_of, FileShortName}, FullFileName),
                            {ok, FullFileName};
                        Error -> Error
                    end;
                FullFileName -> {ok, FullFileName}
            end
    end.
