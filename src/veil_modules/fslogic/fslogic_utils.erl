%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module exports utility tools for fslogic
%% @end
%% ===================================================================
-module(fslogic_utils).

-include("registered_names.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").
-include("fuse_messages_pb.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/dao/dao_users.hrl").
-include("veil_modules/dao/dao_types.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").

%% API
-export([strip_path_leaf/1, basename/1, get_parent_and_name_from_path/2, create_children_list/1, create_children_list/2, update_meta_attr/3, time/0, get_user_id_from_system/1]).
-export([verify_file_name/1, get_full_file_name/1, get_full_file_name/2, get_full_file_name/4]).
-export([get_sh_and_id/3]).
-export([get_user_file_name/1, get_user_file_name/2]).
-export([get_group_owner/1, get_new_file_id/4, update_parent_ctime/2, check_file_perms/5, check_file_perms/4, get_user_groups/2, update_user_files_size_view/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% get_new_file_id/5
%% ====================================================================
%% @doc Returns id for a new file
%% @end
-spec get_new_file_id(File :: string(), UserDoc :: term(), SHInfo :: term(), ProtocolVersion :: integer()) -> string().
%% ====================================================================
get_new_file_id(File, UserDoc, SHInfo, ProtocolVersion) ->
    {Root1, {CountStatus, FilesCount}} =
        case {string:tokens(File, "/"), UserDoc} of
            {[?GROUPS_BASE_DIR_NAME, GroupName | _], _} -> %% Group dir context
                {"/" ++ ?GROUPS_BASE_DIR_NAME ++ "/" ++ GroupName, get_files_number(group, GroupName, ProtocolVersion)};
            {_, get_user_id_error} -> {"/", {error, get_user_id_error}}; %% Unknown context
            _ -> {"/users/" ++ get_user_root(UserDoc), get_files_number(user, UserDoc#veil_document.uuid, ProtocolVersion)}
        end,
    File_id_beg = case Root1 of
                      get_user_id_error -> "/";
                      _ ->
                          case CountStatus of
                              ok -> create_dirs(FilesCount, ?FILE_COUNTING_BASE, SHInfo, Root1 ++ "/");
                              _ -> Root1 ++ "/"
                          end
                  end,

    WithoutSpecial = lists:foldl(fun(Char, Ans) ->
        case Char > 255 of
            true ->
                Ans;
            false ->
                [Char | Ans]
        end
    end, [], lists:reverse(File)),
    File_id_beg ++ dao_helper:gen_uuid() ++ re:replace(WithoutSpecial, "/", "___", [global, {return,list}]).

get_user_file_name(FullFileName) ->
    {ok, UserDoc} = fslogic_objects:get_user(),
    get_user_file_name(FullFileName, UserDoc).

get_user_file_name(FullFileName, UserDoc) ->
    Tokens = verify_file_name(FullFileName),
    UserRec = dao_lib:strip_wrappers(UserDoc),
    UserName = UserRec#user.login,
    case Tokens of
        [UserName | UserTokens] -> "/" ++ string:join(UserTokens, "/");
        _ -> "/" ++ string:join(Tokens, "/")
    end.



%% get_sh_and_id/3
%% ====================================================================
%% @doc Returns storage hel[per info and new file id (it may be changed for Cluster Proxy).
%% @end
-spec get_sh_and_id(FuseID :: string(), Storage :: term(), File_id :: string()) -> Result when
    Result :: {SHI, NewFileId},
    SHI :: term(),
    NewFileId :: string().
%% ====================================================================
get_sh_and_id(FuseID, Storage, File_id) ->
    SHI = fslogic_storage:get_sh_for_fuse(FuseID, Storage),
    #storage_helper_info{name = SHName, init_args = _SHArgs} = SHI,
    case SHName =:= "ClusterProxy" of
        true ->
            {SHI, integer_to_list(Storage#storage_info.id) ++ ?REMOTE_HELPER_SEPARATOR ++ File_id};
        false -> {SHI, File_id}
    end.

%% get_full_file_name/1
%% ====================================================================
%% @doc Gets file's full name (user's root is added to name, but only when asking about non-group dir).
%% @end
-spec get_full_file_name(FileName :: string()) -> Result when
    Result :: {ok, FullFileName} | {error, ErrorDesc},
    FullFileName :: string(),
    ErrorDesc :: atom.
%% ====================================================================

get_full_file_name(FileName) ->
    get_full_file_name(FileName, cluster_request).

%% get_full_file_name/2
%% ====================================================================
%% @doc Gets file's full name (user's root is added to name, but only when asking about non-group dir).
%% @end
-spec get_full_file_name(FileName :: string(), Request :: atom()) -> Result when
    Result :: {ok, FullFileName} | {error, ErrorDesc},
    FullFileName :: string(),
    ErrorDesc :: atom.
%% ====================================================================

get_full_file_name(FileName, Request) ->
    {UserDocStatus, UserDoc} = fslogic_objects:get_user(),
    get_full_file_name(FileName, Request, UserDocStatus, UserDoc).

%% get_full_file_name/4
%% ====================================================================
%% @doc Gets file's full name (user's root is added to name, but only when asking about non-group dir).
%% @end
-spec get_full_file_name(FileName :: string(), Request :: atom(), UserDocStatus :: atom(), UserDoc :: tuple()) -> Result when
    Result :: {ok, FullFileName} | {error, ErrorDesc},
    FullFileName :: string(),
    ErrorDesc :: atom.
%% ====================================================================

get_full_file_name(FileName, Request, UserDocStatus, UserDoc) ->
    {ok, Tokens} = verify_file_name(FileName),
    VerifiedFileName = string:join(Tokens, "/"),
    case UserDocStatus of
        ok ->
            case assert_group_access(UserDoc, Request, VerifiedFileName) of
                ok ->
                    case Tokens of %% Map all /groups/* requests to root of the file system (i.e. dont add any prefix)
                        [?GROUPS_BASE_DIR_NAME | _] ->
                            {ok, VerifiedFileName};
                        _ ->
                            Root = get_user_root(UserDoc),
                            {ok, Root ++ "/" ++ VerifiedFileName}
                    end;
                _ -> {error, {?VEPERM, invalid_group_access}}
            end;
        _ ->
            case UserDoc of
                get_user_id_error   -> {ok, VerifiedFileName};
                _                   -> {error, {?VEPERM, {user_doc_not_found, UserDoc}}}
            end
    end.

verify_file_name(FileName) ->
    Tokens = lists:filter(fun(X) -> X =/= "." end, string:tokens(FileName, "/")),
    case lists:any(fun(X) -> X =:= ".." end, Tokens) of
        true -> {error, {?VEREMOTEIO, wrong_filename}};
        _ -> {ok, Tokens}
    end.

%% strip_path_leaf/1
%% ====================================================================
%% @doc Strips file name from path
-spec strip_path_leaf(Path :: string()) -> string().
%% ==================================================================
strip_path_leaf(Path) when is_list(Path) ->
    strip_path_leaf({split, lists:reverse(string:tokens(Path, [?PATH_SEPARATOR]))});
strip_path_leaf({split, []}) -> [?PATH_SEPARATOR];
strip_path_leaf({split, [_ | Rest]}) ->
    [?PATH_SEPARATOR] ++ string:join(lists:reverse(Rest), [?PATH_SEPARATOR]).


%% basename/1
%% ====================================================================
%% @doc Gives file basename from given path
-spec basename(Path :: string()) -> string().
%% ==================================================================
basename(Path) ->
    case lists:reverse(string:tokens(Path, [?PATH_SEPARATOR])) of
        [Leaf | _] -> Leaf;
        _ -> [?PATH_SEPARATOR]
    end.

%% get_parent_and_name_from_path/2
%% ====================================================================
%% @doc Gets parent uuid and file name on the basis of absolute path.
%% @end
-spec get_parent_and_name_from_path(Path :: string(), ProtocolVersion :: term()) -> Result when
  Result :: tuple().
%% ====================================================================

get_parent_and_name_from_path(Path, ProtocolVersion) ->
  File = fslogic_utils:basename(Path), 
  Parent = fslogic_utils:strip_path_leaf(Path),
  case Parent of
    [?PATH_SEPARATOR] -> {ok, {File, #veil_document{}}};
    _Other ->
      {Status, TmpAns} = dao_lib:apply(dao_vfs, get_file, [Parent], ProtocolVersion),
      case Status of
        ok -> {ok, {File, TmpAns}};
        _BadStatus ->
          lager:error([{mod, ?MODULE}], "Error: cannot find parent for path: ~s", [Path]),
          {error, "Error: cannot find parent: " ++ TmpAns}
      end
  end.

%% create_children_list/1
%% ====================================================================
%% @doc Creates list of children logical names on the basis of list with
%% veil_documents that describe children.
%% @end
-spec create_children_list(Files :: list()) -> Result when
  Result :: term().
%% ====================================================================

create_children_list(Files) ->
  create_children_list(Files, []).

%% create_children_list/2
%% ====================================================================
%% @doc Creates list of children logical names on the basis of list with
%% veil_documents that describe children.
%% @end
-spec create_children_list(Files :: list(), TmpAns :: list()) -> Result when
  Result :: term().
%% ====================================================================

create_children_list([], Ans) ->
  Ans;

create_children_list([File | Rest], Ans) ->
  FileDesc = File#veil_document.record,
  create_children_list(Rest, [FileDesc#file.name | Ans]).


%% update_meta_attr/3
%% ====================================================================
%% @doc Updates file_meta record associated with given #file record. <br/>
%%      Attr agument decides which field has to be updated with Value. <br/>
%%      There is one exception to this rule: if Attr == 'times', Value has to be tuple <br/>
%%      with fallowing format: {ATimeValue, MTimeValue, CTimeValue} or {ATimeValue, MTimeValue}. <br/>
%%      If there is no #file_meta record associated with given #file, #file_meta will be created and whole function call will be blocking. <br/>
%%      Otherwise the method call will be asynchronous. <br/> Returns given as argument #file record unchanged, unless #file_meta had to be created. <br/>
%%      In this case returned #file record will have #file.meta_doc field updated and shall be saved to DB after this call.
%% @end
-spec update_meta_attr(File :: #file{}, Attr, Value :: term()) -> Result :: #file{} when
    Attr :: atime | mtime | ctime | size | times.
%% ====================================================================
update_meta_attr(File, Attr, Value) ->
    update_meta_attr(File, Attr, Value, 5).


%% time/0
%% ====================================================================
%% @doc Returns time in seconds.
%% @end
-spec time() -> Result :: integer().
time() ->
    vcn_utils:time().

%% ====================================================================
%% Integrnal functions
%% ====================================================================

%% update_meta_attr/4
%% ====================================================================
%% @doc Internal implementation of update_meta_attr/3. See update_meta_attr/3 for more information.
%% @end
-spec update_meta_attr(File :: #file{}, Attr, Value :: term(), RetryCount :: integer()) -> Result :: #file{} when
    Attr :: atime | mtime | ctime | size | times.
update_meta_attr(#file{meta_doc = MetaUUID} = File, Attr, Value, RetryCount) ->
    SyncTask = fun() -> 
        case init_file_meta(File) of 
            {File1, #veil_document{record = MetaRec} = MetaDoc} -> 
                NewMeta = 
                    case Attr of 
                        times -> 
                            case Value of 
                                {ATime, MTime, CTime}                    -> MetaRec#file_meta{uid = File#file.uid, atime = ATime, mtime = MTime, ctime = CTime};
                                {ATime, MTime} when ATime > 0, MTime > 0 -> MetaRec#file_meta{uid = File#file.uid, atime = ATime, mtime = MTime};
                                {ATime, _MTime} when ATime > 0           -> MetaRec#file_meta{uid = File#file.uid, atime = ATime};
                                {_ATime, MTime} when MTime > 0           -> MetaRec#file_meta{uid = File#file.uid, mtime = MTime}
                            end;
                        ctime when Value > 0 -> MetaRec#file_meta{uid = File#file.uid, ctime = Value};
                        mtime when Value > 0 -> MetaRec#file_meta{uid = File#file.uid, mtime = Value};
                        atime when Value > 0 -> MetaRec#file_meta{uid = File#file.uid, atime = Value};
                        size when Value >= 0 ->
                          MetaRec#file_meta{uid = File#file.uid, size = Value};
                        _ ->
                            MetaRec
                    end,
                case MetaRec of 
                    NewMeta -> File1;
                    _ ->
                        NewDoc = MetaDoc#veil_document{record = NewMeta},
                        case dao_lib:apply(dao_vfs, save_file_meta, [NewDoc], 1) of 
                            {ok, _} -> File1;
                            {error, conflict} when RetryCount > 0 ->
                                lager:warning("Conflict when saveing file_meta record for file (name = ~p, parent = ~p). Retring...", [File#file.name, File#file.parent]),
                                {_, _, M} = now(),
                                timer:sleep(M rem 100), %% If case of conflict we should wait a little bit before next try (max 100ms)
                                update_meta_attr(File1, Attr, Value, RetryCount - 1);
                            {error, Error} -> 
                                lager:warning("Cannot save file_meta record for file (name = ~p, parent = ~p) due to error: ~p", [File#file.name, File#file.parent, Error])
                        end
                end;
            _Error ->
                lager:warning("Cannot init file_meta record for file (name = ~p, parent = ~p) due to previous errors", [File#file.name, File#file.parent]),
                File
        end
    end, %% SyncTask = fun()

    case MetaUUID of 
        UUID when is_list(UUID) -> %% When MetaUUID is set, run this method async
            spawn(SyncTask),
            File;
        _ ->
            SyncTask()
    end. 


%% init_file_meta/1
%% ====================================================================
%% @doc Internal implementation of update_meta_attr/3. This method handles creation of not existing #file_meta document.
%% @end
-spec init_file_meta(File :: #file{}) -> Result :: {#file{}, term()}.
%% ====================================================================
init_file_meta(#file{meta_doc = MetaUUID} = File) when is_list(MetaUUID) ->
    case dao_lib:apply(dao_vfs, get_file_meta, [MetaUUID], 1)  of
        {ok, #veil_document{} = MetaDoc} -> {File, MetaDoc};
        Error -> 
            lager:error("File (name = ~p, parent = ~p) points to file_meta (uuid = ~p) that is not available. DAO response: ~p", [File#file.name, File#file.parent, MetaUUID, Error]),
            {File, #veil_document{uuid = MetaUUID, record = #file_meta{}}}
    end;
init_file_meta(#file{} = File) ->
    case dao_lib:apply(dao_vfs, save_file_meta, [#file_meta{uid = File#file.uid}], 1) of
        {ok, MetaUUID} when is_list(MetaUUID) -> init_file_meta(File#file{meta_doc = MetaUUID});
        Error ->
            lager:error("Cannot save file_meta record for file (name = ~p, parent = ~p) due to: ~p", [File#file.name, File#file.parent, Error]),
            {File, undefined}
    end.

%% get_user_id_from_system/1
%% ====================================================================
%% @doc Returns id of user in local system.
%% @end
-spec get_user_id_from_system(User :: string()) -> string().
%% ====================================================================
get_user_id_from_system(User) ->
  os:cmd("id -u " ++ User).

%% update_user_files_size_view
%% ====================================================================
%% @doc Updates user files size view in db
%% @end
-spec update_user_files_size_view(ProtocolVersion :: term()) -> term().
%% ====================================================================
update_user_files_size_view(ProtocolVersion) ->
    case dao_lib:apply(dao_users, update_files_size, [], ProtocolVersion) of
        ok ->
            lager:info([{mod, ?MODULE}], "User files size view updated"),
            ok;
        Other ->
            lager:error([{mod, ?MODULE}], "Error during updating user files size view: ~p", [Other]),
            Other
    end.

%% get_user_root/1
%% ====================================================================
%% @doc Gets user's root directory.
%% @end
-spec get_user_root(UserDoc :: term()) -> Result when
    Result :: {ok, RootDir} | {error, ErrorDesc},
    RootDir :: string(),
    ErrorDesc :: atom.
%% ====================================================================

get_user_root(UserDoc) ->
    UserRecord = UserDoc#veil_document.record,
    "/" ++ UserRecord#user.login.


%% get_user_root/2
%% ====================================================================
%% @doc Gets user's root directory.
%% @end
-spec get_user_root(UserDocStatus :: atom(), UserDoc :: term()) -> Result when
    Result :: {ok, RootDir} | {error, ErrorDesc},
    RootDir :: string(),
    ErrorDesc :: atom.
%% ====================================================================

get_user_root(UserDocStatus, UserDoc) ->
    case UserDocStatus of
        ok ->
            {ok, get_user_root(UserDoc)};
        _ ->
            case UserDoc of
                get_user_id_error -> {error, get_user_id_error};
                _ -> {error, get_user_error}
            end
    end.

%% get_user_root/0
%% ====================================================================
%% @doc Gets user's root directory.
%% @end
-spec get_user_root() -> Result when
    Result :: {ok, RootDir} | {error, ErrorDesc},
    RootDir :: string(),
    ErrorDesc :: atom.
%% ====================================================================

get_user_root() ->
    {UserDocStatus, UserDoc} = fslogic_objects:get_user(),
    get_user_root(UserDocStatus, UserDoc).

%% get_user_groups/0
%% ====================================================================
%% @doc Gets user's group
%% @end
-spec get_user_groups(UserDocStatus :: atom(), UserDoc :: term()) -> Result when
    Result :: {ok, Groups} | {error, ErrorDesc},
    Groups :: list(),
    ErrorDesc :: atom.
%% ====================================================================

get_user_groups(UserDocStatus, UserDoc) ->
    case UserDocStatus of
        ok ->
            {ok, user_logic:get_team_names(UserDoc)};
        _ ->
            case UserDoc of
                get_user_id_error -> {error, get_user_groups_error};
                _ -> {error, get_user_error}
            end
    end.

%% get_user_id/0
%% ====================================================================
%% @doc Gets user's id.
%% @end
-spec get_user_id() -> Result when
    Result :: {ok, UserID} | {error, ErrorDesc},
    UserID :: term(),
    ErrorDesc :: atom.
%% ====================================================================
get_user_id() ->
    UserId = get(user_dn),
    case UserId of
        undefined -> {ok, ?CLUSTER_USER_ID};
        DN ->
            {GetUserAns, User} = user_logic:get_user({dn, DN}),
            case GetUserAns of
                ok ->
                    {ok, User#veil_document.uuid};
                _ -> {error, get_user_error}
            end
    end.

%% get_files_number/3
%% ====================================================================
%% @doc Returns number of user's or group's files
%% @end
-spec get_files_number(user | group, UUID :: uuid() | string(), ProtocolVersion :: integer()) -> Result when
    Result :: {ok, Sum} | {error, any()},
    Sum :: integer().
%% ====================================================================
get_files_number(Type, GroupName, ProtocolVersion) ->
    Ans = dao_lib:apply(dao_users, get_files_number, [Type, GroupName], ProtocolVersion),
    case Ans of
        {error, files_number_not_found} -> {ok, 0};
        _ -> Ans
    end.



%% create_dirs/4
%% ====================================================================
%% @doc Creates dir at storage for files (if needed). Returns the path
%% that contains created dirs.
%% @end
-spec create_dirs(Count :: integer(), CountingBase :: integer(), SHInfo :: term(), TmpAns :: string()) -> string().
%% ====================================================================
create_dirs(Count, CountingBase, SHInfo, TmpAns) ->
    case Count > CountingBase of
        true ->
            random:seed(now()),
            DirName = TmpAns ++ integer_to_list(random:uniform(CountingBase)) ++ "/",
            case storage_files_manager:mkdir(SHInfo, DirName) of
                ok -> create_dirs(Count div CountingBase, CountingBase, SHInfo, DirName);
                {error, dir_or_file_exists} -> create_dirs(Count div CountingBase, CountingBase, SHInfo, DirName);
                _ -> create_dirs(Count div CountingBase, CountingBase, SHInfo, TmpAns)
            end;
        false -> TmpAns
    end.

%% assert_group_access/1
%% ====================================================================
%% @doc Checks if operation given as parameter (one of fuse_messages) is allowed to be invoked in groups context.
%% @end
-spec assert_group_access(UserDoc :: tuple(), Request :: atom(), LogicalPath :: string()) -> ok | error.
%% ====================================================================
assert_group_access(_UserDoc, cluster_request, _LogicalPath) ->
    ok;

assert_group_access(UserDoc, Request, LogicalPath) ->
    assert_grp_access(UserDoc, Request, string:tokens(LogicalPath, "/")).

%% assert_grp_access/1
%% ====================================================================
%% @doc Checks if operation given as parameter (one of fuse_messages) is allowed to be invoked in groups context.
%% @end
-spec assert_grp_access(UserDoc :: tuple(), Request :: atom(), Path :: list()) -> ok | error.
%% ====================================================================
assert_grp_access(_UserDoc, Request, [?GROUPS_BASE_DIR_NAME]) ->
    case lists:member(Request, ?GROUPS_BASE_ALLOWED_ACTIONS) of
        false   -> error;
        true    -> ok
    end;
assert_grp_access(UserDoc, Request, [?GROUPS_BASE_DIR_NAME | Tail]) ->
    TailCheck = case Tail of
                    [_GroupName] ->
                        case lists:member(Request, ?GROUPS_ALLOWED_ACTIONS) of
                            false   -> error;
                            true    -> ok
                        end;
                    _ ->
                        ok
                end,

    case TailCheck of
        ok ->
            UserTeams = user_logic:get_team_names(UserDoc),
            [GroupName2 | _] = Tail,
            case lists:member(GroupName2, UserTeams) of
                true    -> ok;
                false   -> error
            end;
        _ ->
            TailCheck
    end;
assert_grp_access(_, _, _) ->
    ok.


%% get_group_owner/1
%% ====================================================================
%% @doc Convinience method that returns list of group name(s) that are considered as default owner of file
%%      created with given path. E.g. when path like "/groups/gname/file1" is passed, the method will
%%      return ["gname"].
%% @end
-spec get_group_owner(FileBasePath :: string()) -> [string()].
%% ====================================================================
get_group_owner(FileBasePath) ->
    case string:tokens(FileBasePath, "/") of
        [?GROUPS_BASE_DIR_NAME, GroupName | _] -> [GroupName];
        _ -> []
    end.

%% check_file_perms/4
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%% @end
-spec check_file_perms(FileName :: string(), UserDocStatus :: atom(), UserDoc :: term(), FileDoc :: term()) -> Result when
    Result :: {ok, boolean()} | {error, ErrorDetail},
    ErrorDetail :: term().
%% ====================================================================
check_file_perms(FileName, UserDocStatus, UserDoc, FileDoc) ->
    check_file_perms(FileName, UserDocStatus, UserDoc, FileDoc, perms).

%% check_file_perms/5
%% ====================================================================
%% @doc Checks if the user has permission to modify file (e,g. change owner).
%% @end
-spec check_file_perms(FileName :: string(), UserDocStatus :: atom(), UserDoc :: term(), FileDoc :: term(), CheckType :: atom()) -> Result when
    Result :: {ok, boolean()} | {error, ErrorDetail},
    ErrorDetail :: term().
%% ====================================================================
check_file_perms(FileName, UserDocStatus, UserDoc, FileDoc, CheckType) ->
    case CheckType of
        root ->
            case {UserDocStatus, UserDoc} of
                {ok, _} ->
                    {ok, false};
                {error, get_user_id_error} -> {ok, true};
                _ -> {UserDocStatus, UserDoc}
            end;
        _ ->
            case string:tokens(FileName, "/") of
                [?GROUPS_BASE_DIR_NAME | _] ->
                    FileRecord = FileDoc#veil_document.record,
                    CheckOwn = case CheckType of
                                   perms -> true;
                                   _ -> %write
                                       case FileRecord#file.perms band ?WR_GRP_PERM of
                                           0 -> true;
                                           _ -> false
                                       end
                               end,

                    case CheckOwn of
                        true ->
                            case {UserDocStatus, UserDoc} of
                                {ok, _} ->
                                    UserUuid = UserDoc#veil_document.uuid,
                                    {ok, FileRecord#file.uid =:= UserUuid};
                                {error, get_user_id_error} -> {ok, true};
                                _ -> {UserDocStatus, UserDoc}
                            end;
                        false ->
                            {ok, true}
                    end;
                _ ->
                    {ok, true}
            end
    end.

%% Updates modification time for parent of Dir
update_parent_ctime(Dir, CTime) ->
    case fslogic_utils:strip_path_leaf(Dir) of
        [?PATH_SEPARATOR] -> ok;
        ParentPath ->
            try
                gen_server:call(?Dispatcher_Name, {fslogic, 1, #veil_request{subject = get(user_id), request = {internal_call, #updatetimes{file_logic_name = ParentPath, mtime = CTime}}}})
            catch
                E1:E2 ->
                    lager:error("update_parent_ctime error: ~p:~p", [E1, E2]),
                    error
            end
    end.
