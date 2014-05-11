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
-export([strip_path_leaf/1, basename/1, get_parent_and_name_from_path/2, create_children_list/1, create_children_list/2, time/0, get_user_id_from_system/1]).
-export([verify_file_name/1, get_full_file_name/1, get_full_file_name/2, get_full_file_name/4]).
-export([get_sh_and_id/3, get_user_id/0, get_files_number/3, get_user_root/0]).
-export([get_user_file_name/1, get_user_file_name/2, create_dirs/4]).
-export([get_group_owner/1, get_new_file_id/4, check_file_perms/5, check_file_perms/4, get_user_groups/2, update_user_files_size_view/1]).


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
    {_, UserDoc} = fslogic_objects:get_user(),
    get_user_file_name(FullFileName, UserDoc).

get_user_file_name(FullFileName, UserDoc) ->
    {ok, Tokens} = verify_file_name(FullFileName),

    UserName =
        case UserDoc of
            get_user_id_error -> "root";
            _ ->
                UserRec = dao_lib:strip_wrappers(UserDoc),
                UserRec#user.login
        end,
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
                _ -> {error, invalid_group_access}
            end;
        _ ->
            case UserDoc of
                get_user_id_error -> {ok, VerifiedFileName};
                _                   -> {error, {user_doc_not_found, UserDoc}}
            end
    end.

verify_file_name(FileName) ->
    Tokens = lists:filter(fun(X) -> X =/= "." end, string:tokens(FileName, "/")),
    case lists:any(fun(X) -> X =:= ".." end, Tokens) of
        true -> {error, wrong_filename};
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




%% time/0
%% ====================================================================
%% @doc Returns time in seconds.
%% @end
-spec time() -> Result :: integer().
time() ->
    vcn_utils:time().

%% ====================================================================
%% Internal functions
%% ====================================================================

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
                get_user_id_error -> get_user_id_error;
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
    fslogic_context:get_fuse_id().

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
