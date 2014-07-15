%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module exports storage management tools for fslogic
%% @end
%% ===================================================================
-module(fslogic_storage).

-include("registered_names.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("dao/include/common.hrl").
-include_lib("files_common.hrl").

%% API
-export([get_sh_for_fuse/2, select_storage/2, insert_storage/2, insert_storage/3, get_new_file_id/4, check_storage_on_node/2]).

-ifdef(TEST).
-export([create_dirs/4, get_relative_path/2, get_mount_points/0, exist_storage_info_in_config/2]).
-endif.

-define(MOUNTS_INFO_FILE, "/proc/mounts").
-define(STORAGE_TEST_FILE_LENGTH, 20).
-define(GEN_SERVER_TIMEOUT, 5000).

%% ====================================================================
%% API functions
%% ====================================================================

%% get_new_file_id/4
%% ====================================================================
%% @doc Returns id for a new file
%% @end
-spec get_new_file_id(File :: string(), UserDoc :: term(), SHInfo :: term(), ProtocolVersion :: integer()) -> string().
%% ====================================================================
get_new_file_id(File, UserDoc, SHInfo, ProtocolVersion) ->
  {Root1, {CountStatus, FilesCount}} =
    case {string:tokens(File, "/"), UserDoc} of
      {[?SPACES_BASE_DIR_NAME, GroupName | _], _} -> %% Group dir context
        {"/" ++ ?SPACES_BASE_DIR_NAME ++ "/" ++ GroupName, fslogic_utils:get_files_number(group, GroupName, ProtocolVersion)};
      {_, #veil_document{uuid = ?CLUSTER_USER_ID}} ->
        {"/", fslogic_utils:get_files_number(user, UserDoc#veil_document.uuid, ProtocolVersion)};
      _ ->
        {"/users/" ++ fslogic_path:get_user_root(UserDoc), fslogic_utils:get_files_number(user, UserDoc#veil_document.uuid, ProtocolVersion)}
    end,
  File_id_beg =
    case CountStatus of
      ok -> create_dirs(FilesCount, ?FILE_COUNTING_BASE, SHInfo, Root1 ++ "/");
      _ -> Root1 ++ "/"
    end,

  WithoutSpecial = lists:foldl(fun(Char, Ans) ->
    case Char > 255 of
      true ->
        Ans;
      false ->
        [Char | Ans]
    end
  end, [], lists:reverse(File)),
  File_id_beg ++ dao_helper:gen_uuid() ++ re:replace(WithoutSpecial, "/", "___", [global, {return, list}]).


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

%% get_sh_for_fuse/2
%% ====================================================================
%% @doc Returns #storage_helper_info{} record which describes storage helper that is connected with given <br/>
%% 		storage (described with #storage_info{} record). Each storage can have multiple storage helpers, <br/>
%%		that varies between FUSE groups, so that different FUSE clients (with different FUSE_ID) could select different storage helper.
%% @end
-spec get_sh_for_fuse(FuseID :: string(), Storage :: #storage_info{}) -> #storage_helper_info{}.
%% ====================================================================
get_sh_for_fuse(FuseID, Storage) ->
  FuseGroup = get_fuse_group(FuseID),
  case FuseGroup of
    default ->
      case dao_lib:apply(dao_cluster, get_fuse_session, [FuseID], 1) of
        {ok, #veil_document{record = #fuse_session{client_storage_info = ClientStorageInfo}}} ->
          case proplists:get_value(Storage#storage_info.id, ClientStorageInfo) of
            Record when is_record(Record, storage_helper_info) -> Record;
            _ -> Storage#storage_info.default_storage_helper
          end;
        _ -> Storage#storage_info.default_storage_helper
      end;
    _ ->
      Match = [SH || #fuse_group_info{name = FuseGroup1, storage_helper = #storage_helper_info{} = SH} <- Storage#storage_info.fuse_groups, FuseGroup == FuseGroup1],
      case Match of
        [] -> Storage#storage_info.default_storage_helper;
        [Group] -> Group;
        [Group | _] ->
          ?warning("Thare are more then one group-specific configurations in storage ~p for group ~p", [Storage#storage_info.name, FuseGroup]),
          Group
      end
  end.


%% select_storage/2
%% ====================================================================
%% @doc Chooses and returns one storage_info from given list of #storage_info records. <br/>
%%      TODO: This method is an mock method that shall be replaced in future. <br/>
%%      Currently returns random #storage_info{}.
%% @end
-spec select_storage(FuseID :: string(), StorageList :: [#storage_info{}]) -> #storage_info{}.
%% ====================================================================
select_storage(_FuseID, []) ->
  {error, no_storage};
select_storage(_FuseID, StorageList) when is_list(StorageList) ->
  ?SEED,
  lists:nth(?RND(length(StorageList)), StorageList);
select_storage(_, _) ->
  {error, wrong_storage_format}.


%% insert_storage/2
%% ====================================================================
%% @doc Creates new mock-storage info in DB that uses default storage helper with name HelperName and argument list HelperArgs.
%% @end
-spec insert_storage(HelperName :: string(), HelperArgs :: [string()]) -> term().
%% ====================================================================
insert_storage(HelperName, HelperArgs) ->
  insert_storage(HelperName, HelperArgs, []).


%% insert_storage/3
%% ====================================================================
%% @doc Creates new mock-storage info in DB that uses default storage helper with name HelperName and argument list HelperArgs.
%%      TODO: This is mock method and should be replaced by GUI-tool form control_panel module.
%% @end
-spec insert_storage(HelperName :: string(), HelperArgs :: [string()], Fuse_groups :: list()) -> term().
%% ====================================================================
insert_storage(HelperName, HelperArgs, Fuse_groups) ->
  {ok, StorageList} = dao_lib:apply(dao_vfs, list_storage, [], 1),
  ID = lists:foldl(fun(X, L) -> erlang:max(L, X#storage_info.id) end, 0, dao_lib:strip_wrappers(StorageList)),
  Storage = #storage_info{id = ID + 1, default_storage_helper = #storage_helper_info{name = HelperName, init_args = HelperArgs}, fuse_groups = Fuse_groups},
  DAO_Ans = dao_lib:apply(dao_vfs, save_storage, [Storage], 1),
  case DAO_Ans of
    {ok, _} ->
      SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
      #storage_helper_info{init_args = [Root]} = SHI,
      case add_storage_info_to_config(ID + 1, Root) of
        ok -> ok;
        _ -> ?error("Can not add storage info (~p, ~p) to config file.", [ID + 1, Root])
      end,
      Ans = storage_files_manager:mkdir(SHI, "users"),
      Ans3 = case Ans of
               ok ->
                 Ans2 = storage_files_manager:chmod(SHI, "users", 8#711),
                 case Ans2 of
                   ok ->
                     ok;
                   _ ->
                     ?error("Can not change owner of users dir using storage helper ~p", [SHI#storage_helper_info.name]),
                     Ans2
                 end;
               _ ->
                 ?error("Can not create users dir using storage helper ~p", [SHI#storage_helper_info.name]),
                 Ans
             end,

      Ans4 = storage_files_manager:mkdir(SHI, "groups"),
      Ans6 = case Ans4 of
               ok ->
                 Ans5 = storage_files_manager:chmod(SHI, "groups", 8#711),
                 case Ans5 of
                   ok ->
                     ok;
                   _ ->
                     ?error("Can not change owner of groups dir using storage helper ~p", [SHI#storage_helper_info.name]),
                     Ans5
                 end;
               _ ->
                 ?error("Can not create groups dir using storage helper ~p", [SHI#storage_helper_info.name]),
                 Ans4
             end,

      case {Ans3, Ans6} of
        {ok, ok} ->
          case add_dirs_for_existing_users(Storage) of
            ok -> DAO_Ans;
            Error ->
              ?error("Can not create dirs for existing users and theirs teams, error: ~p", [Error]),
              {error, users_dirs_creation_error}
          end;
        _ ->
          ?error("Dirs creation error: {users_dir_status, groups_dir_status} = ~p", [{Ans3, Ans6}]),
          {error, dirs_creation_error}
      end;
    _ -> DAO_Ans
  end.


%% ====================================================================
%% Internal functions
%% ====================================================================


%% get_fuse_group/1
%% ====================================================================
%% @doc Translates FUSE ID to its Group ID. <br/>
%%      TODO: Currently not implemented -> Each FUSE belongs to group with GROUP_ID equal to its FUSE_ID.
%% @end
-spec get_fuse_group(FuseID :: string()) -> GroupID :: string().
%% ====================================================================
get_fuse_group(FuseID) ->
  case FuseID of
    ?CLUSTER_FUSE_ID -> ?CLUSTER_FUSE_ID;
    _ ->
      {DAOStatus, DAOAns} = dao_lib:apply(dao_cluster, get_fuse_session, [FuseID], 1),
      case DAOStatus of
        ok ->
          Record = DAOAns#veil_document.record,
          proplists:get_value(group_id, Record#fuse_session.env_vars, default);
        _ ->
          ?error("Cannot get storage helper for fuse: ~p, reason: ~p", [FuseID, {DAOStatus, DAOAns}]),
          default
      end
  end.

%% add_dirs_for_existing_users/1
%% ====================================================================
%% @doc Adds root dirs for all existing users and theirs teams to given storage
-spec add_dirs_for_existing_users(Storage :: #storage_info{}) -> Result when
  Result :: ok | {error, Error :: atom()}.
%% ====================================================================
add_dirs_for_existing_users(Storage) ->
  case user_logic:list_all_users() of
    {ok, Users} ->
      LoginsAndTeams = lists:map(fun(X) -> {user_logic:get_login(X), user_logic:get_team_names(X)} end, Users),
      CreateDirs =
        fun({Login, Teams}, TmpAns) ->
          case user_logic:create_dirs_at_storage(Login, Teams, Storage) of
            ok ->
              TmpAns;
            Error ->
              ?error("Can not create dirs for user ~s, error: ~p", [Login, Error]),
              Error
          end
        end,
      lists:foldl(CreateDirs, ok, LoginsAndTeams);
    {error, Error} ->
      ?error("Can not list all users, error: ~p", [Error]),
      {error, Error}
  end.


%% add_storage_info_to_config/2
%% ====================================================================
%% @doc Adds tuple {StorageId, Relative path against storage mount point}
%% to config file vfs_storage.info, which is later used by client during
%% automatic storage detection.
-spec add_storage_info_to_config(StorageId :: integer(), Path :: string()) -> ok | error.
%% ====================================================================
add_storage_info_to_config(StorageId, Path) ->
  try
    ok = check_storage_on_nodes(Path),
    {ok, MountPoints} = get_mount_points(),
    {ok, MountPoint, RelativePath} = get_best_matching_mount_point(Path, MountPoints),
    ok = save_storage_info_in_config(MountPoint, StorageId, RelativePath),
    ok
  catch
    _:_ -> error
  end.


%% random_ascii_lowercase_sequence/1
%% ====================================================================
%% @doc Creates random sequence of given length, consisting of
%% lowercase ASCII letters.
-spec random_ascii_lowercase_sequence(Length :: integer()) -> Sequence :: string().
%% ====================================================================
random_ascii_lowercase_sequence(Length) ->
  lists:foldl(fun(_, Acc) -> [random:uniform(26) + 96 | Acc] end, [], lists:seq(1, Length)).


%% create_storage_test_file/1
%% ====================================================================
%% @doc Creates storage test file with random name at given path.
%% If selected name exists new one is generated. If successful function
%% returns absolute path to generated file and its content.
-spec create_storage_test_file(Path :: string()) -> Result when
  Result :: {ok, FilePath :: string(), Content :: string} | error.
%% ====================================================================
create_storage_test_file(Path) ->
  case create_storage_test_file(Path, 20) of
    {ok, FilePath, Content} -> {ok, FilePath, Content};
    _ -> error
  end.


%% create_storage_test_file/2
%% ====================================================================
%% @doc Creates storage test file with random name at given path.
%% If selected name exists new one is generated. If successful returns
%% absolute path to generated file and its content. After specified number
%% of unsuccessful attempts to create test file it returns error.
%% Should not be used directly, use create_storage_test_file/2 instead.
-spec create_storage_test_file(Path :: string(), Attempts :: integer()) -> Result when
  Result :: {ok, FilePath :: string(), Content :: string} | {error, attempts_limit_exceeded}.
%% ====================================================================
create_storage_test_file(_, 0) ->
  {error, attempts_limit_exceeded};
create_storage_test_file(Path, Attempts) ->
  {A, B, C} = now(),
  random:seed(A, B, C),
  Filename = random_ascii_lowercase_sequence(8),
  FilePath = filename:join([Path, ?STORAGE_TEST_FILE_PREFIX ++ Filename]),
  try
    {ok, Fd} = file:open(FilePath, [write, exclusive]),
    Content = random_ascii_lowercase_sequence(?STORAGE_TEST_FILE_LENGTH),
    ok = file:write(Fd, Content),
    ok = file:close(Fd),
    {ok, FilePath, Content}
  catch
    _:_ -> create_storage_test_file(Path, Attempts - 1)
  end.


%% delete_storage_test_file/1
%% ====================================================================
%% @doc Deletes storage test file.
-spec delete_storage_test_file(FilePath :: string()) -> Result when
  Result :: ok | {error, Error :: term()}.
%% ====================================================================
delete_storage_test_file(FilePath) ->
  case file:delete(FilePath) of
    ok -> ok;
    {error, Error} ->
      ?error("Error while deleting storage test file: ~p", [Error]),
      {error, Error}
  end.


%% check_storage_on_nodes/1
%% ====================================================================
%% @doc Checks storage availability on all nodes. Returns 'ok' if storage is
%% available on all nodes or first node for which storage is not available.
%% @end
-spec check_storage_on_nodes(Path :: string()) -> Result when
  Result :: ok | {error, Node :: node()}.
%% ====================================================================
check_storage_on_nodes(Path) ->
  case create_storage_test_file(Path) of
    {ok, FilePath, Content} ->
      try
        {ok, NextContent} = check_storage_on_node(FilePath, Content),
        Nodes = gen_server:call({global, ?CCM}, get_nodes, ?GEN_SERVER_TIMEOUT),
        Result = lists:foldl(fun
          (Node, {ok, NewContent}) ->
            gen_server:call({?Node_Manager_Name, Node}, {check_storage, FilePath, NewContent}, ?GEN_SERVER_TIMEOUT);
          (_, {error, Node}) -> {error, Node};
          (Node, _) -> {error, Node}
        end, {ok, NextContent}, Nodes),
        delete_storage_test_file(FilePath),
        case Result of
          {ok, _} -> ok;
          _ -> Result
        end
      catch
        _:_ ->
          delete_storage_test_file(FilePath),
          {error, node()}
      end;
    _ -> {error, node()}
  end.


%% check_storage_on_node/2
%% ====================================================================
%% @doc Checks storage availability on node. If storage is available on node,
%% new content is written to test file and returned by function. If storage is
%% not available function returns error and node name.
%% @end
-spec check_storage_on_node(FilePath :: string(), Content :: string()) -> Result when
  Result :: {ok, NewContent :: string()} | {error, Node :: node()}.
%% ====================================================================
check_storage_on_node(FilePath, Content) ->
  try
    {ok, FdRead} = file:open(FilePath, [read]),
    {ok, Content} = file:read_line(FdRead),
    ok = file:close(FdRead),
    {ok, FdWrite} = file:open(FilePath, [write]),
    NewContent = random_ascii_lowercase_sequence(?STORAGE_TEST_FILE_LENGTH),
    ok = file:write(FdWrite, NewContent),
    ok = file:close(FdWrite),
    {ok, NewContent}
  catch
    _:_ -> {error, node()}
  end.


%% save_storage_info_in_config/3
%% ====================================================================
%% @doc Saves storage info in vfs_storage.info config file at 'MountPoint'
%% path. If storage info already exists in config file it does nothing.
-spec save_storage_info_in_config(MountPoint :: string(), StorageId :: integer(), RelativePath :: string()) -> ok | error.
%% ====================================================================
save_storage_info_in_config(MountPoint, StorageId, RelativePath) ->
  try
    {ok, Config} = application:get_env(?APP_Name, storage_config_name),
    Path = filename:join([MountPoint, Config]),
    StorageInfo = "{" ++ integer_to_list(StorageId) ++ ", " ++ RelativePath ++ "}\n",
    case exist_storage_info_in_config({path, Path}, StorageInfo) of
      false ->
        {ok, Fd} = file:open(Path, [append]),
        ok = file:write(Fd, StorageInfo),
        ok = file:close(Fd),
        ok;
      _ ->
        ?info("Storage info exists in config file: ~p", [Path]),
        ok
    end
  catch
    _:_ ->
      ?error("Can not save storage info (~p, ~p) in config file."),
      error
  end.


%% exist_storage_info_in_config/2
%% ====================================================================
%% @doc Checks whether storage info exists in vfs_storage.info config file located
%% at 'Path' directory or described by file descriptor 'Fd'.
-spec exist_storage_info_in_config({path, Path :: string()} | {fd, Fd :: pid()}, StorageInfo :: string()) -> true | false.
%% ====================================================================
exist_storage_info_in_config({path, Path}, StorageInfo) ->
  try
    {ok, Fd} = file:open(Path, [read]),
    Result = exist_storage_info_in_config({fd, Fd}, StorageInfo),
    ok = file:close(Fd),
    Result
  catch
    _:_ -> false
  end;
exist_storage_info_in_config({fd, Fd}, StorageInfo) ->
  case file:read_line(Fd) of
    {ok, StorageInfo} -> true;
    {ok, _} -> exist_storage_info_in_config({fd, Fd}, StorageInfo);
    _ -> false
  end.


%% get_relative_path/2
%% ====================================================================
%% @doc Returns relative path against base path.
-spec get_relative_path(BasePath :: string(), Path :: string()) -> {ok, RelativePath :: string()} | error.
%% ====================================================================
get_relative_path(BasePath, Path) ->
  Length = length(BasePath),
  case string:left(Path, Length) of
    BasePath ->
      case string:substr(Path, Length + 1) of
        "" -> {ok, "."};
        "/" -> {ok, "."};
        "/" ++ RelativePath -> {ok, filename:join([RelativePath])};
        _ -> error
      end;
    _ -> error
  end.


%% get_mount_points/0
%% ====================================================================
%% @doc Returns list of mount points read from /proc/mounts.
-spec get_mount_points() -> {ok, MountPoints :: [string()]} | error.
%% ====================================================================
get_mount_points() ->
  try
    {ok, Fd} = file:open(?MOUNTS_INFO_FILE, [read]),
    MountPoints = get_mount_points(Fd, []),
    ok = file:close(Fd),
    {ok, MountPoints}
  catch
    _:_ ->
      ?error("Can not get system mount points info."),
      error
  end.


%% get_mount_points/2
%% ====================================================================
%% @doc Helper function. Returns list of mount points read from /proc/mounts.
%% Should not be used directly, use get_mount_points/0 instead.
-spec get_mount_points(Fd :: pid(), MountPoints :: [string()]) -> MountPoints :: [string()].
%% ====================================================================
get_mount_points(Fd, MountPoints) ->
  case file:read_line(Fd) of
    {ok, Line} ->
      [_, MountPoint | _] = string:tokens(Line, " "),
      get_mount_points(Fd, [MountPoint | MountPoints]);
    eof -> MountPoints;
    {error, Reason} -> throw(Reason)
  end.


%% get_best_matching_mount_point/2
%% ====================================================================
%% @doc Returns best matching mount point selected from list of mountpoints
%% and corresponding relative path against returned mount point for given path.
%% Term 'best matching mount point' means longest mount point that is a prefix
%% of a given path and is available on all nodes. If none of mount points match
%% for a path, path itself is returned and relative path equals '.'.
-spec get_best_matching_mount_point(Path :: string(), MountPoints :: [string()]) -> Result when
  Result :: {ok, MountPoint :: string(), RelativePath :: string()}.
%% ====================================================================
get_best_matching_mount_point(Path, MountPoints) ->
  Cmp = fun(MountPointA, MountPointB) -> length(MountPointA) < length(MountPointB) end,
  get_best_matching_mount_point(Path, lists:sort(Cmp, MountPoints), undefined).


%% get_best_matching_mount_point/3
%% ====================================================================
%% @doc Helper function for get_best_matching_mount_point/2. Should not be used directly.
-spec get_best_matching_mount_point(Path :: string(), MountPoints :: [string()], BestMatch) -> Result when
  Result :: {ok, MountPoint, RelativePath},
  BestMatch :: {MountPoint, RelativePath},
  MountPoint :: string(),
  RelativePath :: string().
%% ====================================================================
get_best_matching_mount_point(Path, [], undefined) ->
  ?info("Can not find matching mount point for storage path: ~p.", [Path]),
  {ok, Path, "."};
get_best_matching_mount_point(_, [], {MountPoint, RelativePath}) ->
  {ok, MountPoint, RelativePath};
get_best_matching_mount_point(Path, [MountPoint | MountPoints], BestMatch) ->
  try
    {ok, RelativePath} = get_relative_path(MountPoint, Path),
    ok = check_storage_on_nodes(MountPoint),
    get_best_matching_mount_point(Path, MountPoints, {MountPoint, RelativePath})
  catch
    _:_ -> get_best_matching_mount_point(Path, MountPoints, BestMatch)
  end.
