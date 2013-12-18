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
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("files_common.hrl").

%% API
-export([get_sh_for_fuse/2, select_storage/2, insert_storage/2, insert_storage/3]).


%% ====================================================================
%% API functions
%% ====================================================================


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
    default ->  Storage#storage_info.default_storage_helper;
    _ ->
      Match = [SH || #fuse_group_info{name = FuseGroup1, storage_helper = #storage_helper_info{} = SH} <- Storage#storage_info.fuse_groups, FuseGroup == FuseGroup1],
      case Match of
        [] -> Storage#storage_info.default_storage_helper;
        [Group] -> Group;
        [Group | _] ->
          lager:warning("Thare are more then one group-specific configurations in storage ~p for group ~p", [Storage#storage_info.name, FuseGroup]),
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
  lists:nth(?RND(length(StorageList)) , StorageList);
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
  ID = lists:foldl(fun(X, L) -> erlang:max(L, X#storage_info.id)  end, 1, dao_lib:strip_wrappers(StorageList)),
  Storage = #storage_info{id = ID + 1, default_storage_helper = #storage_helper_info{name = HelperName, init_args = HelperArgs}, fuse_groups = Fuse_groups},
  DAO_Ans = dao_lib:apply(dao_vfs, save_storage, [Storage], 1),
  case DAO_Ans of
    {ok, _} ->
      SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
      Ans = storage_files_manager:mkdir(SHI, "users"),
      Ans3 = case Ans of
        ok ->
          Ans2 = storage_files_manager:chmod(SHI, "users", 8#773),
          case Ans2 of
            ok ->
              ok;
            _ ->
              lager:error("Can not change owner of users dir using storage helper ~p", [SHI#storage_helper_info.name]),
              error
          end;
        _ ->
          lager:error("Can not create users dir using storage helper ~p", [SHI#storage_helper_info.name]),
          error
      end,

      Ans4 = storage_files_manager:mkdir(SHI, "groups"),
      Ans6 = case Ans4 of
               ok ->
                 Ans5 = storage_files_manager:chmod(SHI, "groups", 8#773),
                 case Ans5 of
                   ok ->
                     ok;
                   _ ->
                     lager:error("Can not change owner of groups dir using storage helper ~p", [SHI#storage_helper_info.name]),
                     error
                 end;
               _ ->
                 lager:error("Can not create groups dir using storage helper ~p", [SHI#storage_helper_info.name]),
                 error
             end,

      case {Ans3, Ans6} of
        {ok, ok} -> DAO_Ans;
        _ -> {error, dirs_creation_error}
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
          lager:error("Cannot get storage helper for fuse: ~p, reason: ~p", [FuseID, {DAOStatus, DAOAns}]),
          default
      end
  end.