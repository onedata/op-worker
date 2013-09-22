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
    Match = [SH || #fuse_group_info{name = FuseGroup1, storage_helper = #storage_helper_info{} = SH} <- Storage#storage_info.fuse_groups, FuseGroup == FuseGroup1],
    case Match of
        [] -> Storage#storage_info.default_storage_helper;
        [Group] -> Group;
        [Group | _] ->
            lager:warning("Thare are more then one group-specific configurations in storage ~p for group ~p", [Storage#storage_info.name, FuseGroup]),
            Group
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
  dao_lib:apply(dao_vfs, save_storage, [Storage], 1).


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
    FuseID. %% Not yet implemented