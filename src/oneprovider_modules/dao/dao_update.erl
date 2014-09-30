%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This (static only) module setups/updates database structure.
%%       This module is preloaded always prior to oneprovider update,
%%       therefore getters from this module provide most recent -
%%       most likely not yet synced with DB - declarations.
%% @end
%% ===================================================================
-module(dao_update).
-author("Rafal Slota").

-include("oneprovider_modules/dao/dao.hrl").
-include_lib("dao/include/dao_helper.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_db_structure/0, setup_views/1, get_all_views/0, update_view/1, pre_update/1, pre_reload_modules/1, remove_broken_views/0, remove_outdated_views/0]).

%% ====================================================================
%% API functions
%% ====================================================================


%% get_db_structure/0
%% ====================================================================
%% @doc Getter for newest database structure.
-spec get_db_structure() -> [DBs :: #db_info{}].
%% ====================================================================
get_db_structure() ->
    ?DATABASE_DESIGN_STRUCTURE.


%% get_all_views/0
%% ====================================================================
%% @doc Getter for newest view declarations.
-spec get_all_views() -> [Views :: #view_info{}].
%% ====================================================================
get_all_views() ->
    ?VIEW_LIST.


%% update_view/1
%% ====================================================================
%% @doc Refreshes view's index. May take very long time!
-spec update_view(View :: #view_info{}) -> [Views :: #view_info{}].
%% ====================================================================
update_view(#view_info{} = View) ->
    %% Use common update_view implementation for now
    dao_setup:update_view(View).


%% pre_update/1
%% ====================================================================
%% @doc Custom per-node DAO update step implementation. This function will be called just before DAO upgrade.
%%      Non-OK return will abort whole upgrade procedure.
-spec pre_update(Version :: {version, Major :: integer(), Minor :: integer(), Patch :: integer()}) -> ok | {ok, Data :: any()} | {error, Reason :: term()}.
%% ====================================================================
pre_update(_Version) ->
    ok.


%% pre_reload_modules/1
%% ====================================================================
%% @doc This function shall return list of modules that have to be reloaded prior to DAO upgrade.
%%      Note that DAO upgrade takes place before node-wide code reload, so listed modules have to be compatible with not-listed
%%      modules from old release.
-spec pre_reload_modules(Version :: {version, Major :: integer(), Minor :: integer(), Patch :: integer()}) -> [atom()].
%% ====================================================================
pre_reload_modules(_Version) ->
    [dao_utils].


%% remove_outdated_views/0
%% ====================================================================
%% @doc Removes old views from DB.
-spec remove_outdated_views() -> ok.
%% ====================================================================
remove_outdated_views() ->
    %% Use common remove_outdated_views/1 implementation
    dao_setup:remove_outdated_views(get_all_views()).


%% remove_broken_views/0
%% ====================================================================
%% @doc Removes broken (without map function) views from DB.
-spec remove_broken_views() -> ok.
%% ====================================================================
remove_broken_views() ->
    %% Use common remove_broken_views/1 implementation
    dao_setup:remove_broken_views(get_all_views()).


%% setup_views/1
%% ====================================================================
%% @doc Creates or updates design documents
%% @end
-spec setup_views(DesignStruct :: list()) -> ok.
%% ====================================================================
setup_views(DesignStruct) ->
    %% Use common setup_views/1 implementation
    dao_setup:setup_views(DesignStruct).

