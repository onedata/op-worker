%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This (static only) module setups/updates database structure.
%%       This module is preloaded always prior to VeilCluster update,
%%       therefore getters from this module provide most recent -
%%       most likely not yet synced with DB - declarations.
%% @end
%% ===================================================================
-module(dao_update).
-author("Rafal Slota").

-include("veil_modules/dao/dao.hrl").
-include("veil_modules/dao/dao_helper.hrl").
-include("logging.hrl").

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
update_view(#view_info{name = ViewName, version = ViewVersion, db_name = DbName} = _View) ->
    case dao_helper:query_view(DbName, dao_utils:get_versioned_view_name(ViewName, ViewVersion), dao_utils:get_versioned_view_name(ViewName, ViewVersion), #view_query_args{view_type = reduce}) of
        {ok, _} -> ok;
        {error, _} ->
            case dao_helper:query_view(DbName, dao_utils:get_versioned_view_name(ViewName, ViewVersion), dao_utils:get_versioned_view_name(ViewName, ViewVersion), #view_query_args{view_type = map, limit = 1}) of
                {ok, _} -> ok;
                {error, Reason} ->
                    {error, Reason}
            end
    end.


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
    lists:foreach(
        fun(#view_info{db_name = DB, version = Vsn, name = ViewName}) ->
            [dao_helper:delete_doc(DB, ?DESIGN_DOC_PREFIX ++ dao_utils:get_versioned_view_name(ViewName, OldVsn)) || OldVsn <- lists:seq(0, Vsn - 1)]
        end, get_all_views()),
    ok.


%% remove_broken_views/0
%% ====================================================================
%% @doc Removes broken (without map function) views from DB.
-spec remove_broken_views() -> ok.
%% ====================================================================
remove_broken_views() ->
    Designs = [{DbName, dao_utils:get_versioned_view_name(VName, Vsn)} || #view_info{db_name = DbName, name = VName, version = Vsn} <- get_all_views()],
    Designs1 = lists:usort(Designs),
    lists:foreach(
        fun({DB, Design}) ->
            case dao_helper:open_design_doc(DB, Design) of
                {ok, #doc{body = Body} = Doc} ->
                    ViewsField = dao_json:get_field(Body, "views"),
                    Views = dao_json:get_fields(ViewsField),
                    EmptyString = fun(Str) when is_binary(Str) -> binary_to_list(Str); %% Helper function converting non-string value to empty string
                        (_) -> "" end,
                    ToDel =
                        lists:map(
                            fun({ViewName, View}) ->
                                MapFun = EmptyString(dao_json:get_field(View, "map")),
                                case MapFun of
                                    "" ->
                                        ViewName;
                                    _ -> ""
                                end
                            end, Views),
                    NewViews = dao_json:rm_fields(ViewsField, ToDel),
                    NewBody1 = dao_json:mk_field(Body, "views", NewViews),
                    DbRes = dao_helper:insert_doc(DB, Doc#doc{body = NewBody1}, [?ADMIN_USER_CTX]),
                    ?info("DBRes: ~p", [DbRes]),
                    ok;
                _ ->
                    ok
            end
        end, Designs1),
    ok.


%% setup_views/1
%% ====================================================================
%% @doc Creates or updates design documents
%% @end
-spec setup_views(DesignStruct :: list()) -> ok.
%% ====================================================================
setup_views(DesignStruct) ->
    DesignFun = fun(#design_info{name = Name, views = ViewList}, DbName) ->  %% For each design document
        LastCTX = %% Calculate MD5 sum of current views (read from files)
        lists:foldl(fun(#view_info{name = ViewName, version = ViewVersion}, CTX) ->
            crypto:hash_update(CTX, load_view_def(ViewName, ViewVersion, map) ++ load_view_def(ViewName, ViewVersion, reduce))
        end, crypto:hash_init(md5), ViewList),

        LocalVersion = dao_helper:name(integer_to_list(binary:decode_unsigned(crypto:hash_final(LastCTX)), 16)),
        NewViewList =
            case dao_helper:open_design_doc(DbName, Name) of
                {ok, #doc{body = Body}} -> %% Design document exists, so lets calculate MD5 sum of its views
                    ViewsField = dao_json:get_field(Body, "views"),
                    DbViews = [ dao_json:get_field(ViewsField, dao_utils:get_versioned_view_name(ViewName, ViewVersion)) || #view_info{name = ViewName, version = ViewVersion} <- ViewList ],
                    EmptyString = fun(Str) when is_binary(Str) -> binary_to_list(Str); %% Helper function converting non-string value to empty string
                        (_) -> "" end,
                    VStrings = [ EmptyString(dao_json:get_field(V, "map")) ++ EmptyString(dao_json:get_field(V, "reduce")) || {L}=V <- DbViews, is_list(L)],
                    LastCTX1 = lists:foldl(fun(VStr, CTX) -> crypto:hash_update(CTX, VStr) end, crypto:hash_init(md5), VStrings),
                    DbVersion = dao_helper:name(integer_to_list(binary:decode_unsigned(crypto:hash_final(LastCTX1)), 16)),
                    case DbVersion of %% Compare DbVersion with LocalVersion
                        LocalVersion ->
                            ?info("DB version of design ~p is ~p and matches local version. Design is up to date", [Name, LocalVersion]),
                            [];
                        _Other ->
                            ?info("DB version of design ~p is ~p and does not match ~p. Rebuilding design document", [Name, _Other, LocalVersion]),
                            ViewList
                    end;
                _ ->
                    ?info("Design document ~p in DB ~p not exists. Creating...", [Name, DbName]),
                    ViewList
            end,

        lists:map(fun(#view_info{name = ViewName, version = ViewVersion}) -> %% For each view
            case dao_helper:create_view(DbName, Name, dao_utils:get_versioned_view_name(ViewName, ViewVersion), load_view_def(ViewName, ViewVersion, map), load_view_def(ViewName, ViewVersion, reduce), LocalVersion) of
                ok ->
                    ?info("View ~p in design ~p, DB ~p has been created.", [ViewName, Name, DbName]);
                _Err ->
                    ?error("View ~p in design ~p, DB ~p creation failed. Error: ~p", [ViewName, Name, DbName, _Err])
            end
        end, NewViewList),
        DbName
    end,

    DbFun = fun(#db_info{name = Name, designs = Designs}) -> %% For each database
        dao_helper:create_db(Name, []),
        lists:foldl(DesignFun, Name, Designs)
    end,

    lists:map(DbFun, DesignStruct),
    ok.




%% ====================================================================
%% Internal functions
%% ====================================================================

%% load_view_def/3
%% ====================================================================
%% @doc Loads view definition from file.
%% @end
-spec load_view_def(Name :: string(), Version :: integer(), Type :: map | reduce) -> string().
%% ====================================================================
load_view_def(Name, Version, Type) ->
    case file:read_file(?VIEW_DEF_LOCATION ++ dao_utils:get_versioned_view_name(Name, Version) ++ (case Type of map -> ?MAP_DEF_SUFFIX; reduce -> ?REDUCE_DEF_SUFFIX end)) of
        {ok, Data} -> binary_to_list(Data);
        _ -> ""
    end.

