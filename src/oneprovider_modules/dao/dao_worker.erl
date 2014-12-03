%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements {@link worker_plugin_behaviour} callbacks and contains utility API methods. <br/>
%% DAO API functions are implemented in DAO sub-modules like: {@link dao_cluster}, {@link dao_vfs}. <br/>
%% All DAO API functions Should not be used directly, use {@link dao_worker:handle/2} instead.
%% Module :: atom() is module suffix (prefix is 'dao_'), MethodName :: atom() is the method name
%% and ListOfArgs :: [term()] is list of argument for the method. <br/>
%% If you want to call utility methods from this module - use Module = utils
%% See {@link dao_worker:handle/2} for more details.
%% @end
%% ===================================================================
-module(dao_worker).
-behaviour(worker_plugin_behaviour).

-include_lib("oneprovider_modules/dao/dao.hrl").
-include_lib("dao/include/couch_db.hrl").
-include_lib("oneprovider_modules/dao/dao_types.hrl").
-include_lib("oneprovider_modules/dao/dao_vfs.hrl").
-include_lib("oneprovider_modules/fslogic/fslogic.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("dao/include/common.hrl").
-include("registered_names.hrl").

-import(dao_helper, [name/1]).

-define(init_storage_after_seconds,1).

-ifdef(TEST).
-compile([export_all]).
-endif.

%% worker_plugin_behaviour callbacks
-export([init/1, handle/2, cleanup/0]).

%% Additional exports
-export([init_storage/0]).

%% For integration tests
-export([set_db/0]).

%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1
-spec init(Args :: term()) -> Result when
    Result :: ok | {error, Error},
    Error :: term().
%% ====================================================================
init({Args, {init_status, undefined}}) ->
    ets:new(db_host_store, [named_table, public, bag, {read_concurrency, true}]),
    init({Args, {init_status, table_initialized}});
init({_Args, {init_status, table_initialized}}) -> %% Final stage of initialization. ETS table was initialized
    set_db(),

    ProcFun =
        fun
            (_, {ensure_file_location_exists, FullFileName, File}) ->
                fslogic_file:ensure_file_location_exists_unsafe(FullFileName, File);
            (ProtocolVersion, {Target, Method, Args}) ->
                handle(ProtocolVersion, {Target, Method, Args})
        end,

    MapFun =
        fun
            ({ensure_file_location_exists, FullFileName, _}) ->
                lists:foldl(fun(Char, Sum) -> 10 * Sum + Char end, 0, FullFileName);
            ({_, _, [File, _]}) ->
                lists:foldl(fun(Char, Sum) -> 10 * Sum + Char end, 0, File)
        end,

    SubProcList = worker_host:generate_sub_proc_list(id_generation, 6, 10, ProcFun, MapFun),

    RequestMap = fun
      ({dao_vfs, save_new_file, _}) ->id_generation;
      ({ensure_file_location_exists, _, _}) ->id_generation; %todo move this to fslogic
      (_) -> non
    end,

    DispMapFun = fun
      ({dao_vfs, save_new_file, [File, _]}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, File);
      ({ensure_file_location_exists, FullFileName, _}) ->
            lists:foldl(fun(Char, Sum) -> 2 * Sum + Char end, 0, FullFileName);
      (_) -> non
    end,

		erlang:send_after(?init_storage_after_seconds * 1000, self(), {timer, {asynch, 1, {utils,init_storage,[]}}}),

    #initial_host_description{request_map = RequestMap, dispatcher_request_map = DispMapFun, sub_procs = SubProcList, plug_in_state = ok};
init({Args, {init_status, _TableInfo}}) ->
    init({Args, {init_status, table_initialized}});
init(Args) ->
    ClearFun = fun() -> cache_guard() end,
    ClearFun2 = fun() -> ets:delete_all_objects(storage_cache) end,
    ClearFun3 = fun() -> ets:delete_all_objects(users_cache) end,
    ClearFun4 = fun() -> ets:delete_all_objects(spaces_cache) end,
    %% TODO - check if simple cache is enough for users and fuses; if not, change to advanced cache (sub processes)
    %% We assume that cached data do not change!
    Cache1 = worker_host:create_simple_cache(dao_fuse_cache, dao_fuse_cache_loop_time, ClearFun),
    case Cache1 of
      ok ->
        Cache2 = worker_host:create_simple_cache(storage_cache, storage_cache_loop_time, ClearFun2),
        case Cache2 of
          ok ->
            Cache3 = worker_host:create_simple_cache(users_cache, users_cache_loop_time, ClearFun3),
            case Cache3 of
              ok ->
                  Cache4 = worker_host:create_simple_cache(spaces_cache, users_cache_loop_time, ClearFun4),
                  case Cache4 of
                      ok ->
                          init({Args, {init_status, ets:info(db_host_store)}});
                      _ -> throw({error, {spaces_cache_error, Cache4}})
                  end;
              _ -> throw({error, {users_cache_error, Cache3}})
            end;
          _ -> throw({error, {storage_cache_error, Cache2}})
        end;
      _ -> throw({error, {dao_fuse_cache_error, Cache1}})
    end.

%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1. <br/>
%% All {Module, Method, Args} requests (second argument), executes Method with Args in {@type dao_Module} module, but with one exception:
%% If Module = utils, then dao module will be used. <br/>
%% E.g calling dao_worker:handle(_, {vfs, some_method, [some_arg]}) will call dao_vfs:some_method(some_arg) <br/>
%% but calling dao_worker:handle(_, {utils, some_method, [some_arg]}) will call dao_worker:some_method(some_arg) <br/>
%% You can omit Module atom in order to use default module which is dao_cluster. <br/>
%% E.g calling dao_worker:handle(_, {some_method, [some_arg]}) will call dao_cluster:some_method(some_arg) <br/>
%% Additionally all exceptions from called API method will be caught and converted into {error, Exception} tuple. <br/>
%% E.g. calling handle(_, {save_record, [Id, Rec]}) will execute dao_cluster:save_record(Id, Rec) and normalize return value.
%% @end
-spec handle(ProtocolVersion :: term(), Request) -> Result when
    Request :: {Method, Args} | {Mod :: atom(), Method, Args} | ping | healthcheck | get_version,
    Method :: atom(),
    Args :: list(),
    Result :: ok | {ok, Response} | {error, Error} | pong | Version,
    Response :: term(),
    Version :: term(),
    Error :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
  pong;

handle(ProtocolVersion, healthcheck) ->
	{Status,Msg} = dao_lib:apply(dao_helper,list_dbs,[],ProtocolVersion),
	case Status of
		ok ->
			ok;
		_ ->
			?error("Healthchecking database filed with error: ~p",Msg),
			{error,db_healthcheck_failed}
	end;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(ProtocolVersion, {Target, Method, Args}) when is_atom(Target), is_atom(Method), is_list(Args) ->
    put(protocol_version, ProtocolVersion), %% Some sub-modules may need it to communicate with DAO' gen_server
    Module =
        case atom_to_list(Target) of
            "utils" -> dao_worker;
            [$d, $a, $o, $_ | T] -> list_to_atom("dao_" ++ T);
            T -> list_to_atom("dao_" ++ T)
        end,
    try apply(Module, Method, Args) of
        {error, Err} ->
            ?error("Handling ~p:~p with args ~p returned error: ~p", [Module, Method, Args, Err]),
            {error, Err};
        {ok, Response} -> {ok, Response};
        ok -> ok;
        Other ->
            ?error("Handling ~p:~p with args ~p returned unknown response: ~p", [Module, Method, Args, Other]),
            {error, Other}
    catch
        error:{badmatch, {error, Err}} -> {error, Err};
        _Type:Error ->
            ?debug_stacktrace("Handling ~p:~p with args ~p interrupted by exception: ~p:~p", [Module, Method, Args, _Type, Error]),
            {error, Error}
    end;
handle(ProtocolVersion, {Method, Args}) when is_atom(Method), is_list(Args) ->
    handle(ProtocolVersion, {cluster, Method, Args});
handle(_ProtocolVersion, _Request) ->
    ?error("Unknown request ~p (protocol ver.: ~p)", [_Request, _ProtocolVersion]),
    {error, wrong_args}.

%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
%% ====================================================================
cleanup() ->
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% cache_guard/1
%% ====================================================================
%% @doc Loops infinitly (sleeps Timeout ms on each loop) and runs fallowing predefined tasks on each loop:
%%          - FUSE session cleanup
%%      When used in newly spawned process, the process will infinitly fire up the the tasks while
%%      sleeping 'Timeout' ms between subsequent loops.
%%      NOTE: The function crashes is 'dao_fuse_cache' ETS is not available.
%% @end
-spec cache_guard() -> no_return().
%% ====================================================================
cache_guard() ->
    [_ | _] = ets:info(dao_fuse_cache),
    dao_cluster:clear_sessions(). %% Clear FUSE session

%% init_storage/0
%% ====================================================================
%% @doc Inserts storage defined during worker instalation to database (if db already has defined storage,
%% the function only replaces StorageConfigFile with that definition)
%% @end
-spec init_storage() -> ok | {error, Error :: term()}.
%% ====================================================================
init_storage() ->
    try
        %get storage config file path
        GetEnvResult = application:get_env(?APP_Name, storage_config_path),
        case GetEnvResult of
            {ok, _} -> ok;
            undefined ->
                ?error("Could not get 'storage_config_path' environment variable"),
                throw(get_env_error)
        end,
        {ok, StorageFilePath} = GetEnvResult,

        %get storage list from db
        {Status1, ListStorageValue} = dao_lib:apply(dao_vfs, list_storage, [], 1),
        case Status1 of
            ok -> ok;
            error ->
                ?error("Could not list existing storages"),
                throw(ListStorageValue)
        end,
        ActualDbStorages = [X#db_document.record || X <- ListStorageValue],

        case ActualDbStorages of
            [] -> %db empty, insert storage
                %read from file
                {Status2, FileConsultValue} = file:consult(StorageFilePath),
                case Status2 of
                    ok -> ok;
                    error ->
                        ?error("Could not read storage config file"),
                        throw(FileConsultValue)
                end,

                %parse storage preferences
                UserPreferenceToGroupInfo = fun(GroupPreference) ->
                    case GroupPreference of
                        [{name, cluster_fuse_id}, {root, Root}] ->
                            #fuse_group_info{name = ?CLUSTER_FUSE_ID, storage_helper = #storage_helper_info{name = "DirectIO", init_args = [Root]}};
                        [{name, Name}, {root, Root}] ->
                            #fuse_group_info{name = Name, storage_helper = #storage_helper_info{name = "DirectIO", init_args = [Root]}}
                    end
                end,

                InsertStorageAnswers = lists:map(fun(StoragePreferences) ->
                    FuseGroups = try
                        lists:map(UserPreferenceToGroupInfo, StoragePreferences)
                                 catch
                                     _Type:Err ->
                                         ?error("Wrong format of storage config file"),
                                         Err
                                 end,

                    %create storage
                    {Status3, Value} = apply(fslogic_storage, insert_storage, ["ClusterProxy", [], FuseGroups]),
                    case Status3 of
                        ok ->
                            ok;
                        error ->
                            ?error("Error during inserting storage to db"),
                            Value
                    end
                end, FileConsultValue),
                case lists:all(fun(InsertStorageAnswer) -> InsertStorageAnswer =:= ok end, InsertStorageAnswers) of
                    true -> ok;
                    _ -> {error, InsertStorageAnswers}
                end;

            _NotEmptyList -> %db not empty
                ok
        end
    catch
        Type:Error ->
            ?error_stacktrace("Error during storage init: ~p:~p", [Type, Error]),
            {error, Error}
    end.


%% set_db/0
%% ====================================================================
%% @doc Gets list of db nodes from env and inserts them to dao_hosts
%% @end
-spec set_db() -> ok | {error, Error :: term()}.
%% ====================================================================
set_db() ->
    case application:get_env(?APP_Name, db_nodes) of
        {ok, Nodes} when is_list(Nodes) ->
            [dao_hosts:insert(Node) || Node <- Nodes, is_atom(Node)],
            catch dao_setup:setup_views(?DATABASE_DESIGN_STRUCTURE);
        _ ->
            ?warning("There are no DB hosts given in application env variable.")
    end.