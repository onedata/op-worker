%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This test verifies if the core cluster mechanisms such as nagios,
%%% hasing and datastore models work as expected.
%%% @end
%%%--------------------------------------------------------------------
-module(core_mechanism_test_SUITE).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").

-define(TIMEOUT, timer:minutes(5)).
-define(call_store(N, F, A), ?call(N, datastore, F, A)).
-define(call_store(N, Model, F, A), ?call(N,
    model, execute_with_default_context, [Model, F, A])).
-define(call(N, M, F, A), ?call(N, M, F, A, ?TIMEOUT)).
-define(call(N, M, F, A, T), rpc:call(N, M, F, A, T)).

%% export for ct
-export([all/0]).
-export([nagios_test/1, test_models/1]).

all() -> ?ALL([nagios_test, test_models]).

% Path to nagios endpoint
-define(HEALTHCHECK_PATH, "http://127.0.0.1:6666/nagios").
% How many retries should be performed if nagios endpoint is not responding
-define(HEALTHCHECK_RETRIES, 10).
% How often should the retries be performed
-define(HEALTHCHECK_RETRY_PERIOD, 500).

%%%===================================================================
%%% Test functions
%%%===================================================================

test_models(Config) ->
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    Models = ?call(Worker, datastore_config, models, []),

    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
%%        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(2)),
        % TODO - change to 2 seconds
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)),
        test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, datastore_pool_queue_flush_delay, 1000)
    end, Workers),

    lists:foreach(fun(ModelName) ->
%%        ct:print("Module ~p", [ModelName]),

        #model_config{store_level = SL} = MC = ?call(Worker, ModelName, model_init, []),
        Cache = case SL of
            ?GLOBALLY_CACHED_LEVEL -> true;
            ?LOCALLY_CACHED_LEVEL -> true;
            _ -> false
        end,

        Key = list_to_binary("key_tm_" ++ atom_to_list(ModelName)),
        Doc =  #document{
            key = Key,
            value = MC#model_config.defaults
        },
        ?assertMatch({ok, _}, ?call_store(Worker, ModelName, save, [Doc])),
        ?assertMatch({ok, true}, ?call_store(Worker, ModelName, exists, [Key])),

%%        ct:print("Module ok ~p", [ModelName]),

        case Cache of
            true ->
                PModule = ?call_store(Worker, driver_to_module, [persistence_driver_module]),
                ?assertMatch({ok, true}, ?call(Worker, PModule, exists, [MC, Key]), 10);
%%                ct:print("Module caching ok ~p", [ModelName]);
            _ ->
                ok
        end
    end, Models).

nagios_test(Config) ->
    [Worker1, _, _] = WorkerNodes = ?config(op_worker_nodes, Config),

    {ok, 200, _, XMLString} = rpc:call(Worker1, http_client, get, [?HEALTHCHECK_PATH, #{}, <<>>, [insecure]]),

    {Xml, _} = xmerl_scan:string(str_utils:to_list(XMLString)),

    [MainStatus] = [X#xmlAttribute.value || X <- Xml#xmlElement.attributes, X#xmlAttribute.name == status],
    % Whole app status might become out_of_sync in some marginal cases when dns or dispatcher does
    % not receive update for a long time.
    ?assertEqual(MainStatus, "ok"),

    NodeStatuses = [X || X <- Xml#xmlElement.content, X#xmlElement.name == op_worker],

    WorkersByNodeXML = lists:map(
        fun(#xmlElement{attributes = Attributes, content = Content}) ->
            [NodeName] = [X#xmlAttribute.value || X <- Attributes, X#xmlAttribute.name == name],
            WorkerNames = [X#xmlElement.name || X <- Content, X#xmlElement.name /= ?DISPATCHER_NAME, X#xmlElement.name /= ?NODE_MANAGER_NAME],
            {NodeName, WorkerNames}
        end, NodeStatuses),

    % Check if all nodes are in the report.
    lists:foreach(
        fun(Node) ->
            ?assertNotEqual(undefined, proplists:get_value(atom_to_list(Node), WorkersByNodeXML))
        end, WorkerNodes),

    % Check if all workers are in the report.
    Nodes = rpc:call(Worker1, gen_server, call, [{global, ?CLUSTER_MANAGER}, get_nodes, 1000]),
    lists:foreach(
        fun({WNode, WName}) ->
            WorkersOnNode = proplists:get_value(atom_to_list(WNode), WorkersByNodeXML),
            ?assertEqual(true, lists:member(WName, WorkersOnNode))
        end, [{Node, Worker} || Node <- Nodes, Worker <- node_manager:modules()]),

    % Check if every node's status contains dispatcher and node manager status
    lists:foreach(
        fun(#xmlElement{content = Content}) ->
            ?assertMatch([?NODE_MANAGER_NAME], [X#xmlElement.name || X <- Content, X#xmlElement.name == ?NODE_MANAGER_NAME]),
            ?assertMatch([?DISPATCHER_NAME], [X#xmlElement.name || X <- Content, X#xmlElement.name == ?DISPATCHER_NAME])
        end, NodeStatuses).