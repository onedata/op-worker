%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Provider to Zone connection tests
%%% @end
%%%-------------------------------------------------------------------
-module(zone_connection_test_SUITE).
-author("Lukasz Opiola").

-include("http/gui_paths.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    oneprovider_should_connect_to_onezone_by_default/1,
    oneprovider_should_not_connect_to_onezone_if_incompatible/1,
    oneprovider_should_fetch_registry_from_onezone_if_newer/1,
    oneprovider_should_unify_registry_on_multinode_clusters/1
]).

all() -> ?ALL([
    oneprovider_should_connect_to_onezone_by_default,
    oneprovider_should_not_connect_to_onezone_if_incompatible,
    oneprovider_should_fetch_registry_from_onezone_if_newer,
    oneprovider_should_unify_registry_on_multinode_clusters
]).

-define(ATTEMPTS, 60).

%%%===================================================================
%%% Test functions
%%%===================================================================

oneprovider_should_connect_to_onezone_by_default(_Config) ->
    % after the environment is properly setup, the connection should be established
    foreach_op_worker_node(fun(Node) ->
        ?assertMatch(true, is_connected_to_oz(Node), ?ATTEMPTS)
    end).


oneprovider_should_not_connect_to_onezone_if_incompatible(_Config) ->
    OzWorkerVersion = rpc:call(hd(oct_background:get_zone_nodes()), oz_worker, get_release_version, []),
    OpWorkerVersion = rpc:call(hd(oct_background:get_provider_nodes(krakow)), op_worker, get_release_version, []),
    oneprovider_should_not_connect_to_onezone_if_incompatible_test_base(#{
        <<"compatibility">> => #{
            <<"onezone:oneprovider">> => #{
                OzWorkerVersion => [
                    <<"17.02.6">>
                ]
            }
        }
    }),
    oneprovider_should_not_connect_to_onezone_if_incompatible_test_base(#{
        <<"compatibility">> => #{
            <<"onezone:oneprovider">> => #{
                <<"17.02.6">> => [
                    OpWorkerVersion
                ]
            }
        }
    }).


oneprovider_should_not_connect_to_onezone_if_incompatible_test_base(CompatRegistryContent) ->
    foreach_op_worker_node(fun(Node) ->
        CurrentRegistryPath = rpc:call(Node, ctool, get_env, [current_compatibility_registry_file]),
        rpc:call(Node, ctool, set_env, [compatibility_registry_mirrors, []]),
        rpc:call(Node, file, write_file, [CurrentRegistryPath, json_utils:encode(CompatRegistryContent#{
            <<"revision">> => 2099123199 % use a future revision to ensure registry is not updated
        })]),
        rpc:call(Node, compatibility, clear_registry_cache, [])
    end),

    for_random_op_worker_node(fun(Node) ->
        rpc:call(Node, gs_channel_service, force_restart_connection, [])
    end),

    timer:sleep(timer:seconds(10)),

    foreach_op_worker_node(fun(Node) ->
        ?assertMatch(false, is_connected_to_oz(Node), ?ATTEMPTS)
    end).


oneprovider_should_fetch_registry_from_onezone_if_newer(_Config) ->
    OldRevision = 2000010100,
    foreach_op_worker_node(fun(Node) ->
        CurrentRegistryPath = rpc:call(Node, ctool, get_env, [current_compatibility_registry_file]),
        DefaultRegistryPath = rpc:call(Node, ctool, get_env, [default_compatibility_registry_file]),
        OldRegistry = #{<<"revision">> => OldRevision},
        rpc:call(Node, ctool, set_env, [compatibility_registry_mirrors, []]),
        rpc:call(Node, file, write_file, [CurrentRegistryPath, json_utils:encode(OldRegistry)]),
        rpc:call(Node, file, write_file, [DefaultRegistryPath, json_utils:encode(OldRegistry)]),
        rpc:call(Node, compatibility, clear_registry_cache, [])
    end),

    NewerRevision = for_random_op_worker_node(fun(Node) ->
        rpc:call(Node, gs_channel_service, force_restart_connection, []),
        Rev = peek_current_registry_revision_on_node(Node),
        ?assertNotEqual(Rev, OldRevision),
        Rev
    end),

    foreach_op_worker_node(fun(Node) ->
        ?assertMatch(true, is_connected_to_oz(Node), ?ATTEMPTS),
        % when a new registry is fetched from Onezone, it should be propagated to all nodes
        ?assertEqual(NewerRevision, peek_current_registry_revision_on_node(Node))
    end).


oneprovider_should_unify_registry_on_multinode_clusters(_Config) ->
    % place some initial compatibility registry on all nodes
    OldRevision = 2134010100,
    foreach_op_worker_node(fun(Node) ->
        CurrentRegistryPath = rpc:call(Node, ctool, get_env, [current_compatibility_registry_file]),
        rpc:call(Node, ctool, set_env, [compatibility_registry_mirrors, []]),
        rpc:call(Node, file, write_file, [CurrentRegistryPath, json_utils:encode(#{
            <<"revision">> => OldRevision
        })]),
        rpc:call(Node, compatibility, clear_registry_cache, [])
    end),

    % place a newer default registry on one of the nodes
    NewerRevision = 2189010100,
    ChosenNode = for_random_op_worker_node(fun(Node) ->
        DefaultRegistryPath = rpc:call(Node, ctool, get_env, [default_compatibility_registry_file]),
        rpc:call(Node, file, write_file, [DefaultRegistryPath, json_utils:encode(#{<<"revision">> => NewerRevision})]),
        Node
    end),

    % other nodes should still know the old registry
    foreach_op_worker_node_excluding([ChosenNode], fun(Node) ->
        ?assertEqual(OldRevision, peek_current_registry_revision_on_node(Node))
    end),

    % trigger a query, which should cause the default registry to replace the current one
    {ok, [ChosenNodeIp]} = inet:getaddrs(?GET_HOSTNAME(ChosenNode), inet),
    {ok, ChosenNodeIpBin} = ip_utils:to_binary(ChosenNodeIp),
    URL = str_utils:format("https://~s/api/v3/oneprovider/configuration", [ChosenNodeIpBin]),
    {_, _, _, Body} = ?assertMatch({ok, 200, _, _}, http_client:get(
        URL, #{}, <<>>, [{ssl_options, [{secure, false}]}]
    )),
    ?assertMatch(#{<<"compatibilityRegistryRevision">> := NewerRevision}, json_utils:decode(Body)),

    % upon replacing, the newer registry should be propagated to other nodes
    foreach_op_worker_node_excluding([ChosenNode], fun(Node) ->
        ?assertEqual(NewerRevision, peek_current_registry_revision_on_node(Node))
    end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

foreach_op_worker_node(Callback) ->
    foreach_op_worker_node_excluding([], Callback).


foreach_op_worker_node_excluding(ExcludedNodes, Callback) ->
    Workers = oct_background:get_provider_nodes(krakow),
    lists:foreach(Callback, lists_utils:subtract(Workers, ExcludedNodes)).


for_random_op_worker_node(Callback) ->
    Workers = oct_background:get_provider_nodes(krakow),
    Callback(lists_utils:random_element(Workers)).


peek_current_registry_revision_on_node(Node) ->
    Resolver = compatibility:build_resolver([Node], []),
    {ok, Rev} = rpc:call(Node, compatibility, peek_current_registry_revision, [Resolver]),
    Rev.


is_connected_to_oz(Worker) ->
    Domain = rpc:call(Worker, oneprovider, get_domain, []),
    Url = str_utils:format_bin("https://~s~s", [Domain, ?NAGIOS_OZ_CONNECTIVITY_PATH]),
    CaCerts = rpc:call(Worker, https_listener, get_cert_chain_pems, []),
    Opts = [{ssl_options, [{cacerts, CaCerts}, {hostname, str_utils:to_binary(Domain)}]}],
    Result = case http_client:get(Url, #{}, <<>>, Opts) of
        {ok, 200, _, Body} ->
            case json_utils:decode(Body) of
                #{<<"status">> := <<"ok">>} -> true;
                #{<<"status">> := <<"error">>} -> false
            end;
        _ ->
            error
    end,
    ?assertEqual(Result, rpc:call(Worker, gs_channel_service, is_connected, [])),
    Result.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op-2nodes"
    }).


end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
