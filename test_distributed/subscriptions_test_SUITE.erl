%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% @end
%%%--------------------------------------------------------------------
-module(subscriptions_test_SUITE).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/global_definitions.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([provider_registers_test/1, user_registers_test/1, updates_push_accepted/1]).

-define(CONTENT_TYPE_HEADER, [{<<"content-type">>, <<"application/json">>}]).

all() ->
    ?ALL([provider_registers_test, user_registers_test, updates_push_accepted]).


%%%===================================================================
%%% Test functions
%%%===================================================================

updates_push_accepted(Config) ->
    WorkerNodes = [Node | _] = ?config(op_worker_nodes, Config),
    application:start(ssl2),
    hackney:start(),

    Body = <<"44">>, % todo use sane input & check state
    {ok, 204, _, _} = http_client:post(get_address(Node), ?CONTENT_TYPE_HEADER, Body, [insecure]),

    hackney:stop(),
    application:stop(ssl2),
    ok.

provider_registers_test(Config) ->
    WorkerNodes = ?config(op_worker_nodes, Config),

    verify_messages(WorkerNodes, [
        [{<<"last_seq">>, 1}, {<<"endpoint">>, <<"https://127.0.0.1/updates">>}]
    ], [], 10),
    ok.

user_registers_test(Config) ->
    WorkerNodes = [Node | _] = ?config(op_worker_nodes, Config),

    TTL = 123,
    set_client_ttl(Node, TTL),

    Auth = #auth{macaroon = macaroon:create("a", "b", "c"),
        disch_macaroons = macaroon:create("d", "e", "f")},
    save_session(<<"user1">>, Auth, active, Node),

    verify_messages(WorkerNodes, [
        {[{<<"last_seq">>, 1}, {<<"endpoint">>, <<"https://127.0.0.1/updates">>}], provider},
        {[{<<"ttl_seconds">>, TTL}, {<<"provider">>, <<"non_global_provider">>}], {user, {Auth#auth.macaroon, Auth#auth.disch_macaroons}}}
    ], [], 10),
    ok.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================
init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Nodes, oz_endpoint),
    test_utils:mock_expect(Nodes, oz_endpoint, auth_request,
        fun(_Client, _URN, _Method, _Headers, _Body, _Options) ->
            {ok, 204, [], <<>>}
        end),
    Config.

end_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Nodes, oz_endpoint).

%%%===================================================================
%%% Internal functions
%%%===================================================================


await_message(Nodes, Expected) ->
    verify_messages(Nodes, [Expected], [], 10).

verify_messages(Nodes, Expected, Forbidden, Retries) ->
    NodeContexts = lists:map(fun(Node) -> {Node, 1, Retries} end, Nodes),
    verify_messages(NodeContexts, Expected, Forbidden).
verify_messages(_, [], _) -> ok;
verify_messages(NodeContexts, Expected, Forbidden) ->
    {Messages, NewContexts} = get_messages(NodeContexts),


    AnyUnavailable = lists:member(not_present, Messages),
    case AnyUnavailable of
        true -> timer:sleep(500);
        false ->
            Depleted = lists:all(fun
                (retries_depleted) -> true;
                (_) -> false
            end, Messages),
            case Depleted of
                true -> ?assertEqual([], Expected);
                false -> ok
            end
    end,

    lists:foreach(fun(M) -> lists:foreach(fun(F) ->
        ?assertNotEqual(M, F)
    end, Forbidden) end, Messages),

    verify_messages(NewContexts, Expected -- Messages, Forbidden).

get_messages(NodeContexts) ->
    Results = lists:map(fun
        ({_, _, 0} = Ctx) -> {[retries_depleted], Ctx};
        ({Node, Number, RetriesLeft}) ->
            Messages = get_messages(Node, Number),
            case Messages of
                not_present -> {[not_present], {Node, Number, RetriesLeft - 1}};
                _ -> {Messages, {Node, Number + 1, RetriesLeft}}
            end
    end, NodeContexts),
    {Messages, Contexts} = lists:unzip(Results),
    {lists:append(Messages), Contexts}.

get_messages(Node, Number) ->
    Filter = ['_', "/subscription", '_', '_', '_', '_'],
    Body = mock_capture(Node, [Number, oz_endpoint, auth_request, Filter, 5]),
    Client = mock_capture(Node, [Number, oz_endpoint, auth_request, Filter, 1]),
    case Body of
        {badrpc, _} ->
            not_present;
        _ ->
%%            ct:print("~p ~p", [json_utils:decode(Body), Client]),
            [{json_utils:decode(Body), Client}]
    end.

mock_capture(Node, Args) ->
    rpc:call(Node, meck, capture, Args).

set_client_ttl(Node, TTL) ->
    rpc:call(Node, application, set_env, [op_worker, client_subscription_ttl_seconds, TTL]).

get_address(Node) ->
    {ok, IP} = rpc:call(Node, application, get_env, [op_worker, external_ip]),
    {ok, Port} = rpc:call(Node, application, get_env, [op_worker, rest_port]),
    "https://" ++ atom_to_list(IP) ++ ":" ++ integer_to_list(Port) ++ "/updates".


save_session(UserName, Auth, Status, Node) ->
    Identity = #identity{user_id = UserName},
    Doc = #document{value = #session{auth = Auth, identity = Identity, status = Status}},
    {ok, _} = rpc:call(Node, session, save, [Doc]).