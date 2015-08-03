%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for dns_worker module.
%%% @end
%%%--------------------------------------------------------------------
-module(dns_worker_test).
-author("Piotr Ociepka").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%% from load_balancing.erl
-record(dns_lb_advice, {
    nodes_and_frequency = [] :: [{Node :: term(), Frequency :: float()}],
    node_choices = [] :: [{A :: byte(), B :: byte(), C :: byte(), D :: byte()}]
}).


%%%===================================================================
%%% Test functions
%%%===================================================================

%% tests init() function
init_test() ->
    ?assertEqual(dns_worker:init(test), {ok, #{}}),
    ?assertEqual(dns_worker:init(#{}), {ok, #{}}),
    ?assertEqual(dns_worker:init(#{a => 1, b => 2}), {ok, #{a => 1, b => 2}}),
    ?assertEqual(dns_worker:init([]), {ok, #{}}),
    ?assertException(throw, unknown_initial_state, dns_worker:init([1, 2, 3])),
    ?assertException(throw, unknown_initial_state, dns_worker:init(some_trash)).

%% tests handle(ping) call
ping_test() ->
    ?assertEqual(dns_worker:handle(ping), pong).

%% tests handle(healthckeck) call when LBAdvice is undefined
healthcheck_undefined_test_() ->
    {setup,
        fun healthcheck_undefined_setup/0,
        fun healthcheck_undefined_teardown/1,
        [fun() ->
            ?assertMatch({error, no_lb_advice_received}, dns_worker:handle(healthcheck)),
            ?assertEqual(1, meck:num_calls(worker_host, state_get, ['_', lb_advice])),
            ?assertEqual(1, meck:num_calls(worker_host, state_get, ['_', last_update])),
            ?assert(meck:validate(worker_host))
        end]}.

%% tests handle(healthcheck) call when DNS is out of sync
healthcheck_outofsync_test_() ->
    {setup,
        fun healthcheck_outofsync_setup/0,
        fun healthcheck_outofsync_teardown/1,
        [fun() ->
            ?assertEqual(out_of_sync, dns_worker:handle(healthcheck)),
            ?assertEqual(1, meck:num_calls(worker_host, state_get, ['_', lb_advice])),
            ?assertEqual(1, meck:num_calls(worker_host, state_get, ['_', last_update])),
            ?assertEqual(1, meck:num_calls(application, get_env, ['_', dns_disp_out_of_sync_threshold])),
            ?assertEqual(1, meck:num_calls(timer, now_diff, '_')),
            ?assert(meck:validate(application)),
            ?assert(meck:validate(worker_host)),
            ?assert(meck:validate(timer))
        end]}.

%% tests parse_domain() function
domain_parser_test_() ->
    {setup,
        fun domain_parser_setup/0,
        fun domain_parser_teardown/1,
        [fun() ->
            % GR domain = onedata.org
            % Provider domain = prov.onedata.org
            ?assertEqual(ok, dns_worker:parse_domain("prov.onedata.org")),
            ?assertEqual(ok, dns_worker:parse_domain("alias.onedata.org")),
            ?assertEqual(ok, dns_worker:parse_domain("www.alias.onedata.org")),
            ?assertEqual(ok, dns_worker:parse_domain("www.prov.onedata.org")),
            ?assertEqual(refused, dns_worker:parse_domain("www.part.alias.onedata.org")),
            ?assertEqual(ok, dns_worker:parse_domain("www.part.prov.onedata.org")),
            ?assertEqual(nx_domain, dns_worker:parse_domain("www.part1.part2.prov.onedata.org")),
            ?assertEqual(refused, dns_worker:parse_domain("agh.edu.pl")),
            ?assertEqual(8, meck:num_calls(application, get_env, ['_', global_registry_domain])),
            ?assertEqual(8, meck:num_calls(application, get_env, ['_', provider_domain])),
            ?assert(meck:validate(application))
        end]}.

%% tests handling bad requests
bad_request_handle_test() ->
    ?assertException(throw, {unsupported_request, bad_request}, dns_worker:handle(bad_request)),
    ?assertException(throw, {unsupported_request, pong}, dns_worker:handle(pong)),
    ?assertException(throw,
        {unsupported_request, [handle_a, "domain.org"]},
        dns_worker:handle([handle_a, "domain.org"])).

%% tests unproper calls of handle({handle_a, Domain}) and handle({handle_ns, Domain})
handle_domain_test_() ->
    [
%% LBAdvice is undefined
        {setup,
            fun domain_undefined_lbadvice_setup/0,
            fun domain_teardown/1,
            [fun() ->
                ?assertEqual(serv_fail, dns_worker:handle({handle_a, "onedata.org"})),
                ?assertEqual(serv_fail, dns_worker:handle({handle_ns, "onedata.org"})),
                ?assertEqual(2, meck:num_calls(worker_host, state_get, ['_', lb_advice])),
                ?assert(meck:validate(worker_host))
            end]},
%% domain is not in onedata.org
        {setup,
            fun domain_proper_setup/0,
            fun domain_teardown/1,
            [fun() ->
                ?assertEqual(refused, dns_worker:handle({handle_a, "agh.edu.pl"})),
                ?assertEqual(refused, dns_worker:handle({handle_ns, "agh.edu.pl"})),
                ?assertEqual(2, meck:num_calls(worker_host, state_get, ['_', lb_advice])),
                ?assertEqual(2, meck:num_calls(application, get_env, ['_', global_registry_domain])),
                ?assertEqual(2, meck:num_calls(application, get_env, ['_', provider_domain])),
                ?assert(meck:validate(worker_host)),
                ?assert(meck:validate(application))
            end]},
%% empty domain prefix
        {setup,
            fun domain_proper_setup/0,
            fun domain_teardown/1,
            [fun() ->
                ?assertEqual(refused, dns_worker:handle({handle_a, "onedata.org"})),
                ?assertEqual(refused, dns_worker:handle({handle_ns, "onedata.org"})),
                ?assertEqual(2, meck:num_calls(worker_host, state_get, ['_', lb_advice])),
                ?assertEqual(2, meck:num_calls(application, get_env, ['_', global_registry_domain])),
                ?assertEqual(2, meck:num_calls(application, get_env, ['_', provider_domain])),
                ?assert(meck:validate(worker_host)),
                ?assert(meck:validate(application))
            end]},
%% multiple-part domain prefix
        {setup,
            fun domain_proper_setup/0,
            fun domain_teardown/1,
            [fun() ->
                ?assertEqual(refused, dns_worker:handle({handle_a, "cyf.kr.onedata.org"})),
                ?assertEqual(refused, dns_worker:handle({handle_ns, "cyf.kr.onedata.org"})),
                ?assertEqual(2, meck:num_calls(worker_host, state_get, ['_', lb_advice])),
                ?assertEqual(2, meck:num_calls(application, get_env, ['_', global_registry_domain])),
                ?assertEqual(2, meck:num_calls(application, get_env, ['_', provider_domain])),
                ?assert(meck:validate(worker_host)),
                ?assert(meck:validate(application))
            end]}
    ].

%% test proper call of handle({handle_a, Domain})
a_test_() ->
    {setup,
        fun domain_proper_setup/0,
        fun domain_teardown/1,
        [fun() ->
            ?assertEqual(dns_worker:handle({handle_a, "one.onedata.org"}),
                {ok,
                    [{answer, "one.onedata.org", 60, a, {127, 0, 0, 1}},
                        {answer, "one.onedata.org", 60, a, {127, 0, 0, 2}},
                        {aa, true}]}),
            ?assertEqual(1, meck:num_calls(worker_host, state_get, ['_', lb_advice])),
            ?assertEqual(1, meck:num_calls(application, get_env, ['_', global_registry_domain])),
            ?assertEqual(1, meck:num_calls(application, get_env, ['_', provider_domain])),
            ?assertEqual(1, meck:num_calls(load_balancing, choose_nodes_for_dns, ['_'])),
            ?assert(meck:validate(worker_host)),
            ?assert(meck:validate(application)),
            ?assert(meck:validate(load_balancing))
        end]}.

%% test proper call of habdle({handle_ns, Domain})
ns_test_() ->
    {setup,
        fun domain_proper_setup/0,
        fun domain_teardown/1,
        [fun() ->
            ?assertEqual(dns_worker:handle({handle_ns, "two.onedata.org"}),
                {ok,
                    [{answer, "two.onedata.org", 600, ns, "127.0.0.1"},
                        {answer, "two.onedata.org", 600, ns, "127.0.0.2"},
                        {aa, true}]}),
            ?assertEqual(1, meck:num_calls(worker_host, state_get, ['_', lb_advice])),
            ?assertEqual(1, meck:num_calls(application, get_env, ['_', global_registry_domain])),
            ?assertEqual(1, meck:num_calls(application, get_env, ['_', provider_domain])),
            ?assertEqual(1, meck:num_calls(load_balancing, choose_ns_nodes_for_dns, ['_'])),
            ?assert(meck:validate(worker_host)),
            ?assert(meck:validate(application)),
            ?assert(meck:validate(load_balancing))
        end]}.

%%%===================================================================
%%% Setup and teardown functions
%%%===================================================================

healthcheck_undefined_setup() ->
    meck:new(worker_host, [unstick]),
    meck:expect(worker_host, state_get,
        fun(_, Param) ->
            case Param of
                lb_advice -> undefined;
                last_update -> {1, 2, 3}
            end
        end).

healthcheck_undefined_teardown(_) ->
    meck:unload(worker_host).

%%%-------------------------------------------------------------------

healthcheck_outofsync_setup() ->
    meck:new(worker_host, [unstick]),
    meck:expect(worker_host, state_get,
        fun(_, Param) ->
            case Param of
                lb_advice -> some_lb;
                last_update -> {1, 2, 3}
            end
        end),

    meck:new(application, [unstick]),
    meck:expect(application, get_env, fun(_, dns_disp_out_of_sync_threshold) ->
        {ok, 10} end),

    meck:new(timer, [unstick]),
    meck:expect(timer, now_diff, fun(_Now, _LastUpdate) -> 12000 end).

healthcheck_outofsync_teardown(_) ->
    meck:unload(timer),
    meck:unload(application),
    meck:unload(worker_host).

%%%-------------------------------------------------------------------

domain_parser_setup() ->
    meck:new(application, [unstick]),
    meck:expect(application, get_env,
        fun(_, Param) ->
            case Param of
                global_registry_domain -> {ok, "onedata.org"};
                provider_domain -> {ok, "prov.onedata.org"}
            end
        end).

domain_parser_teardown(_) ->
    meck:unload(application).

%%%-------------------------------------------------------------------

domain_undefined_lbadvice_setup() ->
    meck:new(worker_host, [unstick]),
    meck:expect(worker_host, state_get, fun(_, lb_advice) -> undefined end).

domain_proper_setup() ->
    meck:new(worker_host, [unstick]),
    meck:expect(worker_host, state_get, fun(_, lb_advice) ->
        #dns_lb_advice{
            nodes_and_frequency = [{{127, 0, 0, 1}, 1.2},
                {{127, 0, 0, 2}, 0.7},
                {{8, 8, 8, 8}, 12.2}],
            node_choices = [{127, 0, 0, 1}, {127, 0, 0, 2}]
        }
    end),

    meck:new(application, [unstick]),
    meck:expect(application, get_env,
        fun(_, Param) ->
            case Param of
                global_registry_domain -> {ok, "onedata.org"};
                provider_domain -> {ok, "prov.onedata.org"};
                current_loglevel -> {ok, info};
                dns_a_response_ttl -> {ok, 60};
                dns_ns_response_ttl -> {ok, 600}
            end
        end),

    meck:new(load_balancing),
    meck:expect(load_balancing,
        choose_nodes_for_dns,
        fun(DNSAdvice) ->
            #dns_lb_advice{nodes_and_frequency = _NodesAndFreq,
                node_choices = Nodes} = DNSAdvice,
            Nodes
        end),
    meck:expect(load_balancing,
        choose_ns_nodes_for_dns,
        fun(DNSAdvice) ->
            #dns_lb_advice{nodes_and_frequency = _NodesAndFreq,
                node_choices = Nodes} = DNSAdvice,
            Nodes
        end).

domain_teardown(_) ->
    meck:unload().

%%%-------------------------------------------------------------------

-endif.