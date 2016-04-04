%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for dns_worker_plugin_plugin module.
%%% @end
%%%--------------------------------------------------------------------
-module(dns_worker_plugin_test).
-author("Michal Zmuda").
-author("Piotr Ociepka").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%% from load_balancing.erl
-record(dns_lb_advice, {
    nodes_and_frequency = [] :: [{Node :: term(), Frequency :: float()}],
    node_choices = [] :: [{A :: byte(), B :: byte(), C :: byte(), D :: byte()}]
}).

-define(LB, #dns_lb_advice{
    nodes_and_frequency = [{{127, 0, 0, 1}, 1.2}, {{127, 0, 0, 2}, 0.7}, {{8, 8, 8, 8}, 12.2}],
    node_choices = [{127, 0, 0, 1}, {127, 0, 0, 2}]
}).

%%%===================================================================
%%% Test functions
%%%===================================================================

%% tests parse_domain() function
domain_parser_test_() ->
    {setup,
        fun domain_parser_setup/0,
        fun domain_parser_teardown/1,
        [fun() ->
            % GR domain = onedata.org
            % Provider domain = prov.onedata.org
            ?assertEqual(ok, dns_worker_plugin:parse_domain("prov.onedata.org")),
            ?assertEqual(ok, dns_worker_plugin:parse_domain("alias.onedata.org")),
            ?assertEqual(ok, dns_worker_plugin:parse_domain("www.alias.onedata.org")),
            ?assertEqual(ok, dns_worker_plugin:parse_domain("www.prov.onedata.org")),
            ?assertEqual(refused, dns_worker_plugin:parse_domain("www.part.alias.onedata.org")),
            ?assertEqual(ok, dns_worker_plugin:parse_domain("www.part.prov.onedata.org")),
            ?assertEqual(nx_domain, dns_worker_plugin:parse_domain("www.part1.part2.prov.onedata.org")),
            ?assertEqual(refused, dns_worker_plugin:parse_domain("agh.edu.pl")),
            ?assertEqual(8, meck:num_calls(application, get_env, ['_', oz_domain])),
            ?assertEqual(8, meck:num_calls(application, get_env, ['_', provider_domain])),
            ?assert(meck:validate(application))
        end]}.


%% tests improper calls of handle({handle_a, Domain}) and handle({handle_ns, Domain})
handle_domain_test_() ->
    [
%% domain is not in onedata.org
        {setup,
            fun domain_proper_setup/0,
            fun domain_teardown/1,
            [fun() ->
                ?assertEqual(refused, dns_worker_plugin:resolve(handle_a, "agh.edu.pl", ?LB)),
                ?assertEqual(refused, dns_worker_plugin:resolve(handle_ns, "agh.edu.pl", ?LB)),
                ?assert(meck:validate(application))
            end]},
%% empty domain prefix
        {setup,
            fun domain_proper_setup/0,
            fun domain_teardown/1,
            [fun() ->
                ?assertEqual(refused, dns_worker_plugin:resolve(handle_a, "onedata.org", ?LB)),
                ?assertEqual(refused, dns_worker_plugin:resolve(handle_ns, "onedata.org", ?LB)),
                ?assert(meck:validate(application))
            end]},
%% multiple-part domain prefix
        {setup,
            fun domain_proper_setup/0,
            fun domain_teardown/1,
            [fun() ->
                ?assertEqual(refused, dns_worker_plugin:resolve(handle_a, "cyf.kr.onedata.org", ?LB)),
                ?assertEqual(refused, dns_worker_plugin:resolve(handle_ns, "cyf.kr.onedata.org", ?LB)),
                ?assert(meck:validate(application))
            end]}
    ].
%% test proper call of handle({handle_a, Domain})
a_test_() ->
    {setup,
        fun domain_proper_setup/0,
        fun domain_teardown/1,
        [fun() ->
            ?assertEqual(dns_worker_plugin:resolve(handle_a, "one.onedata.org", ?LB),
                {ok,
                    [{answer, "one.onedata.org", 60, a, {127, 0, 0, 1}},
                        {answer, "one.onedata.org", 60, a, {127, 0, 0, 2}},
                        {aa, true}]}),
            ?assertEqual(1, meck:num_calls(load_balancing, choose_nodes_for_dns, ['_'])),
            ?assert(meck:validate(application)),
            ?assert(meck:validate(load_balancing))
        end]}.

%% test proper call of handle({handle_ns, Domain})
ns_test_() ->
    {setup,
        fun domain_proper_setup/0,
        fun domain_teardown/1,
        [fun() ->
            ?assertEqual(dns_worker_plugin:resolve(handle_ns, "two.onedata.org", ?LB),
                {ok,
                    [{answer, "two.onedata.org", 600, ns, "127.0.0.1"},
                        {answer, "two.onedata.org", 600, ns, "127.0.0.2"},
                        {aa, true}]}),
            ?assertEqual(1, meck:num_calls(load_balancing, choose_ns_nodes_for_dns, ['_'])),
            ?assert(meck:validate(application)),
            ?assert(meck:validate(load_balancing))
        end]}.

%%%-------------------------------------------------------------------

domain_parser_setup() ->
    meck:new(application, [unstick]),
    meck:expect(application, get_env,
        fun(_, Param) ->
            case Param of
                oz_domain -> {ok, "onedata.org"};
                provider_domain -> {ok, "prov.onedata.org"}
            end
        end).

domain_parser_teardown(_) ->
    meck:unload(application).

domain_proper_setup() ->
    meck:new(application, [unstick]),
    meck:expect(application, get_env,
        fun(_, Param) ->
            case Param of
                dns_worker_plugin -> {ok, dns_worker_plugin};
                oz_domain -> {ok, "onedata.org"};
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