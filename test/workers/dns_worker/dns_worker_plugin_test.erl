%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
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

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

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
            ?assertEqual(8, meck:num_calls(application, get_env, ['_', global_registry_domain])),
            ?assertEqual(8, meck:num_calls(application, get_env, ['_', provider_domain])),
            ?assert(meck:validate(application))
        end]}.

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

-endif.