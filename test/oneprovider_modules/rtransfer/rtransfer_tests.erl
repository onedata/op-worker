%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module tests the functionality of helper functions contained
%% in rt_utils module.
%% @end
%% ===================================================================
-module(rtransfer_tests).

-ifdef(TEST).

-include("registered_names.hrl").
-include("oneprovider_modules/rtransfer/rtransfer.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").

-define(GW_PORT, 12345).

%% ===================================================================
%% Tests description
%% ===================================================================

rtransfer_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            {"should cache Provider address responses from Global Registry", fun should_cache_provider_address/0}
        ]
    }.

%% ===================================================================
%% Setup/teardown functions
%% ===================================================================

setup() ->
    application:set_env(?APP_Name, rtransfer_fetch_retry_number, 1),
    application:set_env(?APP_Name, gateway_listener_port, ?GW_PORT),
    ets:new(rtransfer_tab, [public, named_table]).

teardown(_) ->
    ok.

%% ===================================================================
%% Tests functions
%% ===================================================================

should_cache_provider_address() ->
    ProviderId = <<"ProviderId">>,

    ok = meck:new(gr_providers),
    ok = meck:expect(gr_providers, get_details,
        fun(_, _) -> {ok, #provider_details{urls = [<<"192.168.1.1">>]}} end),

    ?assertEqual({{192,168,1,1}, ?GW_PORT}, rtransfer:provider_id_to_remote(ProviderId)),
    ?assertEqual({{192,168,1,1}, ?GW_PORT}, rtransfer:provider_id_to_remote(ProviderId)),

    ?assertEqual(1, meck:num_calls(gr_providers, get_details, [provider, ProviderId])),
    meck:unload().

-endif.
