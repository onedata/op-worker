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

%%%===================================================================
%%% Test functions
%%%===================================================================

%% tests init() function when 'test' atom passed as argument
testing_init_test_() ->
  Res = dns_worker:init(test),
  ?_assertEqual(Res, {ok, #{}}).

%% tests init() function when empty map passed as argument
init_with_empty_map_test_() ->
  Res = dns_worker:init(#{}),
  ?_assertEqual(Res, {ok, #{}}).

%% tests init() function when trash passed as argument
init_with_undefined_test_() ->
  ?_assertException(throw, unknown_initial_state, dns_worker:init(some_trash)).

%%%-------------------------------------------------------------------

%%   tests healthcheck() behaviour when LBAdvice is 'undefined'
healthcheck_undefined_test_() ->
  fun() ->
    meck:new(worker_host, [unstick]),
  %%   I need to mock two clauses of one function so I need such a case
    meck:expect(worker_host, state_get,
      fun(_, Param) ->
        case Param of
          lb_advice -> undefined;
          last_update -> {1,2,3}
        end
      end),
    ?assertEqual({error, no_lb_advice_received}, dns_worker:handle(healthcheck)),
    meck:unload(worker_host)
  end.

%% tests whether dns_worker returns out_of_sync in case it's out_of_sync indeed
healthcheck_outofsync_test_() ->
  fun() ->
    meck:new(worker_host, [unstick]),
    meck:expect(worker_host, state_get,
      fun(_, Param) ->
        case Param of
          lb_advice -> some_lb;
          last_update -> {1,2,3}
        end
      end),
    meck:new(application, [unstick]),
    meck:expect(application, get_env, fun(_, dns_disp_out_of_sync_threshold) -> {ok, 10} end),
    meck:new(timer, [unstick]),
    meck:expect(timer, now_diff, fun(_Now, _LastUpdate) -> 12000 end),

    ?assertEqual(out_of_sync, dns_worker:handle(healthcheck)),
    meck:unload(timer),
    meck:unload(application),
    meck:unload(worker_host)
  end.



-endif.