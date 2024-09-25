%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Logic related to the circuit breaker mechanism, managed
%%% by Onepanel's DB disk usage monitor.
%%% @end
%%%-------------------------------------------------------------------
-module(op_worker_circuit_breaker).
-author("Katarzyna Such").

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-define(THROTTLE_LOG(Log), utils:throttle({?MODULE, ?FUNCTION_NAME}, timer:minutes(5), fun() -> Log end)).

%% API
-export([assert_closed/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec assert_closed() -> ok | no_return().
assert_closed() ->
    case op_worker:get_env(service_circuit_breaker_state, closed) of
        open ->
            ?THROTTLE_LOG(?critical(
                "All services have been temporarily disabled, consult Onepanel logs for details"
            )),
            throw(?ERROR_SERVICE_UNAVAILABLE);
        closed ->
            ok
    end.

