%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Machinery for suspending a product's background jobs deterministically and
%%% releasing them in batches, so that a test can interleave other operations
%%% with a traverse/transfer that is underway.
%%%
%%% A gate is a permit counter kept on a provider node. Every job passing through
%%% it must acquire a permit; a job finding none parks (notifying the test
%%% process) until permits are granted. No permits exist initially, so the first
%%% job to reach the gate parks.
%%%
%%% == Usage ==
%%%
%%% The mock that routes jobs through the gate stays with the caller - which
%%% product function to intercept is domain knowledge, and its arity decides the
%%% shape of the expectation fun:
%%% ```
%%%     TestProcess = self(),
%%%     ok = permit_gate_test_utils:install(my_gate, Nodes),
%%%     ok = test_utils:mock_new(Nodes, some_worker, [passthrough]),
%%%     ok = test_utils:mock_expect(Nodes, some_worker, process_job, fun(Job) ->
%%%         permit_gate_test_utils:acquire_permit(my_gate, TestProcess),
%%%         meck:passthrough([Job])
%%%     end),
%%%     ...
%%%     ok = permit_gate_test_utils:uninstall(my_gate, Nodes, some_worker).
%%% '''
%%% As acquire_permit/2 runs on the provider node, this module must be added to
%%% ?LOAD_MODULES of any suite installing a gate.
%%%
%%% Several gates may coexist - the gate name scopes both the permit counter and
%%% the parking notification.
%%%
%%% NOTE: never unload the gated mock by hand - uninstall/3 does it in the only
%%% safe order (see its doc).
%%% @end
%%%-------------------------------------------------------------------
-module(permit_gate_test_utils).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([install/2, uninstall/3]).
-export([grant_permits/3, await_parked_job/2]).
-export([await_no_ongoing_calls_within_module/2]).
%% on-node routines (executed on op_worker via rpc or from within a mock)
-export([acquire_permit/2]).

-type gate() :: atom().
-export_type([gate/0]).

% Granting "all" permits sets the counter to a value no test can exhaust rather
% than removing the gate - jobs keep passing through the (still installed) mock.
-define(INFINITE_PERMITS, 1 bsl 50).
-define(PERMIT_POLL_INTERVAL_MS, 100).

-define(PERMITS_KEY(__GATE), {?MODULE, permits, __GATE}).
-define(JOB_PARKED_MSG(__GATE), {?MODULE, job_parked, __GATE}).

-define(MOCKED_MODULE_CALLS_DRAIN_ATTEMPTS, 150).
-define(MOCKED_MODULE_CALLS_DRAIN_POLL_INTERVAL_MS, 100).


%%%===================================================================
%%% API functions
%%%===================================================================


%% @doc Creates the gate's permit counter, with no permits available.
-spec install(gate(), node() | [node()]) -> ok.
install(Gate, NodeOrNodes) ->
    lists:foreach(fun(Node) ->
        % the counter must be created on the provider node (atomics are
        % node-local); the node-wide cache entry keeps the ref alive (an ets
        % table would die with its owner - the transient rpc process)
        ok = opw_test_rpc:call(Node, fun() ->
            node_cache:put(?PERMITS_KEY(Gate), atomics:new(1, []))
        end)
    end, utils:ensure_list(NodeOrNodes)).


%%--------------------------------------------------------------------
%% @doc
%% Removes the gate: releases everything parked, awaits the released jobs
%% leaving the mocked module, unloads its mock and drops the permit counter.
%% This order is load-bearing - a job killed by the code purge mid-call never
%% reports back to whatever dispatched it, which typically leaves the enclosing
%% operation (transfer, auto-cleaning run, traverse) ongoing forever, holding
%% its slot in a provider-wide worker pool until the provider restarts.
%%
%% Tolerates the gate not being installed at all, so that it can also be used to
%% clear one left behind by an interrupted run.
%% @end
%%--------------------------------------------------------------------
-spec uninstall(gate(), node() | [node()], module()) -> ok.
uninstall(Gate, NodeOrNodes, MockedModule) ->
    Nodes = utils:ensure_list(NodeOrNodes),
    lists:foreach(fun(Node) ->
        ok = opw_test_rpc:call(Node, fun() ->
            % unlike grant_permits/3, tolerate the gate not being installed
            case node_cache:get(?PERMITS_KEY(Gate), undefined) of
                undefined -> ok;
                PermitsRef -> atomics:put(PermitsRef, 1, ?INFINITE_PERMITS)
            end
        end)
    end, Nodes),
    await_no_ongoing_calls_within_module(Nodes, MockedModule),
    ok = test_utils:mock_unload(Nodes, MockedModule),
    lists:foreach(fun(Node) ->
        ok = opw_test_rpc:call(Node, node_cache, clear, [?PERMITS_KEY(Gate)])
    end, Nodes).


%% @doc Lets through the given number of parked/upcoming jobs ('all' - any number).
%% Fails if the gate is not installed (which would otherwise surface only as the
%% gated operation never progressing).
-spec grant_permits(gate(), node() | [node()], pos_integer() | all) -> ok.
grant_permits(Gate, NodeOrNodes, CountOrAll) ->
    % permits are granted per node (irrelevant for single node providers)
    lists:foreach(fun(Node) ->
        ok = opw_test_rpc:call(Node, fun() ->
            PermitsRef = node_cache:get(?PERMITS_KEY(Gate)),
            case CountOrAll of
                all -> atomics:put(PermitsRef, 1, ?INFINITE_PERMITS);
                Count -> atomics:add(PermitsRef, 1, Count)
            end
        end)
    end, utils:ensure_list(NodeOrNodes)).


%%--------------------------------------------------------------------
%% @doc
%% Awaits the notification a job sends when it parks at the gate - proof that
%% the gated operation is underway and suspended. Only jobs that actually park
%% notify, so leftover mailbox messages cannot produce a false positive for an
%% already-drained gate.
%% @end
%%--------------------------------------------------------------------
-spec await_parked_job(gate(), time:seconds()) -> ok.
await_parked_job(Gate, TimeoutSeconds) ->
    receive
        ?JOB_PARKED_MSG(Gate) -> ok
    after timer:seconds(TimeoutSeconds) ->
        ct:fail({no_job_parked_at_gate, Gate})
    end.


%%--------------------------------------------------------------------
%% @doc
%% Awaits until no process on the given nodes executes the code of the given
%% (mocked) module - unloading a mock purges the module's code, killing any
%% process executing it (the mock and the meck-renamed original alike). The
%% await is bounded: on timeout a warning is logged and the caller proceeds
%% with the unload (a test run must not hang on a wedged job forever).
%% @end
%%--------------------------------------------------------------------
-spec await_no_ongoing_calls_within_module(node() | [node()], module()) -> ok.
await_no_ongoing_calls_within_module(NodeOrNodes, Module) ->
    MatchedModules = [Module, meck_util:original_name(Module)],
    lists:foreach(fun(Node) ->
        await_no_ongoing_calls_within_module(
            Node, MatchedModules, ?MOCKED_MODULE_CALLS_DRAIN_ATTEMPTS
        )
    end, utils:ensure_list(NodeOrNodes)).


%%%===================================================================
%%% On-node routines
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Runs on the provider node, within the gated job's process (called from the
%% mock installed by the caller - see the module doc). Returns as soon as a
%% permit is acquired; otherwise notifies the given process and parks.
%% @end
%%--------------------------------------------------------------------
-spec acquire_permit(gate(), pid()) -> ok.
acquire_permit(Gate, TestProcess) ->
    PermitsRef = node_cache:get(?PERMITS_KEY(Gate)),
    case try_acquire_permit(PermitsRef) of
        true ->
            ok;
        false ->
            TestProcess ! ?JOB_PARKED_MSG(Gate),
            wait_for_permit(PermitsRef)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec wait_for_permit(atomics:atomics_ref()) -> ok.
wait_for_permit(PermitsRef) ->
    case try_acquire_permit(PermitsRef) of
        true ->
            ok;
        false ->
            timer:sleep(?PERMIT_POLL_INTERVAL_MS),
            wait_for_permit(PermitsRef)
    end.


%% @private
-spec try_acquire_permit(atomics:atomics_ref()) -> boolean().
try_acquire_permit(PermitsRef) ->
    case atomics:sub_get(PermitsRef, 1, 1) of
        Permits when Permits >= 0 ->
            true;
        _ ->
            % return the overdrawn permit (the counter may transiently go
            % negative under concurrent acquisitions but never loses permits)
            atomics:add(PermitsRef, 1, 1),
            false
    end.


%% @private
-spec await_no_ongoing_calls_within_module(node(), [module()], non_neg_integer()) ->
    ok.
await_no_ongoing_calls_within_module(Node, MatchedModules, 0) ->
    ct:pal(
        "WARNING: unloading the mock of ~tp on node ~tp while some process is "
        "still executing its code - the code purge will kill it",
        [hd(MatchedModules), Node]
    );
await_no_ongoing_calls_within_module(Node, MatchedModules, AttemptsLeft) ->
    AnyProcessExecutingModule = opw_test_rpc:call(Node, fun() ->
        lists:any(fun(Pid) ->
            case erlang:process_info(Pid, current_stacktrace) of
                {current_stacktrace, Stacktrace} ->
                    lists:any(fun(StackModule) ->
                        lists:keymember(StackModule, 1, Stacktrace)
                    end, MatchedModules);
                undefined ->
                    false
            end
        end, erlang:processes())
    end),
    case AnyProcessExecutingModule of
        false ->
            ok;
        true ->
            timer:sleep(?MOCKED_MODULE_CALLS_DRAIN_POLL_INTERVAL_MS),
            await_no_ongoing_calls_within_module(Node, MatchedModules, AttemptsLeft - 1)
    end.
