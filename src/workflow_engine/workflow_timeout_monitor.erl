%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for checking timeouts of async task executions.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_timeout_monitor).
-author("Michal Wrzeszcz").

%% API
-export([init/1, report_heartbeat/2]).

-define(TIMEOUT_CHECK_PERIOD, timer:seconds(30)).

%%%===================================================================
%%% API
%%%===================================================================

% TODO VFS-7787 - attach to supervisor, use gen_server
-spec init(workflow_engine:id()) -> ok.
init(EngineId) ->
    Node = datastore_key:any_responsible_node(EngineId),
    spawn(Node, fun() -> server_loop(EngineId) end),
    ok.

-spec report_heartbeat(workflow_engine:execution_id(), workflow_jobs:job_identifier()) -> ok.
report_heartbeat(ExecutionId, JobIdentifier) ->
    workflow_execution_state:reset_keepalive_timer(ExecutionId, JobIdentifier).

%%%===================================================================
%%% Server loop
%%%===================================================================

-spec server_loop(workflow_engine:id()) -> ok.
server_loop(EngineId) ->
    receive
        stop ->
            ok
    after
        ?TIMEOUT_CHECK_PERIOD ->
            check_timeouts(EngineId),
            server_loop(EngineId)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec check_timeouts(workflow_engine:id()) -> ok.
check_timeouts(EngineId) ->
    ExecutionIds = workflow_engine_state:get_execution_ids(EngineId),
    lists:foreach(fun workflow_execution_state:check_timeouts/1, ExecutionIds).
