%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal (persistent) service that maintains single instance
%%% (across cluster) for each process supporting automation machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_warden_service).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([setup_internal_service/0]).

%% Internal Service callbacks
-export([start_service/0, stop_service/0]).


-define(SERVICE_SUP, ?ATM_SUPERVISION_WORKER_SUP).


%%%===================================================================
%%% API
%%%===================================================================


-spec setup_internal_service() -> ok.
setup_internal_service() ->
    ok = internal_services_manager:start_service(
        ?MODULE,
        ?ATM_WARDEN_SERVICE_NAME,
        ?ATM_WARDEN_SERVICE_ID,
        #{
            start_function => start_service,
            stop_function => stop_service
        }
    ).


%%%===================================================================
%%% Internal services API
%%%===================================================================


-spec start_service() -> ok | abort.
start_service() ->
    start_processes([
        atm_openfaas_monitor:spec(),
        atm_workflow_execution_garbage_collector:spec()
    ]).


-spec stop_service() -> ok.
stop_service() ->
    lists:foreach(fun stop_process/1, [
        atm_workflow_execution_garbage_collector:id(),
        atm_openfaas_monitor:id()
    ]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec start_processes([supervisor:child_spec()]) -> ok | abort.
start_processes([]) ->
    ok;
start_processes([ChildSpec | LeftoverChildSpecs]) ->
    case start_process(ChildSpec) of
        ok -> start_processes(LeftoverChildSpecs);
        abort -> abort
    end.


%% @private
-spec start_process(supervisor:child_spec()) -> ok | abort.
start_process(#{id := ChildId} = ChildSpec) ->
    case catch supervisor:start_child(?SERVICE_SUP, ChildSpec) of
        {ok, _} ->
            ok;
        Error ->
            ?critical("Failed to start ~p due to: ~p", [ChildId, Error]),
            abort
    end.


%% @private
-spec stop_process(atom()) -> ok.
stop_process(ChildId) ->
    ok = supervisor:terminate_child(?SERVICE_SUP, ChildId),
    ok = supervisor:delete_child(?SERVICE_SUP, ChildId).
