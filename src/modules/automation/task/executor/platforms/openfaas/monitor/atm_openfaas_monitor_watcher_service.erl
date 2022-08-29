%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal (persistent) service that maintains an alive atm_openfaas_monitor
%%% process.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_monitor_watcher_service).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([setup_internal_service/0]).

%% Internal Service callbacks
-export([start_service/0, stop_service/0]).


-define(SERVICE_NAME, <<"atm-openfaas-monitor-watcher-service">>).


%%%===================================================================
%%% API
%%%===================================================================


-spec setup_internal_service() -> ok.
setup_internal_service() ->
    ok = internal_services_manager:start_service(?MODULE, ?SERVICE_NAME, ?SERVICE_NAME, #{
        start_function => start_service,
        stop_function => stop_service
    }).


%%%===================================================================
%%% Internal services API
%%%===================================================================


-spec start_service() -> ok | error.
start_service() ->
    case catch supervisor:start_child(?FSLOGIC_WORKER_SUP, atm_openfaas_monitor:spec()) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        Error->
            ?debug("Failed to start file_upload_manager due to: ~p", [Error]),
            {error, Error}
    end.


-spec stop_service() -> ok.
stop_service() ->
    supervisor:terminate_child(?FSLOGIC_WORKER_SUP, maps:get(id, atm_openfaas_monitor:spec())).
