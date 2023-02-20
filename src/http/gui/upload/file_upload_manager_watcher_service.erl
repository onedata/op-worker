%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal (persistent) service that maintains an alive file_upload_manager
%%% process. The internal service interface is used in a non-standard way -
%%% this service does not depend on the start_function and stop_function but
%%% manages file_upload_manager restarts by itself.
%%% @end
%%%-------------------------------------------------------------------
-module(file_upload_manager_watcher_service).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([setup_internal_service/0]).
-export([terminate_internal_service/0]).

%% Internal Service callbacks
-export([start_service/0, stop_service/0, healthcheck/1]).


-define(SERVICE_NAME, <<"file-upload-manager-watcher-service">>).

-define(HEALTHCHECK_BASE_INTERVAL, timer:seconds(1)).
-define(HEALTHCHECK_BACKOFF_RATE, 1.35).
-define(HEALTHCHECK_MAX_BACKOFF, timer:seconds(20)).


%%%===================================================================
%%% API
%%%===================================================================


-spec setup_internal_service() -> ok.
setup_internal_service() ->
    ok = internal_services_manager:start_service(?MODULE, ?SERVICE_NAME, ?SERVICE_NAME, #{
        start_function => start_service,
        stop_function => stop_service,
        healthcheck_fun => healthcheck,
        healthcheck_interval => ?HEALTHCHECK_BASE_INTERVAL,
        async_start => true
    }).


-spec terminate_internal_service() -> ok.
terminate_internal_service() ->
    case node() =:= internal_services_manager:get_processing_node(?SERVICE_NAME) of
        true ->
            try
                ok = internal_services_manager:stop_service(?MODULE, ?SERVICE_NAME, ?SERVICE_NAME)
            catch Class:Reason:Stacktrace ->
                ?error_exception(Class, Reason, Stacktrace)
            end;
        false ->
            ok
    end.


%%%===================================================================
%%% Internal services API
%%%===================================================================


-spec start_service() -> ok.
start_service() ->
    start_file_upload_manager(),
    ok.


-spec stop_service() -> ok.
stop_service() ->
    ok.


-spec healthcheck(time:millis()) -> {ok, time:millis()}.
healthcheck(LastInterval) ->
    case file_upload_manager:whereis() of
        undefined ->
            case start_file_upload_manager() of
                ok -> {ok, ?HEALTHCHECK_BASE_INTERVAL};
                error -> {ok, calculate_backoff(LastInterval)}
            end;
        _Pid ->
            {ok, ?HEALTHCHECK_BASE_INTERVAL}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec start_file_upload_manager() -> ok | error.
start_file_upload_manager() ->
    % Catch errors when supervisor does not exist
    case catch supervisor:start_child(?FSLOGIC_WORKER_SUP, file_upload_manager:spec()) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        Error->
            ?critical("Failed to start file_upload_manager due to: ~p", [Error]),
            error
    end.


%% @private
-spec calculate_backoff(time:millis()) -> time:millis().
calculate_backoff(LastInterval) ->
    min(?HEALTHCHECK_MAX_BACKOFF, round(LastInterval * ?HEALTHCHECK_BACKOFF_RATE)).
