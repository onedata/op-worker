%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for monitoring storage backends' health.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_monitoring).

-include_lib("ctool/include/logging.hrl").

-export([
    perform_regular_checks/1
]).

-define(THROTTLE_STORAGE_MONITORING_LOG(Identifier, Log), utils:throttle(Identifier, 2*3600, fun() -> Log end)). % 2 hours

%%%===================================================================
%%% API
%%%===================================================================

-spec perform_regular_checks([storage:id()]) -> [storage:id()].
perform_regular_checks(PreviousUnhealthyStorageIds) ->
    {HealthyStorageIds, UnhealthyStorageIds} = classify_backends_by_health(),
    case UnhealthyStorageIds of
        PreviousUnhealthyStorageIds ->
            ?THROTTLE_STORAGE_MONITORING_LOG(
                {?MODULE, ?FUNCTION_NAME},
                log_backends_health_report(UnhealthyStorageIds, HealthyStorageIds)
            );
        _ ->
            PreviousUnhealthyStorageIds -- UnhealthyStorageIds =/= [] andalso
                ?notice("Following storage backends are no longer unhealthy:~n~ts",
                    [format_backends_log(PreviousUnhealthyStorageIds -- UnhealthyStorageIds)]),
            UnhealthyStorageIds -- PreviousUnhealthyStorageIds =/= [] andalso
                ?warning("Following storage backends became unhealthy - all request concerning the suppported spaces "
                "will be rejected:~n~ts", [format_backends_log(UnhealthyStorageIds -- PreviousUnhealthyStorageIds)])
    end,
    UnhealthyStorageIds.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec classify_backends_by_health() -> {HealthyStorageIds :: [storage:id()], UnhealthyStorageIds :: [storage:id()]}.
classify_backends_by_health() ->
    {ok, StorageList} = storage:get_all(),
    {HealthyStorageList, UnhealthyStorageList} = lists:partition(fun is_storage_healthy/1, StorageList),
    {lists:map(fun storage:get_id/1, HealthyStorageList), lists:map(fun storage:get_id/1, UnhealthyStorageList)}.


%% @private
-spec is_storage_healthy(storage:data()) -> boolean().
is_storage_healthy(StorageData) ->
    Helper = storage:get_helper(StorageData),
    LumaFeed = storage:get_luma_feed(StorageData),
    IgnoreReadWriteTest = storage:is_local_storage_readonly(StorageData) orelse
        (storage:is_imported(StorageData) andalso storage:supports_any_space(StorageData)),
    DiagnosticOpts = #{read_write_test => not IgnoreReadWriteTest},
    case storage_detector:run_diagnostics(this_node, Helper, LumaFeed, DiagnosticOpts) of
        ok ->
            true;
        {_Error, Reason} ->
            StorageId = storage:get_id(StorageData),
            ?THROTTLE_STORAGE_MONITORING_LOG(
                {?MODULE, StorageId, Reason},
                begin
                    ?error(?autoformat_with_msg("Storage '~ts' is unhealthy", [StorageId], Reason))
                end
            ),
            false
    end.


%% @private
-spec format_backends_log([storage:id()]) -> string().
format_backends_log(StorageIds) ->
    string:join([format_backend_log(S) || S <- StorageIds], "\n").


%% @private
-spec format_backend_log(storage:id()) -> string().
format_backend_log(StorageId) ->
    try
        Name = storage:fetch_name_of_local_storage(StorageId),
        Type = storage:get_helper_name(StorageId),
        str_utils:format(" - StorageId: ~ts~n   Name: ~ts~n   Type: ~ts", [StorageId, Name, Type])
    catch _:_ ->
        str_utils:format(" - StorageId: ~ts", [StorageId])
    end.


%% @private
-spec log_backends_health_report([storage:id()], [storage:id()]) -> ok.
log_backends_health_report([], []) ->
    ok;
log_backends_health_report(Unhealthy, []) ->
    ?warning("Storage health report: ~ts", [format_unhealthy_backends_report(Unhealthy)]);
log_backends_health_report([], Healthy) ->
    ?info("Storage health report: ~ts", [format_healthy_backends_report(Healthy)]);
log_backends_health_report(Unhealthy, Healthy) ->
    ?warning("Storage health report:~n - ~ts~n - ~ts", [
        format_healthy_backends_report(Healthy), format_unhealthy_backends_report(Unhealthy)]).


%% @private
-spec format_healthy_backends_report([storage:id()]) -> string().
format_healthy_backends_report([StorageId]) ->
    str_utils:format("the storage backend ~ts is healthy", [StorageId]);
format_healthy_backends_report(HealthyStorageIds) ->
    str_utils:format("~tp storage backends are healthy", [length(HealthyStorageIds)]).


%% @private
-spec format_unhealthy_backends_report([storage:id()]) -> string().
format_unhealthy_backends_report([StorageId]) ->
    str_utils:format("the storage backend ~ts remains unhealthy", [StorageId]);
format_unhealthy_backends_report(UnhealthyStorageIds) ->
    str_utils:format("~tp storage backends remain unhealthy", [length(UnhealthyStorageIds)]).
