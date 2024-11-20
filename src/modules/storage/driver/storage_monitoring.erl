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
    perform_storages_check/1,
    check_storages_health/0
]).

-define(THROTTLE_STORAGE_MONITORING_LOG(Identifier, Log), utils:throttle(Identifier, 2*3600, fun() -> Log end)). % 2 hours

%%%===================================================================
%%% API
%%%===================================================================

-spec perform_storages_check([storage:id()]) -> [storage:id()].
perform_storages_check(PreviousUnhealthyStorages) ->
    {ok, UnhealthyStoragesIds, AllStoragesIds} = check_storages_health(),
    case UnhealthyStoragesIds of
        PreviousUnhealthyStorages ->
            ?THROTTLE_STORAGE_MONITORING_LOG(
                {?MODULE, ?FUNCTION_NAME},
                log_storages_health_report(UnhealthyStoragesIds, AllStoragesIds -- UnhealthyStoragesIds)
            );
        _ ->
            PreviousUnhealthyStorages -- UnhealthyStoragesIds =/= [] andalso
                ?notice("Following storage backends are no longer unhealthy:~n~ts",
                    [format_storages_log(PreviousUnhealthyStorages -- UnhealthyStoragesIds)]),
            UnhealthyStoragesIds -- PreviousUnhealthyStorages =/= [] andalso
                ?warning("Following storage backends became unhealthy - all request concerning the suppported spaces "
                "will be rejected:~n~ts", [format_storages_log(UnhealthyStoragesIds -- PreviousUnhealthyStorages)])
    end,
    UnhealthyStoragesIds.


-spec check_storages_health() -> {ok, [storage:id()], [storage:id()]}.
check_storages_health() ->
    {ok, StoragesData} = storage:get_all(),
    UnhealthyStoragesIds = lists:filtermap(fun(StorageData) ->
        case is_storage_healthy(StorageData) of
            true -> false;
            false -> {true, storage:get_id(StorageData)}
        end
    end, StoragesData),
    {ok, UnhealthyStoragesIds, lists:map(fun storage:get_id/1, StoragesData)}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

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
-spec format_storages_log([storage:id()]) -> string().
format_storages_log(StorageIds) ->
    string:join([format_storage_log(S) || S <- StorageIds], "\n").


%% @private
-spec format_storage_log(storage:id()) -> string().
format_storage_log(StorageId) ->
    try
        Name = storage:fetch_name_of_local_storage(StorageId),
        Type = storage:get_helper_name(StorageId),
        str_utils:format(" - StorageId: ~ts~n   Name: ~ts~n   Type: ~ts", [StorageId, Name, Type])
    catch _:_ ->
        str_utils:format(" - StorageId: ~ts", [StorageId])
    end.


%% @private
-spec log_storages_health_report([storage:id()], [storage:id()]) -> ok.
log_storages_health_report([], []) ->
    ok;
log_storages_health_report(Unhealthy, []) ->
    ?warning("Storage health report: ~ts", [format_unhealthy_storages_report(Unhealthy)]);
log_storages_health_report([], Healthy) ->
    ?info("Storage health report: ~ts", [format_healthy_storages_report(Healthy)]);
log_storages_health_report(Unhealthy, Healthy) ->
    ?warning("Storage health report:~n - ~ts~n - ~ts", [
        format_healthy_storages_report(Healthy), format_unhealthy_storages_report(Unhealthy)]).


%% @private
-spec format_healthy_storages_report([storage:id()]) -> string().
format_healthy_storages_report([StorageId]) ->
    str_utils:format("the storage backend ~ts is healthy", [StorageId]);
format_healthy_storages_report(HealthyStorages) ->
    str_utils:format("~tp storage backends are healthy", [length(HealthyStorages)]).


%% @private
-spec format_unhealthy_storages_report([storage:id()]) -> string().
format_unhealthy_storages_report([StorageId]) ->
    str_utils:format("the storage backend ~ts remains unhealthy", [StorageId]);
format_unhealthy_storages_report(HealthyStorages) ->
    str_utils:format("~tp storage backends remain unhealthy", [length(HealthyStorages)]).
