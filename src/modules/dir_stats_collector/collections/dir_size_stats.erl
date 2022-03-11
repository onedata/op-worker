%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for counting children and data size inside a directory
%%% (recursively, i.e. including all its subdirectories).
%%% It provides following statistics for each directory:
%%%    - ?REG_FILE_AND_LINK_COUNT - total number of regular files, hardlinks and symlinks,
%%%    - ?DIR_COUNT - total number of nested directories,
%%%    - ?TOTAL_SIZE - total byte size of the logical data,
%%%    - ?SIZE_ON_STORAGE(StorageId) - physical byte size on a specific storage.
%%% NOTE: the total size is not a sum of sizes on different storages, as the blocks stored
%%% on different storages may overlap.
%%%
%%% All statistics are stored as time series collection to track changes in time.
%%% Current values for each statistic are also stored in the time series collection in
%%% a special ?CURRENT_METRIC, along with the metrics representing changes in time.
%%% The current value metrics are pruned when retrieving the regular time series metrics.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_size_stats).
-author("Michal Wrzeszcz").


-behavior(dir_stats_collection_behaviour).


-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/time_series/common.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([get_stats/1, get_stats/2, get_stats_and_time_series_collections/1,
    report_reg_file_size_changed/3,
    report_file_created/2, report_file_deleted/2,
    report_file_moved/4,
    delete_stats/1]).

%% dir_stats_collection_behaviour callbacks
-export([acquire/1, consolidate/3, save/2, delete/1, init_dir/1, init_child/1]).

%% datastore_model callbacks
-export([get_ctx/0]).


-type ctx() :: datastore:ctx().


-define(CTX, #{
    model => ?MODULE
}).

-define(NOW(), global_clock:timestamp_seconds()).
% Metric storing current value of each statistic
-define(CURRENT_METRIC, <<"current">>).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_stats(file_id:file_guid()) ->
    {ok, dir_stats_collection:collection()} |
    ?ERROR_INTERNAL_SERVER_ERROR | ?ERROR_DIR_STATS_DISABLED_FOR_SPACE | ?ERROR_FORBIDDEN.
get_stats(Guid) ->
    get_stats(Guid, all).


%%--------------------------------------------------------------------
%% @doc
%% Provides subset of collection's statistics.
%% @end
%%--------------------------------------------------------------------
-spec get_stats(file_id:file_guid(), dir_stats_collection:stats_selector()) ->
    {ok, dir_stats_collection:collection()} |
    ?ERROR_INTERNAL_SERVER_ERROR | ?ERROR_DIR_STATS_DISABLED_FOR_SPACE | ?ERROR_FORBIDDEN.
get_stats(Guid, StatNames) ->
    dir_stats_collector:get_stats(Guid, ?MODULE, StatNames).


%%--------------------------------------------------------------------
%% @doc
%% Returns statistics collection (actual values of statistics) together with time series collection
%% (mean values of statistics in chosen time windows).
%% @end
%%--------------------------------------------------------------------
-spec get_stats_and_time_series_collections(file_id:file_guid()) ->
    {ok, {dir_stats_collection:collection(), time_series_collection:windows_map()}} |
    ?ERROR_INTERNAL_SERVER_ERROR | ?ERROR_DIR_STATS_DISABLED_FOR_SPACE | ?ERROR_FORBIDDEN.
get_stats_and_time_series_collections(Guid) ->
    case dir_stats_collector_config:is_collecting_active(file_id:guid_to_space_id(Guid)) of
        true ->
            case dir_stats_collector:flush_stats(Guid, ?MODULE) of
                ok ->
                    Uuid = file_id:guid_to_uuid(Guid),
                    case datastore_time_series_collection:list_windows(?CTX, Uuid, #{}) of
                        {ok, WindowsMap} -> {ok, all_metrics_to_stats_and_time_series_collections(WindowsMap)};
                        {error, not_found} -> {ok, {gen_empty_stats_collection(Guid), gen_empty_time_series_collection(Guid)}}
                    end;
                ?ERROR_FORBIDDEN ->
                    ?ERROR_FORBIDDEN;
                ?ERROR_INTERNAL_SERVER_ERROR ->
                    ?ERROR_INTERNAL_SERVER_ERROR
            end;
        false ->
            ?ERROR_DIR_STATS_DISABLED_FOR_SPACE
    end.


-spec report_reg_file_size_changed(file_id:file_guid(), total | {on_storage, storage:id()}, integer()) -> ok.
report_reg_file_size_changed(_Guid, _Scope, 0) ->
    ok;
report_reg_file_size_changed(Guid, total, SizeDiff) ->
    ok = dir_stats_collector:update_stats_of_parent(Guid, ?MODULE, #{?TOTAL_SIZE => SizeDiff});
report_reg_file_size_changed(Guid, {on_storage, StorageId}, SizeDiff) ->
    ok = dir_stats_collector:update_stats_of_parent(Guid, ?MODULE, #{?SIZE_ON_STORAGE(StorageId) => SizeDiff}).


-spec report_file_created(file_meta:type(), file_id:file_guid()) -> ok.
report_file_created(?DIRECTORY_TYPE, Guid) ->
    update_stats(Guid, #{?DIR_COUNT => 1});
report_file_created(_, Guid) ->
    update_stats(Guid, #{?REG_FILE_AND_LINK_COUNT => 1}).


-spec report_file_deleted(file_meta:type(), file_id:file_guid()) -> ok.
report_file_deleted(?DIRECTORY_TYPE, Guid) ->
    update_stats(Guid, #{?DIR_COUNT => -1});
report_file_deleted(_, Guid) ->
    update_stats(Guid, #{?REG_FILE_AND_LINK_COUNT => -1}).


-spec report_file_moved(file_meta:type(), file_id:file_guid(), file_id:file_guid(), file_id:file_guid()) -> ok.
report_file_moved(?DIRECTORY_TYPE, FileGuid, SourceParentGuid, TargetParentGuid) ->
    case dir_stats_collector_config:is_collecting_active(file_id:guid_to_space_id(FileGuid)) of
        true ->
            {ok, Collection} = get_stats(FileGuid),
            update_stats(TargetParentGuid, Collection),
            update_stats(SourceParentGuid, maps:map(fun(_, Value) -> -Value end, Collection));
        false ->
            ok
    end;
report_file_moved(Type, _FileGuid, SourceParentGuid, TargetParentGuid) ->
    report_file_created(Type, TargetParentGuid),
    report_file_deleted(Type, SourceParentGuid).


-spec delete_stats(file_id:file_guid()) -> ok.
delete_stats(Guid) ->
    dir_stats_collector:delete_stats(Guid, ?MODULE).


%%%===================================================================
%%% dir_stats_collection_behaviour callbacks
%%%===================================================================

-spec acquire(file_id:file_guid()) -> dir_stats_collection:collection().
acquire(Guid) ->
    case datastore_time_series_collection:list_windows(
        ?CTX, file_id:guid_to_uuid(Guid), {all, ?CURRENT_METRIC}, #{limit => 1}
    ) of
        {ok, WindowsMap} -> current_metrics_to_stats_collection(WindowsMap);
        {error, not_found} -> gen_empty_stats_collection(Guid)
    end.


-spec consolidate(dir_stats_collection:stat_name(), dir_stats_collection:stat_value(),
    dir_stats_collection:stat_value()) -> dir_stats_collection:stat_value().
consolidate(_, Value, Diff) ->
    Value + Diff.


-spec save(file_id:file_guid(), dir_stats_collection:collection()) -> ok.
save(Guid, Collection) ->
    Uuid = file_id:guid_to_uuid(Guid),
    case datastore_time_series_collection:update(?CTX, Uuid, ?NOW(), maps:to_list(Collection)) of
        ok ->
            ok;
        ?ERROR_NOT_FOUND ->
            Config = maps:from_list(lists:map(fun(StatName) ->
                {StatName, metrics_extended_with_current_value()}
            end, stat_names(Guid))),
            % NOTE: single pes process is dedicated for each guid so race resulting in
            % {error, collection_already_exists} is impossible - match create answer to ok
            ok = datastore_time_series_collection:create(?CTX, Uuid, Config),
            save(Guid, Collection)
    end.


-spec delete(file_id:file_guid()) -> ok.
delete(Guid) ->
    case datastore_time_series_collection:delete(?CTX, file_id:guid_to_uuid(Guid)) of
        ok -> ok;
        {error, not_found} -> ok
    end.


-spec init_dir(file_id:file_guid()) -> dir_stats_collection:collection().
init_dir(Guid) ->
    gen_empty_stats_collection(Guid).


-spec init_child(file_id:file_guid()) -> dir_stats_collection:collection().
init_child(Guid) ->
    EmptyCollection = gen_empty_stats_collection(Guid),
    case file_meta:get_including_deleted(file_id:guid_to_uuid(Guid)) of
        {ok, Doc} ->
            case file_meta:get_type(Doc) of
                ?DIRECTORY_TYPE ->
                    EmptyCollection#{?DIR_COUNT => 1};
                _ ->
                    {FileSizes, _} = file_ctx:get_file_size_summary(file_ctx:new_by_guid(Guid)),
                    lists:foldl(fun
                        ({total, Size}, Acc) -> Acc#{?TOTAL_SIZE => Size};
                        ({StorageId, Size}, Acc) -> Acc#{?SIZE_ON_STORAGE(StorageId) => Size}
                    end, EmptyCollection#{?REG_FILE_AND_LINK_COUNT => 1}, FileSizes)
            end;
        ?ERROR_NOT_FOUND ->
            EmptyCollection % Race with file deletion - stats will be invalidated by next update
    end.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec update_stats(file_id:file_guid(), dir_stats_collection:collection()) -> ok.
update_stats(Guid, CollectionUpdate) ->
    ok = dir_stats_collector:update_stats_of_dir(Guid, ?MODULE, CollectionUpdate).


%% @private
-spec stat_names(file_id:file_guid()) -> [dir_stats_collection:stat_name()].
stat_names(Guid) ->
    {ok, StorageId} = space_logic:get_local_supporting_storage(file_id:guid_to_space_id(Guid)),
    [?REG_FILE_AND_LINK_COUNT, ?DIR_COUNT, ?TOTAL_SIZE, ?SIZE_ON_STORAGE(StorageId)].


%% @private
-spec metrics_extended_with_current_value() -> #{ts_metric:id() => ts_metric:config()}.
metrics_extended_with_current_value() ->
    maps:put(?CURRENT_METRIC, #metric_config{
        resolution = 1,
        retention = 1,
        aggregator = last
    }, metrics()).


%% @private
-spec metrics() -> #{ts_metric:id() => ts_metric:config()}.
metrics() ->
    #{
        ?MINUTE_METRIC => #metric_config{
            resolution = ?MINUTE_RESOLUTION,
            retention = 120,
            aggregator = sum
        },
        ?HOUR_METRIC => #metric_config{
            resolution = ?HOUR_RESOLUTION,
            retention = 48,
            aggregator = sum
        },
        ?DAY_METRIC => #metric_config{
            resolution = ?DAY_RESOLUTION,
            retention = 60,
            aggregator = sum
        },
        ?MONTH_METRIC => #metric_config{
            resolution = ?MONTH_RESOLUTION,
            retention = 12,
            aggregator = sum
        }
    }.


%% @private
-spec all_metrics_to_stats_and_time_series_collections(time_series_collection:windows_map()) ->
    {dir_stats_collection:collection(), time_series_collection:windows_map()}.
all_metrics_to_stats_and_time_series_collections(WindowsMap) ->
    CurrentValues = maps:filter(fun(
        {_, ?CURRENT_METRIC}, _) -> true;
        (_, _) -> false
    end, WindowsMap),

    {
        current_metrics_to_stats_collection(CurrentValues),
        time_metrics_to_time_series_collection(maps:without(maps:keys(CurrentValues), WindowsMap))
    }.


%% @private
-spec current_metrics_to_stats_collection(time_series_collection:windows_map()) -> dir_stats_collection:collection().
current_metrics_to_stats_collection(WindowsMap) ->
    maps_utils:map_key_value(fun
        ({StatName, _}, [{_Timestamp, Value}]) -> {StatName, Value};
        ({StatName, _}, []) -> {StatName, 0}
    end, WindowsMap).


%% @private
-spec time_metrics_to_time_series_collection(time_series_collection:windows_map()) -> dir_stats_collection:collection().
time_metrics_to_time_series_collection(WindowsMap) ->
    maps:map(fun(_, Windows) ->
        lists:map(fun({Timestamp, {Count, Sum}}) -> {Timestamp, round(Sum / Count)} end, Windows)
    end, WindowsMap).


%% @private
-spec gen_empty_stats_collection(file_id:file_guid()) -> dir_stats_collection:collection().
gen_empty_stats_collection(Guid) ->
    maps:from_list(lists:map(fun(StatName) -> {StatName, 0} end, stat_names(Guid))).


%% @private
-spec gen_empty_time_series_collection(file_id:file_guid()) -> time_series_collection:windows_map().
gen_empty_time_series_collection(Guid) ->
    MetricIds = maps:keys(metrics()),
    maps:from_list(lists:flatmap(fun(StatName) ->
        lists:map(fun(MetricId) -> {{StatName, MetricId}, []} end, MetricIds)
    end, stat_names(Guid))).