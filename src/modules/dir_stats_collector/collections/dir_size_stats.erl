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
%%% This module offers two types of statistics in its API:
%%%   * current_stats() - a collection with current values for each statistic,
%%%   * time_stats() - time series collection slice showing the changes of stats in time.
%%% Internally, both collections are kept in the same underlying persistent
%%% time series collection - internal_stats(). The current statistics are stored
%%% in the special ?CURRENT_METRIC. Additionally, the internal_stats() hold dir stats
%%% incarnation info in a separate time series. The internal_stats() are properly
%%% trimmed into current_stats() and/or time_stats() when these collections are retrieved.
%%%
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
-export([
    get_layout/1,
    get_stats/1, get_stats/2, 
    get_stats_and_time_series_collection/1,
    get_stats_and_time_series_collection/3,
    report_reg_file_size_changed/3,
    report_file_created/2, report_file_deleted/2,
    report_file_moved/4,
    delete_stats/1]).

%% dir_stats_collection_behaviour callbacks
-export([acquire/1, consolidate/3, save/3, delete/1, init_dir/1, init_child/1]).

%% datastore_model callbacks
-export([get_ctx/0]).


-type ctx() :: datastore:ctx().

%% see the module doc
-type current_stats() :: dir_stats_collection:collection().
-type time_stats() :: time_series_collection:slice().
-type internal_stats() :: time_series_collection:slice().


-define(CTX, #{
    model => ?MODULE
}).

-define(NOW(), global_clock:timestamp_seconds()).
% Metric storing current value of statistic or incarnation (depending on time series)
-define(CURRENT_METRIC, <<"current">>).
% Time series storing incarnation - historical values are not required
% but usage of time series allows keeping everything in single structure
-define(INCARNATION_TIME_SERIES, <<"incarnation">>).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_layout(file_id:file_guid()) -> {ok, time_series_collection:layout()} | {error, term()}.
get_layout(Guid) ->
    datastore_time_series_collection:get_layout(?CTX, file_id:guid_to_uuid(Guid)).


-spec get_stats(file_id:file_guid()) -> {ok, current_stats()} | dir_stats_collector:error().
get_stats(Guid) ->
    get_stats(Guid, all).


%%--------------------------------------------------------------------
%% @doc
%% Provides subset of collection's statistics.
%% @end
%%--------------------------------------------------------------------
-spec get_stats(file_id:file_guid(), dir_stats_collection:stats_selector()) ->
    {ok, current_stats()} | dir_stats_collector:error().
get_stats(Guid, StatNames) ->
    dir_stats_collector:get_stats(Guid, ?MODULE, StatNames).


%%--------------------------------------------------------------------
%% @doc
%% Returns statistics collection (actual values of statistics) together with time series collection
%% (mean values of statistics in chosen time windows).
%% @end
%%--------------------------------------------------------------------
-spec get_stats_and_time_series_collection(file_id:file_guid()) ->
    {ok, {current_stats(), internal_stats()}} |
    dir_stats_collector:collecting_status_error() | ?ERROR_INTERNAL_SERVER_ERROR.
get_stats_and_time_series_collection(Guid) ->
    case get_stats_and_time_series_collection(Guid, ?COMPLETE_LAYOUT, #{}) of
        {ok, Slice} ->
            {ok, {internal_stats_to_current_stats(Slice), internal_stats_to_time_stats(Slice)}};
        {error, not_found} ->
            {ok, {gen_empty_current_stats(Guid), gen_empty_time_stats(Guid)}};
        {error, _} = Error ->
            Error
    end.


-spec get_stats_and_time_series_collection(file_id:file_guid(), time_series_collection:layout(), 
    ts_windows:list_options()) -> {ok, {current_stats(), internal_stats()}} |
    dir_stats_collector:collecting_status_error() | ?ERROR_INTERNAL_SERVER_ERROR.
get_stats_and_time_series_collection(Guid, Layout, Opts) ->
    case dir_stats_collector_config:is_collecting_active(file_id:guid_to_space_id(Guid)) of
        true ->
            case dir_stats_collector:flush_stats(Guid, ?MODULE) of
                ok ->
                    Uuid = file_id:guid_to_uuid(Guid),
                    datastore_time_series_collection:get_slice(?CTX, Uuid, Layout, Opts);
                {error, _} = Error ->
                    Error
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

-spec acquire(file_id:file_guid()) -> {dir_stats_collection:collection(), non_neg_integer()}.
acquire(Guid) ->
    Uuid = file_id:guid_to_uuid(Guid),
    SliceLayout = #{?ALL_TIME_SERIES => [?CURRENT_METRIC]},
    case datastore_time_series_collection:get_slice(?CTX, Uuid, SliceLayout, #{window_limit => 1}) of
        {ok, Slice} ->
            {internal_stats_to_current_stats(Slice), internal_stats_to_incarnation(Slice)};
        {error, not_found} ->
            {gen_empty_current_stats(Guid), 0}
    end.


-spec consolidate(dir_stats_collection:stat_name(), dir_stats_collection:stat_value(),
    dir_stats_collection:stat_value()) -> dir_stats_collection:stat_value().
consolidate(_, Value, Diff) ->
    Value + Diff.


-spec save(file_id:file_guid(), dir_stats_collection:collection(), non_neg_integer() | current) -> ok.
save(Guid, Collection, Incarnation) ->
    Uuid = file_id:guid_to_uuid(Guid),
    Timestamp = ?NOW(),
    IncarnationConsumeSpec = case Incarnation of
        current -> #{};
        _ -> #{?INCARNATION_TIME_SERIES => #{?CURRENT_METRIC => [{Timestamp, Incarnation}]}}
    end,
    StatsConsumeSpec = maps:map(fun(_StatName, Value) -> #{?ALL_METRICS => [{Timestamp, Value}]} end, Collection),
    ConsumeSpec = maps:merge(StatsConsumeSpec, IncarnationConsumeSpec),
    case datastore_time_series_collection:consume_measurements(?CTX, Uuid, ConsumeSpec) of
        ok ->
            ok;
        {error, not_found} ->
            Config = internal_stats_config(Guid),
            % NOTE: single pes process is dedicated for each guid so race resulting in
            % {error, already_exists} is impossible - match create answer to ok
            ok = datastore_time_series_collection:create(?CTX, Uuid, Config),
            save(Guid, Collection, Incarnation)
    end.


-spec delete(file_id:file_guid()) -> ok.
delete(Guid) ->
    case datastore_time_series_collection:delete(?CTX, file_id:guid_to_uuid(Guid)) of
        ok -> ok;
        ?ERROR_NOT_FOUND -> ok
    end.


-spec init_dir(file_id:file_guid()) -> dir_stats_collection:collection().
init_dir(Guid) ->
    gen_empty_current_stats(Guid).


-spec init_child(file_id:file_guid()) -> dir_stats_collection:collection().
init_child(Guid) ->
    EmptyCurrentStats = gen_empty_current_stats(Guid),
    case file_meta:get_including_deleted(file_id:guid_to_uuid(Guid)) of
        {ok, Doc} ->
            case file_meta:get_type(Doc) of
                ?DIRECTORY_TYPE ->
                    EmptyCurrentStats#{?DIR_COUNT => 1};
                _ ->
                    {FileSizes, _} = file_ctx:get_file_size_summary(file_ctx:new_by_guid(Guid)),
                    lists:foldl(fun
                        ({total, Size}, Acc) -> Acc#{?TOTAL_SIZE => Size};
                        ({StorageId, Size}, Acc) -> Acc#{?SIZE_ON_STORAGE(StorageId) => Size}
                    end, EmptyCurrentStats#{?REG_FILE_AND_LINK_COUNT => 1}, FileSizes)
            end;
        ?ERROR_NOT_FOUND ->
            EmptyCurrentStats % Race with file deletion - stats will be invalidated by next update
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
-spec internal_stats_config(file_id:file_guid()) -> time_series_collection:config().
internal_stats_config(Guid) ->
    maps_utils:generate_from_list(fun
        (?INCARNATION_TIME_SERIES) ->
            {?INCARNATION_TIME_SERIES, current_metric_composition()};
        (StatName) ->
            {StatName, maps:merge(time_stats_metric_composition(), current_metric_composition())}
    end, [?INCARNATION_TIME_SERIES | stat_names(Guid)]).


%% @private
-spec stat_names(file_id:file_guid()) -> [dir_stats_collection:stat_name()].
stat_names(Guid) ->
    {ok, StorageId} = space_logic:get_local_supporting_storage(file_id:guid_to_space_id(Guid)),
    [?REG_FILE_AND_LINK_COUNT, ?DIR_COUNT, ?TOTAL_SIZE, ?SIZE_ON_STORAGE(StorageId)].


%% @private
-spec current_metric_composition() -> time_series:metric_composition().
current_metric_composition() ->
    #{
        ?CURRENT_METRIC => #metric_config{
            resolution = 1,
            retention = 1,
            aggregator = last
        }
    }.


%% @private
-spec time_stats_metric_composition() -> time_series:metric_composition().
time_stats_metric_composition() ->
    #{
        ?MINUTE_METRIC => #metric_config{
            resolution = ?MINUTE_RESOLUTION,
            retention = 720,
            aggregator = last
        },
        ?HOUR_METRIC => #metric_config{
            resolution = ?HOUR_RESOLUTION,
            retention = 1440,
            aggregator = last
        },
        ?DAY_METRIC => #metric_config{
            resolution = ?DAY_RESOLUTION,
            retention = 550,
            aggregator = last
        },
        ?MONTH_METRIC => #metric_config{
            resolution = ?MONTH_RESOLUTION,
            retention = 360,
            aggregator = last
        }
    }.


%% @private
-spec internal_stats_to_current_stats(internal_stats()) -> current_stats().
internal_stats_to_current_stats(InternalStats) ->
    maps:map(fun(_TimeSeriesName, #{?CURRENT_METRIC := Windows}) ->
        case Windows of
            [{_Timestamp, Value}] -> Value;
            [] -> 0
        end
    end, maps:without([?INCARNATION_TIME_SERIES], InternalStats)).


%% @private
-spec internal_stats_to_time_stats(internal_stats()) -> time_stats().
internal_stats_to_time_stats(InternalStats) ->
    maps:map(fun(_TimeSeriesName, WindowsPerMetric) ->
        maps:without([?CURRENT_METRIC], WindowsPerMetric)
    end, maps:without([?INCARNATION_TIME_SERIES], InternalStats)).


%% @private
-spec internal_stats_to_incarnation(internal_stats()) -> non_neg_integer().
internal_stats_to_incarnation(#{?INCARNATION_TIME_SERIES := #{?CURRENT_METRIC := []}}) -> 0;
internal_stats_to_incarnation(#{?INCARNATION_TIME_SERIES := #{?CURRENT_METRIC := [{_Timestamp, Value}]}}) -> Value.


%% @private
-spec gen_empty_current_stats(file_id:file_guid()) -> current_stats().
gen_empty_current_stats(Guid) ->
    maps_utils:generate_from_list(fun(StatName) -> {StatName, 0} end, stat_names(Guid)).


%% @private
-spec gen_empty_time_stats(file_id:file_guid()) -> time_stats().
gen_empty_time_stats(Guid) ->
    MetricNames = maps:keys(time_stats_metric_composition()),
    maps_utils:generate_from_list(fun(TimeSeriesName) ->
        {TimeSeriesName, maps_utils:generate_from_list(fun(MetricName) ->
            {MetricName, []}
        end, MetricNames)}
    end, stat_names(Guid)).
