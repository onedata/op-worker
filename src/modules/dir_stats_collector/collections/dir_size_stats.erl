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
%%%   * historical_stats() - time series collection slice showing the changes of stats in time.
%%% Internally, both collections are kept in the same underlying persistent
%%% time series collection - internal_stats(). The current statistics are stored
%%% in the special ?CURRENT_METRIC. Additionally, the internal_stats() hold dir stats
%%% incarnation info in a separate time series. The internal_stats() are properly
%%% trimmed into current_stats() and/or historical_stats() when these collections are retrieved.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(dir_size_stats).
-author("Michal Wrzeszcz").


-behavior(dir_stats_collection_behaviour).


-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_time_series.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").
-include_lib("ctool/include/time_series/common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([
    get_stats/1, get_stats/2, 
    browse_historical_stats_collection/2,
    report_reg_file_size_changed/3,
    report_file_created/2, report_file_deleted/2, report_remote_links_change/2,
    delete_stats/1]).

%% dir_stats_collection_behaviour callbacks
-export([
    acquire/1, consolidate/3, on_collection_move/2, save/3, delete/1, init_dir/1, init_child/2,
    encode/1, decode/1
]).

%% datastore_model callbacks
-export([get_ctx/0]).


-type ctx() :: datastore:ctx().

%% see the module doc
-type current_stats() :: dir_stats_collection:collection().
-type historical_stats() :: time_series_collection:slice().
-type internal_stats() :: time_series_collection:slice().

-export_type([current_stats/0]).

-define(CTX, #{
    model => ?MODULE
}).

-define(NOW(), global_clock:timestamp_seconds()).
% Metric storing current value of statistic or incarnation (depending on time series)
-define(CURRENT_METRIC, <<"current">>).
% Time series storing incarnation - historical values are not required
% but usage of time series allows keeping everything in single structure
-define(INCARNATION_TIME_SERIES, <<"incarnation">>).

-define(ERROR_HANDLING_MODE, op_worker:get_env(dir_size_stats_init_errors_handling_mode, repeat)).

%%%===================================================================
%%% API
%%%===================================================================

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


-spec browse_historical_stats_collection(file_id:file_guid(), ts_browse_request:record()) -> 
    {ok, ts_browse_result:record()} | dir_stats_collector:collecting_status_error() | ?ERROR_INTERNAL_SERVER_ERROR.
browse_historical_stats_collection(Guid, BrowseRequest) ->
    case dir_stats_service_state:is_active(file_id:guid_to_space_id(Guid)) of
        true ->
            case dir_stats_collector:flush_stats(Guid, ?MODULE) of
                ok ->
                    Uuid = file_id:guid_to_uuid(Guid),
                    case datastore_time_series_collection:browse(?CTX, Uuid, BrowseRequest) of
                        {ok, BrowseResult} -> {ok, internal_to_historical_stats_browse_result(BrowseResult)};
                        {error, not_found} -> {ok, gen_empty_historical_stats_browse_result(BrowseRequest, Guid)};
                        {error, _} = Error2 -> Error2
                    end;
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


-spec report_remote_links_change(file_meta:uuid(), od_space:id()) -> ok.
report_remote_links_change(Uuid, SpaceId) ->
    % Check is uuid is dir space uuid to prevent its creation by file_meta:get_including_deleted/1
    case fslogic_file_id:is_space_dir_uuid(Uuid) of
        true ->
            % Send empty update to prevent race between links sync and initialization
            update_stats(file_id:pack_guid(Uuid, SpaceId), #{});
        false ->
            case file_meta:get_including_deleted(Uuid) of
                {ok, Doc} ->
                    case file_meta:get_type(Doc) of
                        ?DIRECTORY_TYPE ->
                            % Send empty update to prevent race between links sync and initialization
                            update_stats(file_id:pack_guid(Uuid, SpaceId), #{});
                        _ ->
                            ok
                    end;
                ?ERROR_NOT_FOUND ->
                    ok
            end
    end.


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


-spec on_collection_move(dir_stats_collection:stat_name(), dir_stats_collection:stat_value()) ->
    {update_source_parent, dir_stats_collection:stat_value()}.
on_collection_move(_, Value) ->
    {update_source_parent, -Value}.



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


-spec init_child(file_id:file_guid(), boolean()) -> dir_stats_collection:collection().
init_child(Guid, IncludeDeleted) ->
    case file_meta:get_including_deleted(file_id:guid_to_uuid(Guid)) of
        {ok, Doc} ->
            case file_meta:is_deleted(Doc) andalso not IncludeDeleted of
                true ->
                    % Race with file deletion - stats will be invalidated by next update
                    gen_empty_current_stats_and_handle_errors(Guid);
                false ->
                    init_existing_child(Guid, Doc)
            end;
        ?ERROR_NOT_FOUND ->
            % Race with file deletion - stats will be invalidated by next update
            gen_empty_current_stats_and_handle_errors(Guid)
    end.


-spec encode(dir_stats_collection:collection()) -> term().
encode(Collection) ->
    maps:fold(fun(StatName, Values, Acc) ->
        Acc#{encode_stat_name(StatName) => Values}
    end, #{}, Collection).

-spec decode(term()) -> dir_stats_collection:collection().
decode(EncodedCollection) ->
    maps:fold(fun(StatName, Values, Acc) ->
        Acc#{decode_stat_name(StatName) => Values}
    end, #{}, EncodedCollection).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_existing_child(file_id:file_guid(), file_meta:doc()) -> dir_stats_collection:collection().
init_existing_child(Guid, Doc) ->
    case file_meta:get_type(Doc) of
        ?DIRECTORY_TYPE ->
            try
                EmptyCurrentStats = gen_empty_current_stats(Guid), % TODO VFS-9204 - maybe refactor as gen_empty_current_stats
                % gets storage_id that is also used by prepare_file_size_summary
                EmptyCurrentStats#{?DIR_COUNT => 1}
            catch
                Error:Reason:Stacktrace ->
                    handle_init_error(Guid, Error, Reason, Stacktrace),
                    #{?DIR_COUNT => 1, ?DIR_ERRORS_COUNT => 1}
            end;
        Type ->
            try
                EmptyCurrentStats = gen_empty_current_stats(Guid),

                case Type of
                    ?REGULAR_FILE_TYPE ->
                        % gets storage_id that is also used by prepare_file_size_summary
                        FileCtx = file_ctx:new_by_guid(Guid),
                        {FileSizes, _} = try
                            file_ctx:prepare_file_size_summary(FileCtx)
                        catch
                            throw:{error, {file_meta_missing, _}} ->
                                % It is impossible to create file_location because of missing ancestor's file_meta.
                                % Sizes will be counted on location creation.
                                {[], FileCtx}
                        end,
                        lists:foldl(fun
                            ({total, Size}, Acc) -> Acc#{?TOTAL_SIZE => Size};
                            ({StorageId, Size}, Acc) -> Acc#{?SIZE_ON_STORAGE(StorageId) => Size}
                        end, EmptyCurrentStats#{?REG_FILE_AND_LINK_COUNT => 1}, FileSizes);
                    _ ->
                        % Links are counted with size 0
                        EmptyCurrentStats#{?REG_FILE_AND_LINK_COUNT => 1}
                end
            catch
                Error:Reason:Stacktrace ->
                    handle_init_error(Guid, Error, Reason, Stacktrace),
                    #{?REG_FILE_AND_LINK_COUNT => 1, ?FILE_ERRORS_COUNT => 1}
            end
    end.


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
            {StatName, maps:merge(?DIR_SIZE_STATS_METRICS, current_metric_composition())}
    end, [?INCARNATION_TIME_SERIES | stat_names(Guid)]).


%% @private
-spec stat_names(file_id:file_guid()) -> [dir_stats_collection:stat_name()].
stat_names(Guid) ->
    {ok, StorageId} = space_logic:get_local_supporting_storage(file_id:guid_to_space_id(Guid)),
    [?REG_FILE_AND_LINK_COUNT, ?DIR_COUNT, ?FILE_ERRORS_COUNT, ?DIR_ERRORS_COUNT, ?TOTAL_SIZE, ?SIZE_ON_STORAGE(StorageId)].


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
-spec gen_default_historical_stats_layout(file_id:file_guid()) -> time_series_collection:layout().
gen_default_historical_stats_layout(Guid) ->
    MetricNames = maps:keys(?DIR_SIZE_STATS_METRICS),
    maps_utils:generate_from_list(fun(TimeSeriesName) -> {TimeSeriesName, MetricNames} end, stat_names(Guid)).


%% @private
-spec internal_stats_to_current_stats(internal_stats()) -> current_stats().
internal_stats_to_current_stats(InternalStats) ->
    maps:map(fun(_TimeSeriesName, #{?CURRENT_METRIC := Windows}) ->
        case Windows of
            [#window_info{value = Value}] -> Value;
            [] -> 0
        end
    end, maps:without([?INCARNATION_TIME_SERIES], InternalStats)).


%% @private
-spec internal_stats_to_historical_stats(internal_stats()) -> historical_stats().
internal_stats_to_historical_stats(InternalStats) ->
    maps:map(fun(_TimeSeriesName, WindowsPerMetric) ->
        maps:without([?CURRENT_METRIC], WindowsPerMetric)
    end, maps:without([?INCARNATION_TIME_SERIES], InternalStats)).


%% @private
-spec internal_layout_to_historical_stats_layout(time_series_collection:layout()) -> 
    time_series_collection:layout().
internal_layout_to_historical_stats_layout(InternalLayout) ->
    maps:map(fun(_TimeSeriesName, Metrics) ->
        lists:delete(?CURRENT_METRIC, Metrics)
    end, maps:without([?INCARNATION_TIME_SERIES], InternalLayout)).


%% @private
-spec internal_stats_to_incarnation(internal_stats()) -> non_neg_integer().
internal_stats_to_incarnation(#{?INCARNATION_TIME_SERIES := #{?CURRENT_METRIC := []}}) -> 0;
internal_stats_to_incarnation(#{?INCARNATION_TIME_SERIES := #{?CURRENT_METRIC := [#window_info{value = Value}]}}) -> Value.


%% @private
-spec internal_to_historical_stats_browse_result(ts_browse_result:record()) -> ts_browse_result:record().
internal_to_historical_stats_browse_result(#time_series_layout_get_result{layout = InternalLayout}) ->
    #time_series_layout_get_result{layout = internal_layout_to_historical_stats_layout(InternalLayout)};
internal_to_historical_stats_browse_result(#time_series_slice_get_result{slice = InternalStats}) ->
    #time_series_slice_get_result{slice = internal_stats_to_historical_stats(InternalStats)}.


%% @private
-spec gen_empty_historical_stats_browse_result(ts_browse_request:record(), file_id:file_guid()) ->
    ts_browse_result:record().
gen_empty_historical_stats_browse_result(#time_series_layout_get_request{}, Guid) ->
    #time_series_layout_get_result{layout = gen_default_historical_stats_layout(Guid)};
gen_empty_historical_stats_browse_result(#time_series_slice_get_request{}, Guid) ->
    #time_series_slice_get_result{slice = gen_empty_historical_stats(Guid)}.


%% @private
-spec gen_empty_current_stats(file_id:file_guid()) -> current_stats().
gen_empty_current_stats(Guid) ->
    maps_utils:generate_from_list(fun(StatName) -> {StatName, 0} end, stat_names(Guid)).


%% @private
-spec gen_empty_current_stats_and_handle_errors(file_id:file_guid()) -> current_stats().
gen_empty_current_stats_and_handle_errors(Guid) ->
    try
        gen_empty_current_stats(Guid)
    catch
        Error:Reason:Stacktrace ->
            handle_init_error(Guid, Error, Reason, Stacktrace),
            #{}
    end.


%% @private
-spec gen_empty_historical_stats(file_id:file_guid()) -> historical_stats().
gen_empty_historical_stats(Guid) ->
    MetricNames = maps:keys(?DIR_SIZE_STATS_METRICS),
    maps_utils:generate_from_list(fun(TimeSeriesName) ->
        {TimeSeriesName, maps_utils:generate_from_list(fun(MetricName) ->
            {MetricName, []}
        end, MetricNames)}
    end, stat_names(Guid)).


%% @private
-spec handle_init_error(file_id:file_guid(), term(), term(), list()) -> ok | no_return().
handle_init_error(Guid, Error, Reason, Stacktrace) ->
    case ?ERROR_HANDLING_MODE of
        ignore ->
            ?error_stacktrace("Error initializing size stats for ~p: ~p:~p",
                [Guid, Error, Reason], Stacktrace);
        silent_ignore ->
            ok;

        % throw to repeat init by collector
        repeat ->
            case datastore_runner:normalize_error(Reason) of
                no_connection_to_onezone ->
                    ok;
                _ ->
                    ?error_stacktrace("Error initializing size stats for ~p: ~p:~p",
                        [Guid, Error, Reason], Stacktrace)
            end,
            throw(dir_size_stats_init_error);
        silent_repeat ->
            throw(dir_size_stats_init_error);

        repeat_connection_errors ->
            case datastore_runner:normalize_error(Reason) of
                no_connection_to_onezone ->
                    % Collector handles problems with zone connection
                    throw(no_connection_to_onezone);
                _ ->
                    ?error_stacktrace("Error initializing size stats for ~p: ~p:~p",
                        [Guid, Error, Reason], Stacktrace)
            end
    end.


%% @private
-spec encode_stat_name(dir_stats_collection:stat_name()) -> non_neg_integer() | {non_neg_integer(), binary()}.
encode_stat_name(?REG_FILE_AND_LINK_COUNT) -> 0;
encode_stat_name(?DIR_COUNT) -> 1;
encode_stat_name(?FILE_ERRORS_COUNT) -> 2;
encode_stat_name(?DIR_ERRORS_COUNT) -> 3;
encode_stat_name(?TOTAL_SIZE) -> 4;
encode_stat_name(?SIZE_ON_STORAGE(StorageId)) -> {5, StorageId}.


%% @private
-spec decode_stat_name(non_neg_integer() | {non_neg_integer(), binary()}) -> dir_stats_collection:stat_name().
decode_stat_name(0) -> ?REG_FILE_AND_LINK_COUNT;
decode_stat_name(1) -> ?DIR_COUNT;
decode_stat_name(2) -> ?FILE_ERRORS_COUNT;
decode_stat_name(3) -> ?DIR_ERRORS_COUNT;
decode_stat_name(4) -> ?TOTAL_SIZE;
decode_stat_name({5, StorageId}) -> ?SIZE_ON_STORAGE(StorageId).
