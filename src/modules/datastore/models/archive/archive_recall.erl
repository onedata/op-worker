%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for storing information about archive recalls.
%%% It uses time_series_collection structure for storing statistics.
%%% For each recall there are 3 time series:
%%%     * currentBytes - stores sum of copied bytes
%%%     * currentFiles - stores number of copied files 
%%%     * failedFiles - stores number of unsuccessfully recalled files
%%% Recall statistics are only kept locally on provider that is 
%%% performing recall.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_recall).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/file_details.hrl/").
-include_lib("cluster_worker/include/modules/datastore/infinite_log.hrl").
-include_lib("cluster_worker/include/modules/datastore/ts_metric_config.hrl").


%% API
-export([create/2, delete/1]).
-export([report_started/1, report_finished/2, 
    report_bytes_copied/2, report_file_finished/1, report_file_failed/3]).
-export([get_details/1, get_stats/1, get_progress/1]).
-export([get_effective_recall/1]).
%% Datastore callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-type id() :: file_meta:uuid().
-type record() :: #archive_recall{}.
-export_type([id/0, record/0]).



-define(CTX, #{
    model => ?MODULE
}).

-define(SYNC_CTX, ?CTX#{
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

-define(BYTES_TS, <<"currentBytes">>).
-define(FILES_TS, <<"currentFiles">>).
-define(FAILED_FILES_TS, <<"failedFiles">>).
-define(TSC_ID(Id), <<Id/binary, "tsc">>).

-define(ERROR_LOG_ID(Id), <<Id/binary, "el">>).

-define(TOTAL_METRIC, <<"total">>).
-define(MINUTE_METRIC, <<"minute">>).
-define(HOUR_METRIC, <<"hour">>).
-define(DAY_METRIC, <<"day">>).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create(id(), archive:doc()) -> ok | {error, term()}.
create(Id, ArchiveDoc) ->
    try
        ok = create_tsc(Id),
        {ok, _} = json_infinite_log_model:create(#{id => ?ERROR_LOG_ID(Id)}),
        ok = create_doc(Id, ArchiveDoc),
        {ok, SpaceId} = archive:get_space_id(ArchiveDoc),
        ok = archive_recall_cache:invalidate_on_all_nodes(SpaceId)
    catch _:{badmatch, Error} ->
        delete(Id),
        Error
    end.


-spec delete(id()) -> ok | {error, term()}.
delete(Id) ->
    % no need to invalidate cache, as whole subtree has been already deleted
    datastore_time_series_collection:delete(?CTX, ?TSC_ID(Id)),
    json_infinite_log_model:destroy(?ERROR_LOG_ID(Id)),
    case get_details(Id) of
        {ok, #archive_recall{source_archive = ArchiveId}} ->
            archive:report_recall_removed(ArchiveId, Id),
            datastore_model:delete(?SYNC_CTX, Id);
        {error, not_found} -> 
            ok;
        Error -> 
            Error
    end.


-spec report_started(id()) -> ok | {error, term()}.
report_started(Id) ->
    ?extract_ok(datastore_model:update(?SYNC_CTX, Id, fun(ArchiveRecall) ->
        {ok, ArchiveRecall#archive_recall{start_timestamp = global_clock:timestamp_millis()}}
    end)).


-spec report_finished(id(), od_space:id()) -> ok | {error, term()}.
report_finished(Id, SpaceId) ->
    ok = archive_recall_cache:invalidate_on_all_nodes(SpaceId),
    ?extract_ok(datastore_model:update(?SYNC_CTX, Id, fun(ArchiveRecall) ->
        {ok, ArchiveRecall#archive_recall{finish_timestamp = global_clock:timestamp_millis()}}
    end)).


-spec report_file_finished(id()) -> ok | {error, term()}.
report_file_finished(Id) ->
    datastore_time_series_collection:update(?CTX, ?TSC_ID(Id), global_clock:timestamp_millis(), [
        {?FILES_TS, 1}
    ]).


-spec report_file_failed(id(), file_id:file_guid(), {error, term()}) -> ok | {error, term()}.
report_file_failed(Id, FileGuid, Error) ->
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    json_infinite_log_model:append(?ERROR_LOG_ID(Id), #{
        <<"fileId">> => ObjectId,
        <<"reason">> => errors:to_json(Error)
    }),
    datastore_time_series_collection:update(?CTX, ?TSC_ID(Id), global_clock:timestamp_millis(), [
        {?FAILED_FILES_TS, 1}
    ]).



-spec report_bytes_copied(id(), non_neg_integer()) -> ok | {error, term()}.
report_bytes_copied(Id, Bytes) ->
    datastore_time_series_collection:update(?CTX, ?TSC_ID(Id), global_clock:timestamp_millis(), [
        {?BYTES_TS, Bytes}
    ]).


-spec get_details(id()) -> {ok, record()} | {error, term()}.
get_details(Id) ->
    case datastore_model:get(?SYNC_CTX, Id) of
        {ok, #document{value = Record}} ->
            {ok, Record};
        Error ->
            Error
    end.


-spec get_stats(id()) -> time_series_collection:windows_map() | {error, term()}.
get_stats(Id) ->
    datastore_time_series_collection:list_windows(?CTX, ?TSC_ID(Id), #{}).


get_progress(Id) ->
    {ok, CountersCurrentValue} = get_counters_current_value(Id),
    case CountersCurrentValue of
        #{?FAILED_FILES_TS := 0} -> 
            {ok, CountersCurrentValue#{<<"lastError">> => undefined}};
        _ ->
            %% @TODO VFS-8839 - browse error log when gui supports it
            {ok, #{
                <<"logEntries">> := [#{
                    <<"content">> := LastEntryContent
                }]
            }} = json_infinite_log_model:browse_content(?ERROR_LOG_ID(Id), 
                #{limit => 1, direction => ?BACKWARD}),
            {ok, CountersCurrentValue#{<<"lastError">> => LastEntryContent}}
    end.


-spec get_effective_recall(file_meta:doc()) -> {ok, id() | undefined} | {error, term()}.
get_effective_recall(#document{scope = SpaceId, key = Id} = FileMetaDoc) ->
    case archive_recall_cache:get(SpaceId, FileMetaDoc) of
        {ok, {finished, Id}} -> {ok, Id};
        {ok, {finished, _}} -> {ok, undefined};
        {ok, {ongoing, AncestorId}} -> {ok, AncestorId};
        Other -> Other
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec create_tsc(id()) -> ok | {error, term()}.
create_tsc(Id) ->
    TotalMetric = #{
        ?TOTAL_METRIC => #metric_config{
            resolution = 0,
            retention = 1,
            aggregator = sum
        }
    },
    Config = #{
        ?BYTES_TS => maps:merge(TotalMetric, supported_metrics()),
        ?FILES_TS => maps:merge(TotalMetric, supported_metrics()),
        ?FAILED_FILES_TS => TotalMetric
    },
    case datastore_time_series_collection:create(?CTX, ?TSC_ID(Id), Config) of
        ok -> ok;
        {error, collection_already_exists} -> ok;
        Error -> Error
    end.


%% @private
-spec create_doc(id(), archive:doc()) -> ok | {error, term()}.
create_doc(Id, ArchiveDoc) ->
    {ok, SpaceId} = archive:get_space_id(ArchiveDoc),
    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
    {ok, DatasetId} = archive:get_dataset_id(ArchiveDoc),
    {ok, Stats} = archive_api:get_aggregated_stats(ArchiveDoc),
    ?extract_ok(datastore_model:create(?SYNC_CTX, #document{
        key = Id,
        scope = SpaceId,
        value = #archive_recall{
            source_archive = ArchiveId,
            source_dataset = DatasetId,
            target_bytes = archive_stats:get_archived_bytes(Stats),
            target_files = archive_stats:get_archived_files(Stats)
        }}
    )).


%% @private
-spec supported_metrics() -> #{ts_metric:id() => ts_metric:config()}.
supported_metrics() -> #{
    ?MINUTE_METRIC => #metric_config{
        resolution = timer:minutes(1),
        retention = 120,
        aggregator = sum
    },
    ?HOUR_METRIC => #metric_config{
        resolution = timer:hours(1),
        retention = 48,
        aggregator = sum
    },
    ?DAY_METRIC => #metric_config{
        resolution = timer:hours(24),
        retention = 60,
        aggregator = sum
    }
}.


%% @private
-spec get_counters_current_value(id()) -> {ok, map()}.
get_counters_current_value(Id) ->
    RequestRange = lists:map(fun(Parameter) -> {Parameter, ?TOTAL_METRIC} end, [?BYTES_TS, ?FILES_TS, ?FAILED_FILES_TS]),
    
    case datastore_time_series_collection:list_windows(?CTX, ?TSC_ID(Id), RequestRange, #{limit => 1}) of
        {ok, WindowsMap} ->
            WindowToValue = fun
                ({{Parameter, ?TOTAL_METRIC}, [{_Timestamp, {_Measurements, Value}}]}) -> {Parameter, Value};
                ({{Parameter, ?TOTAL_METRIC}, []}) -> {Parameter, 0}
            end,
            {ok, maps:from_list(lists:map(WindowToValue, maps:to_list(WindowsMap)))};
        Error ->
            Error
    end.


%%%===================================================================
%%% Datastore callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?SYNC_CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.

-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {source_archive, string},
        {source_dataset, string},
        {start_timestamp, integer},
        {finish_timestamp, integer},
        {target_files, integer},
        {target_bytes, integer}
    ]}.
