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
%%% For each recall there are 2 time series:
%%%     * bytes - stores sum of copied bytes
%%%     * files - stores number of copied files
%%% Recall statistics are only kept locally on provider that is 
%%% performing recall.
% fixme 
%%% @end
%%%-------------------------------------------------------------------
-module(archive_recall).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/file_details.hrl/").
-include_lib("cluster_worker/include/modules/datastore/ts_metric_config.hrl").


%% API
-export([create/2, delete/1]).
-export([report_started/2, report_finished/2, report_bytes_copied/2, report_file_finished/1]).
-export([get_details/1, get_stats/1, get_progress/1]).
-export([get_effective_recall/1]).
%% Datastore callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-type id() :: datastore_key:key().
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
-define(TSC_ID(Id), <<Id/binary, "tsc">>).

-define(TOTAL_METRIC, <<"total">>).
-define(MINUTE_METRIC, <<"minute">>).
-define(HOUR_METRIC, <<"hour">>).
-define(DAY_METRIC, <<"day">>).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create(id(), archive:doc()) -> ok | {error, term()}.
create(Id, ArchiveDoc) ->
%%    fixme create infinite_log
    case create_tsc(Id) of
        ok -> case create_doc(Id, ArchiveDoc) of
            ok -> ok;
            Error ->
                delete(Id),
                Error
        end
    end.


-spec delete(id()) -> ok | {error, term()}.
delete(Id) ->
    % fixme remove from archive record
    datastore_time_series_collection:delete(?SYNC_CTX, ?TSC_ID(Id)),
    datastore_model:delete(?SYNC_CTX, Id).


report_started(Id, SpaceId) ->
    ok = archive_recall_cache:invalidate_on_all_nodes(SpaceId),
    ?extract_ok(datastore_model:update(?SYNC_CTX, Id, fun(ArchiveRecall) ->
        {ok, ArchiveRecall#archive_recall{start_timestamp = global_clock:timestamp_millis()}}
    end)).


-spec report_file_finished(id()) -> ok | {error, term()}.
report_file_finished(Id) ->
    datastore_time_series_collection:update(?CTX, ?TSC_ID(Id), global_clock:timestamp_millis(), [
        {?FILES_TS, 1}
    ]).


report_finished(Id, SpaceId) ->
    ok = archive_recall_cache:invalidate_on_all_nodes(SpaceId),
    ?extract_ok(datastore_model:update(?SYNC_CTX, Id, fun(ArchiveRecall) ->
        {ok, ArchiveRecall#archive_recall{finish_timestamp = global_clock:timestamp_millis()}}
    end)).



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
    RequestRange = lists:map(fun(Parameter) -> {Parameter, ?TOTAL_METRIC} end, [?BYTES_TS, ?FILES_TS]),
    
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


% fixme spec
get_effective_recall(#document{scope = SpaceId, key = Id} = Doc) ->
    case archive_recall_cache:get(SpaceId, Doc) of
        {ok, {finished, Id}} -> {ok, Id};
        {ok, {finished, _}} -> {ok, undefined};
        {ok, {ongoing, AncestorId}} -> {ok, AncestorId};
        Other -> Other
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_tsc(id()) -> ok | {error, term()}.
create_tsc(Id) ->
    Config = #{
        % fixme failed files
        ?BYTES_TS => supported_metrics(),
        ?FILES_TS => supported_metrics()
    },
    case datastore_time_series_collection:create(?CTX, ?TSC_ID(Id), Config) of
        ok -> ok;
        {error, collection_already_exists} -> ok;
        Error -> Error
    end.

% fixme specs
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

-spec supported_metrics() -> #{ts_metric:id() => ts_metric:config()}.
supported_metrics() -> #{
    ?TOTAL_METRIC => #metric_config{
        resolution = 0,
        retention = 1,
        aggregator = sum
    },
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
        {failed_files, integer}, % fixme keep in tsc
        {target_files, integer},
        {target_bytes, integer}
    ]}.
