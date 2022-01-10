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
%%% @end
%%%-------------------------------------------------------------------
-module(archive_recall).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("cluster_worker/include/modules/datastore/ts_metric_config.hrl").


%% API
-export([create/4, delete/1]).
-export([report_started/1, report_bytes_copied/2, report_file_finished/1]).
-export([get_details/1, get_stats/1]).
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

-define(BYTES_TS, <<"bytes">>).
-define(FILES_TS, <<"files">>).
-define(TSC_ID(Id), <<Id/binary, "tsc">>).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create(id(), file_id:file_guid(), file_meta:name(), archive_stats:record()) -> 
    ok | {error, term()}.
create(Id, TargetParentGuid, Name, Stats) ->
    case create_tsc(Id) of
        ok -> case create_doc(Id, TargetParentGuid, Name, Stats) of
            ok -> ok;
            Error ->
                delete(Id),
                Error
        end
    end.


-spec delete(id()) -> ok | {error, term()}.
delete(Id) ->
    datastore_time_series_collection:delete(?SYNC_CTX, ?TSC_ID(Id)),
    datastore_model:delete(?SYNC_CTX, Id).


report_started(Id) ->
    ?extract_ok(datastore_model:update(?SYNC_CTX, Id, fun(ArchiveRecall) ->
        {ok, ArchiveRecall#archive_recall{start_timestamp = global_clock:timestamp_millis()}}
    end)).


-spec report_file_finished(id()) -> ok | {error, term()}.
report_file_finished(Id) ->
    datastore_time_series_collection:update(?CTX, ?TSC_ID(Id), global_clock:timestamp_millis(), [
        {?FILES_TS, 1}
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_tsc(id()) -> ok | {error, term()}.
create_tsc(Id) ->
    Config = #{
        ?BYTES_TS => supported_metrics(),
        ?FILES_TS => supported_metrics()
    },
    case datastore_time_series_collection:create(?CTX, ?TSC_ID(Id), Config) of
        ok -> ok;
        {error, collection_already_exists} -> ok;
        Error -> Error
    end.

% fixme specs
create_doc(Id, TargetParentGuid, Name, Stats) ->
    ?extract_ok(datastore_model:create(?SYNC_CTX, #document{
        key = Id,
        scope = file_id:guid_to_space_id(TargetParentGuid),
        value = #archive_recall{
            target_guid = TargetParentGuid,
            name = Name,
            total_bytes = archive_stats:get_archived_bytes(Stats),
            total_files = archive_stats:get_archived_files(Stats)
        }}
    )).

-spec supported_metrics() -> #{ts_metric:id() => ts_metric:config()}.
supported_metrics() -> #{
    <<"minute">> => #metric_config{
        resolution = timer:minutes(1),
        retention = 120,
        aggregator = sum
    },
    <<"hour">> => #metric_config{
        resolution = timer:hours(1),
        retention = 48,
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
        {target_guid, string},
        {name, binary},
        {timestamp, integer},
        {total_files, integer},
        {total_bytes, integer}
    ]}.
