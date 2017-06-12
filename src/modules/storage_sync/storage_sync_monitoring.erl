%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% This module is responsible for reporting metrics
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_monitoring).
-author("Jakub Kudzia").

-include_lib("ctool/include/logging.hrl").


%% API
-export([ start_reporter/0, delete_reporter/0]).
-export([start_imported_files_counter/2, increase_imported_files_counter/2,
    stop_imported_files_counter/2, start_files_to_import_counter/2,
    stop_files_to_import_counter/2, update_files_to_import_counter/3,
    get_files_to_import_value/2, get_imported_files_value/2]).

-type type() :: imported_files | files_to_import.
-type error() :: {error, term()}.


-define(COUNTER_NAME(SpaceId, StorageId, Type), [
    storage_import, binary_to_atom(SpaceId, latin1),
    binary_to_atom(StorageId, latin1), Type
]).
-define(COUNTER_LOGGING_INTERVAL, timer:seconds(60)).

-define(REPORTER_NAME, exometer_report_lager).
-define(IMPORTED_FILES, imported_files).
-define(FILES_TO_IMPORT, files_to_import).
-define(LOG_LEVEL, info).

%%%-------------------------------------------------------------------
%%% @doc
%%% Starts space_sync_monitoring_reporter
%%% @end
%%%-------------------------------------------------------------------
-spec start_reporter() -> ok | error().
start_reporter() ->
    exometer_report:add_reporter(?REPORTER_NAME, [
        {type_map,[{'_',integer}]},
        {level, ?LOG_LEVEL}
    ]).

%%%-------------------------------------------------------------------
%%% @doc
%%% Deletes space_sync_monitoring_reporter
%%% @end
%%%-------------------------------------------------------------------
-spec delete_reporter() -> ok | error().
delete_reporter() ->
    exometer_report:remove_reporter(?REPORTER_NAME).

%%%-------------------------------------------------------------------
%%% @doc
%%% Starts counter of imported files.
%%% @end
%%%-------------------------------------------------------------------
-spec start_imported_files_counter(od_space:id(), storage:id()) -> ok.
start_imported_files_counter(SpaceId, StorageId) ->
    start_and_subscribe_storage_import_counter(SpaceId, StorageId, ?IMPORTED_FILES).

%%%-------------------------------------------------------------------
%%% @doc
%%% Starts counter of files to be imported
%%% @end
%%%-------------------------------------------------------------------
-spec start_files_to_import_counter(od_space:id(), storage:id()) -> ok.
start_files_to_import_counter(SpaceId, StorageId) ->
    start_and_subscribe_storage_import_counter(SpaceId, StorageId, ?FILES_TO_IMPORT).

%%%-------------------------------------------------------------------
%%% @doc
%%% Stops counter of imported files
%%% @end
%%%-------------------------------------------------------------------
-spec stop_imported_files_counter(od_space:id(), storage:id()) -> ok | {error, term()}.
stop_imported_files_counter(SpaceId, StorageId) ->
    stop_and_unsubscribe_storage_import_counter(SpaceId, StorageId, ?IMPORTED_FILES).

%%%-------------------------------------------------------------------
%%% @doc
%%% Stops counter of files to be imported
%%% @end
%%%-------------------------------------------------------------------
-spec stop_files_to_import_counter(od_space:id(), storage:id())
        -> ok | {error, term()}.
stop_files_to_import_counter(SpaceId, StorageId) ->
    stop_and_unsubscribe_storage_import_counter(SpaceId, StorageId, ?FILES_TO_IMPORT).


%%%-------------------------------------------------------------------
%%% @doc
%%% Increases counter of imported files
%%% @end
%%%-------------------------------------------------------------------
-spec increase_imported_files_counter(od_space:id(), storage:id())
        -> ok | {error, term()}.
increase_imported_files_counter(SpaceId, StorageId) ->
    update_counter(SpaceId, StorageId, ?IMPORTED_FILES, 1).

%%%-------------------------------------------------------------------
%%% @doc
%%% Updates counter of files to be imported with given Value.
%%% Value can be negative.
%%% @end
%%%-------------------------------------------------------------------
-spec update_files_to_import_counter(od_space:id(), storage:id(), integer())
        -> ok | error().
update_files_to_import_counter(SpaceId, StorageId, Value) ->
    update_counter(SpaceId, StorageId, ?FILES_TO_IMPORT, Value).


%%%-------------------------------------------------------------------
%%% @doc
%%% Returns values of files to be imported counter
%%% @end
%%%-------------------------------------------------------------------
-spec get_files_to_import_value(od_space:id(), storage:id()) -> integer().
get_files_to_import_value(SpaceId, StorageId) ->
    get_value(SpaceId, StorageId, files_to_import).


%%%-------------------------------------------------------------------
%%% @doc
%%% Returns values of imported files counter
%%% @end
%%%-------------------------------------------------------------------
-spec get_imported_files_value(od_space:id(), storage:id()) -> integer().
get_imported_files_value(SpaceId, StorageId) ->
    get_value(SpaceId, StorageId, imported_files).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Starts and subscribes to given type of counter.
%%% @end
%%%-------------------------------------------------------------------
-spec start_and_subscribe_storage_import_counter(od_space:id(), storage:id(),
    type()) -> ok | error().
start_and_subscribe_storage_import_counter(SpaceId, StorageId, CounterType) ->
    CounterName = ?COUNTER_NAME(SpaceId, StorageId, CounterType),
    ok = exometer:new(CounterName, counter),
    ok = exometer_report:subscribe(?REPORTER_NAME, CounterName,[value], ?COUNTER_LOGGING_INTERVAL).

%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Stops and unsubscribes counter of given type.
%%% @end
%%%-------------------------------------------------------------------
-spec stop_and_unsubscribe_storage_import_counter(od_space:id(), storage:id(),
    type()) -> ok | {error, term()}.
stop_and_unsubscribe_storage_import_counter(SpaceId, StorageId, CounterType) ->
    CounterName = ?COUNTER_NAME(SpaceId, StorageId, CounterType),
    exometer_report:unsubscribe(?REPORTER_NAME, CounterName, value),
    exometer:delete(CounterName).

%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Updates given counter with given Value.
%%% @end
%%%-------------------------------------------------------------------
-spec update_counter(od_space:id(), storage:id(), type(), integer())
        -> ok | error().
update_counter(SpaceId, StorageId, CounterType, Value) ->
    CounterName = ?COUNTER_NAME(SpaceId, StorageId, CounterType),
    exometer:update(CounterName, Value).


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Gets value of given counter.
%%% @end
%%%-------------------------------------------------------------------
-spec get_value(od_space:id(), storage:id(), type()) -> integer().
get_value(SpaceId, StorageId, CounterType) ->
    {ok, [{value, Value}]} = exometer:get_value(
        ?COUNTER_NAME(SpaceId, StorageId, CounterType), [value]),
    Value.


