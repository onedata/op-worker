%%%--------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides functions operating on file timestamps - @see times.
%%%
%%% In order to minimize number of datastore document updates (especially after
%%% atime is changed) this module utilizes cache which aggregates times changes
%%% for single file - @see times_cache.erl for more details.
%%%
%%% File creation time is set to minimum of atime, mtime and ctime when it is unknown
%%% (when created by storage import or after an upgrade for legacy files).
%%%
%%% Note: this module operates on referenced uuids - all operations on hardlinks
%%% are treated as operations on original file. Thus, all hardlinks pointing on
%%% the same file share single times document and times_cache entry.
%%% @end
%%%--------------------------------------------------------------------
-module(times_api).
-author("Michał Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    report_file_created/1, report_file_created/2,
    touch/2, report_change/2,
    get/2,
    report_file_deleted/1
]).

-type times_type() :: ?attr_atime | ?attr_ctime | ?attr_mtime | ?attr_creation_time.

-export_type([times_type/0]).

-define(NOW(), global_clock:timestamp_seconds()).

%%%===================================================================
%%% API
%%%===================================================================

-spec report_file_created(file_ctx:ctx()) -> ok.
report_file_created(FileCtx) ->
    report_file_created(FileCtx, build_current_times_record()).

-spec report_file_created(file_ctx:ctx(), times:record()) -> ok.
report_file_created(FileCtx, TimesRecord) ->
    dir_update_time_stats:report_update(FileCtx, TimesRecord),
    {IsSyncEnabled, FileCtx2} = file_ctx:is_synchronization_enabled(FileCtx),
    ?catch_exceptions(
        ok = times_cache:report_created(file_ctx:get_referenced_guid_const(FileCtx2), not IsSyncEnabled, TimesRecord)
    ).


-spec touch(file_ctx:ctx(), [times_type()]) -> ok.
touch(FileCtx, TimesToUpdate) ->
    TimesRecord = build_times_record(TimesToUpdate, ?NOW()),
    report_change(FileCtx, TimesRecord).


-spec report_change(file_ctx:ctx(), times:record()) -> ok.
report_change(FileCtx, TimesRecord) ->
    dir_update_time_stats:report_update(FileCtx, TimesRecord),
    ?catch_exceptions(
        ok = times_cache:report_changed(file_ctx:get_referenced_guid_const(FileCtx), TimesRecord)
    ).


-spec get(file_ctx:ctx(), [times_type()]) -> times:record().
get(FileCtx, RequestedTimes) ->
    case times_cache:acquire(file_ctx:get_referenced_guid_const(FileCtx), RequestedTimes) of
        {ok, Times} -> Times;
        {error, not_found} -> build_times_record(0) % documents not synchronized yet, return dummy times
    end.


-spec report_file_deleted(file_ctx:ctx()) -> ok.
report_file_deleted(FileCtx) ->
    dir_update_time_stats:report_update(FileCtx, ?NOW()),
    ?catch_exceptions(
        ok = times_cache:report_deleted(file_ctx:get_referenced_guid_const(FileCtx))
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec build_current_times_record() -> times:record().
build_current_times_record() ->
    build_times_record(?NOW()).


%% @private
-spec build_times_record(times:time()) -> times:record().
build_times_record(Time) ->
    build_times_record([?attr_creation_time, ?attr_atime, ?attr_mtime, ?attr_ctime], Time).


%% @private
-spec build_times_record([times_type()], times:time()) -> times:record().
build_times_record(TimesList, Time) ->
    build_times_record(TimesList, Time, #times{}).


%% @private
-spec build_times_record([times_type()], times:time(), times:record()) -> times:record().
build_times_record([], _Time, TimesRecord) ->
    TimesRecord;
build_times_record([?attr_creation_time | TimesList], Time, TimesRecord) ->
    build_times_record(TimesList, Time, TimesRecord#times{creation_time = Time});
build_times_record([?attr_atime | TimesList], Time, TimesRecord) ->
    build_times_record(TimesList, Time, TimesRecord#times{atime = Time});
build_times_record([?attr_mtime | TimesList], Time, TimesRecord) ->
    build_times_record(TimesList, Time, TimesRecord#times{mtime = Time});
build_times_record([?attr_ctime | TimesList], Time, TimesRecord) ->
    build_times_record(TimesList, Time, TimesRecord#times{ctime = Time}).
