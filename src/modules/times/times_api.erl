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
    touch/2, update/2,
    get/1, get/2,
    report_file_deleted/1
]).

-type times_type() :: ?attr_atime | ?attr_ctime | ?attr_mtime | ?attr_creation_time.

-export_type([times_type/0]).

-define(NOW(), global_clock:timestamp_seconds()).

%%%===================================================================
%%% API
%%%===================================================================

-spec report_file_created(file_ctx:ctx()) -> ok | {error, term()}.
report_file_created(FileCtx) ->
    report_file_created(FileCtx, build_current_times_record()).

-spec report_file_created(file_ctx:ctx(), times:record()) -> ok | {error, term()}.
report_file_created(FileCtx, #times{creation_time = 0, atime = ATime, mtime = MTime, ctime = CTime} = TimesRecord) ->
    report_file_created(FileCtx, TimesRecord#times{creation_time = lists:min([ATime, CTime, MTime])});
report_file_created(FileCtx, TimesRecord) ->
    dir_update_time_stats:report_update(FileCtx, TimesRecord),
    {IsSyncEnabled, FileCtx2} = file_ctx:is_synchronization_enabled(FileCtx),
    times_cache:report_created(
        file_ctx:get_referenced_guid_const(FileCtx2), not IsSyncEnabled, TimesRecord).


-spec touch(file_ctx:ctx(), [times_type()]) -> ok.
touch(FileCtx, TimesToUpdate) ->
    TimesRecord = build_times_record(TimesToUpdate, ?NOW()),
    update(FileCtx, TimesRecord).


-spec update(file_ctx:ctx(), times:record()) -> ok.
update(FileCtx, TimesRecord) ->
    dir_update_time_stats:report_update(FileCtx, TimesRecord),
    times_cache:report_change(file_ctx:get_referenced_guid_const(FileCtx), TimesRecord).


-spec get(file_ctx:ctx()) -> {ok, times:record()} | {error, term()}.
get(FileCtx) ->
    get(FileCtx, [?attr_atime, ?attr_mtime, ?attr_ctime]).

-spec get(file_ctx:ctx(), [times_type()]) -> {ok, times:record()} | {error, term()}.
get(FileCtx, RequestedTimes) ->
    case times_cache:get(file_ctx:get_referenced_guid_const(FileCtx), RequestedTimes) of
        {ok, Times} -> {ok, Times};
        {error, not_found} -> {ok, #times{}};
        {error, _} = Error -> Error
    end.


-spec report_file_deleted(file_ctx:ctx()) -> ok | {error, term()}.
report_file_deleted(FileCtx) ->
    dir_update_time_stats:report_update(FileCtx, build_times_record([?attr_atime, ?attr_mtime, ?attr_ctime], ?NOW())),
    times_cache:report_deleted(file_ctx:get_referenced_guid_const(FileCtx)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec build_current_times_record() -> times:record().
build_current_times_record() ->
    build_times_record([?attr_creation_time, ?attr_atime, ?attr_mtime, ?attr_ctime], ?NOW()).


-spec build_times_record([times_type()], times:time()) -> times:record().
build_times_record(TimesToUpdate, Time) ->
    build_times_record(TimesToUpdate, Time, #times{}).

-spec build_times_record([times_type()], times:time(), times:record()) -> times:record().
build_times_record([], _Time, TimesRecord) ->
    TimesRecord;
build_times_record([?attr_creation_time | TimesToUpdate], Time, TimesRecord) ->
    build_times_record(TimesToUpdate, Time, TimesRecord#times{creation_time = Time});
build_times_record([?attr_atime | TimesToUpdate], Time, TimesRecord) ->
    build_times_record(TimesToUpdate, Time, TimesRecord#times{atime = Time});
build_times_record([?attr_mtime | TimesToUpdate], Time, TimesRecord) ->
    build_times_record(TimesToUpdate, Time, TimesRecord#times{mtime = Time});
build_times_record([?attr_ctime | TimesToUpdate], Time, TimesRecord) ->
    build_times_record(TimesToUpdate, Time, TimesRecord#times{ctime = Time}).
