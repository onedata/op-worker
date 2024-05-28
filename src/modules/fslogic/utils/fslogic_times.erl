%%%--------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module provides functions operating on file timestamps % fixme
% fixme below is on referenced, as times are kept only for referenced
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_times).
-author("Mateusz Paciorek").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    report_file_created/1, report_file_created/2,
    report_change/2, update/2,
    get/1, get/2,
    report_file_deleted/1
]).


-define(NOW(), global_clock:timestamp_seconds()).

%%%===================================================================
%%% API
%%%===================================================================

report_file_created(FileCtx) ->
    report_file_created(FileCtx, #{event_verbosity => emit}).


report_file_created(FileCtx, #times{} = TimesRecord) ->
    report_file_created(FileCtx, #{times => TimesRecord});
report_file_created(FileCtx, Opts) ->
    report_file_created(FileCtx,
        maps:get(event_verbosity, Opts, emit),
        maps:get(times, Opts, build_current_times_record())
    ).


% fixme private
report_file_created(FileCtx, EventVerbosity, #times{creation_time = 0} = TimesRecord) ->
    report_file_created(FileCtx, EventVerbosity, TimesRecord#times{creation_time = min_time(TimesRecord)});
report_file_created(FileCtx, EventVerbosity, TimesRecord) ->
    dir_update_time_stats:report_update_of_nearest_dir(file_ctx:get_logical_guid_const(FileCtx), TimesRecord),
    {IsSyncEnabled, FileCtx2} = file_ctx:is_synchronization_enabled(FileCtx),
    times_cache:report_created(
        file_ctx:get_referenced_guid_const(FileCtx2), not IsSyncEnabled, EventVerbosity, TimesRecord).


build_current_times_record() ->
    CurrentTime = ?NOW(), % fixme refactor, repeats several times
    #times{
        atime = CurrentTime,
        ctime = CurrentTime,
        mtime = CurrentTime,
        creation_time = CurrentTime
    }.


min_time(#times{atime = ATime, ctime = CTime, mtime = MTime}) ->
    lists:min([ATime, CTime, MTime]).


report_change(FileCtx, TimesListToUpdate) ->
    TimesRecord = build_times_record(TimesListToUpdate, ?NOW()),
    update(FileCtx, TimesRecord).


update(FileCtx, TimesRecord) ->
    dir_update_time_stats:report_update_of_nearest_dir(file_ctx:get_logical_guid_const(FileCtx), TimesRecord), % fixme pass file_ctx and handle there whether nearest_dir or dir
    times_cache:update(file_ctx:get_referenced_guid_const(FileCtx), TimesRecord).

get(FileCtx) -> % fixme return file_ctx with filled times??
    get(FileCtx, [atime, ctime, mtime]).


get(FileCtx, RequestedTimes) ->
    times_cache:get(file_ctx:get_referenced_guid_const(FileCtx), RequestedTimes).


report_file_deleted(FileCtx) ->
    CurrentTime = ?NOW(), % fixme refactor, repeats several times
    dir_update_time_stats:report_update_of_nearest_dir(file_ctx:get_logical_guid_const(FileCtx), #times{
        atime = CurrentTime,
        ctime = CurrentTime,
        mtime = CurrentTime
    }),
    times_cache:report_deleted(file_ctx:get_referenced_guid_const(FileCtx)).


build_times_record(TimesToUpdate, Time) ->
    build_times_record(TimesToUpdate, Time, #times{}).


build_times_record([], _Time, TimesRecord) ->
    TimesRecord;
build_times_record([atime | TimesToUpdate], Time, TimesRecord) ->
    build_times_record(TimesToUpdate, Time, TimesRecord#times{atime = Time});
build_times_record([ctime | TimesToUpdate], Time, TimesRecord) ->
    build_times_record(TimesToUpdate, Time, TimesRecord#times{ctime = Time});
build_times_record([mtime | TimesToUpdate], Time, TimesRecord) ->
    build_times_record(TimesToUpdate, Time, TimesRecord#times{mtime = Time}).
