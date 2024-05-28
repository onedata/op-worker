%%%--------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module provides functions operating on file timestamps % fixme
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
    get/1,
    report_file_deleted/1
]).


-define(NOW(), global_clock:timestamp_seconds()).

%%%===================================================================
%%% API
%%%===================================================================

report_file_created(FileCtx) ->
    CurrentTime = ?NOW(), % fixme refactor, repeats several times
    report_file_created(FileCtx, #times{
        atime = CurrentTime,
        ctime = CurrentTime,
        mtime = CurrentTime,
        creation_time = CurrentTime
    }).


report_file_created(FileCtx, #times{creation_time = 0} = TimesRecord) ->
    report_file_created(FileCtx, TimesRecord#times{creation_time = ?NOW()}); % fixme min of other times, do it in sync modules??
report_file_created(FileCtx, TimesRecord) ->
    dir_update_time_stats:report_update_of_nearest_dir(file_ctx:get_logical_guid_const(FileCtx), TimesRecord),
    {IsSyncEnabled, FileCtx2} = file_ctx:is_synchronization_enabled(FileCtx),
    times_cache:report_created(file_ctx:get_referenced_guid_const(FileCtx2), not IsSyncEnabled, TimesRecord).


report_change(FileCtx, TimesListToUpdate) ->
    CurrentTime = ?NOW(),
    TimesRecord = build_times_record(TimesListToUpdate, CurrentTime),
    update(FileCtx, TimesRecord).


update(FileCtx, TimesRecord) ->
    dir_update_time_stats:report_update_of_nearest_dir(file_ctx:get_logical_guid_const(FileCtx), TimesRecord), % fixme pass file_ctx and handle there whether nearest_dir or dir
    times_cache:update(file_ctx:get_referenced_guid_const(FileCtx), TimesRecord).


get(FileCtx) ->
    times_cache:get(file_ctx:get_referenced_guid_const(FileCtx)).


report_file_deleted(FileCtx) ->
    CurrentTime = ?NOW(), % fixme refactor, repeats several times
    dir_update_time_stats:report_update_of_nearest_dir(file_ctx:get_logical_guid_const(FileCtx), #times{
        atime = CurrentTime,
        ctime = CurrentTime,
        mtime = CurrentTime
    }),
    times_cache:report_deleted(FileCtx).


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
