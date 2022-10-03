%%%--------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module provides functions operating on file timestamps
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_times).
-author("Mateusz Paciorek").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([update_atime/1, update_ctime/1, update_ctime_without_emit/1, update_ctime/3, update_mtime_ctime/1,
    update_mtime_ctime/2, update_times_and_emit/2]).

-define(NOW(), global_clock:timestamp_seconds()).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Updates entry atime to current time, unless it is actual
%% @end
%%--------------------------------------------------------------------
-spec update_atime(file_ctx:ctx()) -> ok.
update_atime(FileCtx) ->
    case calculate_atime(FileCtx, ?NOW()) of
        actual ->
            ok;
        NewATime ->
            ok = update_times_and_emit(FileCtx, fun(Times = #times{atime = Time}) ->
                case Time of
                    NewATime ->
                        {error, {not_changed, Times}};
                    _ ->
                        {ok, Times#times{atime = NewATime}}
                end
            end)
    end.

%%--------------------------------------------------------------------
%% @equiv update_ctime(FileCtx, ?NOW()).
%% @end
%%--------------------------------------------------------------------
-spec update_ctime(file_ctx:ctx()) -> ok.
update_ctime(FileCtx) ->
    update_ctime(FileCtx, ?NOW(), true).

-spec update_ctime_without_emit(file_ctx:ctx()) -> ok.
update_ctime_without_emit(FileCtx) ->
    update_ctime(FileCtx, ?NOW(), false).

%%--------------------------------------------------------------------
%% @doc
%% Updates entry ctime to given time
%% @end
%%--------------------------------------------------------------------
-spec update_ctime(file_ctx:ctx(), CurrentTime :: file_meta:time(), boolean()) -> ok.
update_ctime(FileCtx, CurrentTime, Emit) ->
    ok = update_times(FileCtx, fun(Times = #times{ctime = Time}) ->
        case Time of
            CurrentTime ->
                {error, {not_changed, Times}};
            _ ->
                {ok, Times#times{ctime = CurrentTime}}
        end
    end, Emit).

%%--------------------------------------------------------------------
%% @equiv update_mtime_ctime(FileCtx, ?NOW()).
%% @end
%%--------------------------------------------------------------------
-spec update_mtime_ctime(file_ctx:ctx()) ->
    ok.
update_mtime_ctime(FileCtx) ->
    update_mtime_ctime(FileCtx, ?NOW()).

%%--------------------------------------------------------------------
%% @doc
%% Updates entry mtime and ctime to given time
%% @end
%%--------------------------------------------------------------------
-spec update_mtime_ctime(file_ctx:ctx(), CurrentTime :: file_meta:time()) -> ok.
update_mtime_ctime(FileCtx, CurrentTime) ->
    ok = update_times_and_emit(FileCtx, fun(Times = #times{mtime = MTime, ctime = CTime}) ->
        case {MTime, CTime} of
            {CurrentTime, CurrentTime} ->
                {error, {not_changed, Times}};
            _ ->
                {ok, Times#times{mtime = CurrentTime, ctime = CurrentTime}}
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Updates entry with given map and emits times update event
%% @end
%%--------------------------------------------------------------------
-spec update_times_and_emit(file_ctx:ctx(), times:diff()) -> ok.
update_times_and_emit(FileCtx, TimesDiff) ->
    update_times(FileCtx, TimesDiff, true).

-spec update_times(file_ctx:ctx(), times:diff(), boolean()) -> ok.
update_times(FileCtx, TimesDiff, Emit) ->
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    case fslogic_file_id:is_share_root_dir_uuid(FileUuid) of
        true ->
            ok;
        false ->
            Times = prepare_times(TimesDiff),
            case times:create_or_update(#document{key = FileUuid,
                value = Times, scope = file_ctx:get_space_id_const(FileCtx)}, TimesDiff) of
                {ok, #document{value = FinalTimes}} ->
                    spawn(fun() ->
                        % TODO VFS-8830 - set file_ctx:is_dir in functions that update times and know file type
                        % to optimize type check
                        dir_update_time_stats:report_update_of_nearest_dir(file_ctx:get_logical_guid_const(FileCtx), FinalTimes),
                        Emit andalso fslogic_event_emitter:emit_sizeless_file_attrs_changed(FileCtx)
                    end),
                    ok;
                {error, {not_changed, _}} ->
                    ok
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc Returns value of atime to be set for given file
%% or atom "actual" if time should not be changed.
%% Atom "actual" will be returned when current atime of entry
%% is not before mtime or ctime of entry and not older than 24 hours.
%% @end
%%--------------------------------------------------------------------
-spec calculate_atime(file_ctx:ctx(), CurrentTime :: file_meta:time()) ->
    file_meta:time() | actual.
calculate_atime(FileCtx, CurrentTime) ->
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    {ok, {ATime, CTime, MTime}} = times:get_or_default(FileUuid),
    case ATime of
        Outdated when Outdated =< MTime orelse Outdated =< CTime ->
            CurrentTime;
        _ ->
            case (CurrentTime - ATime) of
                TooLongTime when TooLongTime > (24 * 60 * 60) ->
                    CurrentTime;
                _ ->
                    actual
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert map to times document
%% @end
%%--------------------------------------------------------------------
-spec prepare_times(times:diff()) -> #times{}.
prepare_times(TimesDiff) ->
    case TimesDiff(#times{}) of
        {ok, Times} -> Times;
        {error, {not_changed, Times}} -> Times
    end.
