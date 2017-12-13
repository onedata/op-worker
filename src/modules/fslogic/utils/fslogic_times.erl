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

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([update_atime/1, update_ctime/1, update_ctime/2, update_mtime_ctime/1,
    update_mtime_ctime/2, update_times_and_emit/2]).

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
    CurrentTime = utils:system_time_seconds(),
    case calculate_atime(FileCtx, CurrentTime) of
        actual ->
            ok;
        NewATime ->
            ok = update_times_and_emit(FileCtx, #{atime => NewATime})
    end.

%%--------------------------------------------------------------------
%% @equiv update_ctime(FileCtx, utils:system_time_seconds()).
%% @end
%%--------------------------------------------------------------------
-spec update_ctime(file_ctx:ctx()) -> ok.
update_ctime(FileCtx) ->
    update_ctime(FileCtx, utils:system_time_seconds()).

%%--------------------------------------------------------------------
%% @doc
%% Updates entry ctime to given time
%% @end
%%--------------------------------------------------------------------
-spec update_ctime(file_ctx:ctx(), CurrentTime :: file_meta:time()) -> ok.
update_ctime(FileCtx, CurrentTime) ->
    ok = update_times_and_emit(FileCtx, #{ctime => CurrentTime}).

%%--------------------------------------------------------------------
%% @equiv update_mtime_ctime(FileCtx, utils:system_time_seconds()).
%% @end
%%--------------------------------------------------------------------
-spec update_mtime_ctime(file_ctx:ctx()) ->
    ok.
update_mtime_ctime(FileCtx) ->
    update_mtime_ctime(FileCtx, utils:system_time_seconds()).

%%--------------------------------------------------------------------
%% @doc
%% Updates entry mtime and ctime to given time
%% @end
%%--------------------------------------------------------------------
-spec update_mtime_ctime(file_ctx:ctx(), CurrentTime :: file_meta:time()) -> ok.
update_mtime_ctime(FileCtx, CurrentTime) ->
    ok = update_times_and_emit(FileCtx, #{mtime => CurrentTime, ctime => CurrentTime}).

%%--------------------------------------------------------------------
%% @doc
%% Updates entry with given map and emits times update event
%% @end
%%--------------------------------------------------------------------
-spec update_times_and_emit(file_ctx:ctx(),
    TimesMap :: #{atom() => file_meta:time()}) -> ok.
update_times_and_emit(FileCtx, TimesMap) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    Times = prepare_times(TimesMap),
    {ok, FileUuid} = times:create_or_update(#document{key = FileUuid,
        value = Times, scope = file_ctx:get_space_id_const(FileCtx)}, TimesMap),
%%    spawn(fun() ->
        fslogic_event_emitter:emit_sizeless_file_attrs_changed(FileCtx),
%%    end),
    ok.

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
    FileUuid = file_ctx:get_uuid_const(FileCtx),
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
-spec prepare_times(#{atom() => file_meta:time()}) -> #times{}.
prepare_times(TimesMap) ->
    #times{
        atime = maps:get(atime, TimesMap, 0),
        ctime = maps:get(ctime, TimesMap, 0),
        mtime = maps:get(mtime, TimesMap, 0)
    }.