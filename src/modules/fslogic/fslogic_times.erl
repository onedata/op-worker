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
-export([update_atime/2, update_atime/3, update_ctime/2, update_ctime/3,
    update_mtime_ctime/2, update_mtime_ctime/3, update_times_and_emit/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Returns value of atime to be set for given file
%% or atom "actual" if time should not be changed.
%% Atom "actual" will be returned when current atime of entry
%% is not before mtime or ctime of entry and not older than 24 hours.
%% @end
%%--------------------------------------------------------------------
-spec calculate_atime(FileEntry :: fslogic_worker:file(),
    CurrentTime :: file_meta:time()) -> file_meta:time() | actual.
calculate_atime(FileEntry, CurrentTime) ->
    {ok, #document{value = #file_meta{
        atime = ATime,
        mtime = MTime,
        ctime = CTime}}
    } = file_meta:get(FileEntry),
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
%% @doc Updates entry atime to current time, unless it is actual
%%--------------------------------------------------------------------
-spec update_atime(fslogic_worker:file(), UserId :: onedata_user:id()) -> ok.
update_atime(Entry, UserId) ->
    update_atime(Entry, UserId, erlang:system_time(seconds)).

%%--------------------------------------------------------------------
%% @doc Updates entry atime to given time, unless it is actual
%%--------------------------------------------------------------------
-spec update_atime(fslogic_worker:file(), UserId :: onedata_user:id(),
    CurrentTime :: file_meta:time()) -> ok.
update_atime(Entry, UserId, CurrentTime) ->
    case calculate_atime(Entry, CurrentTime) of
        actual ->
            ok;
        NewATime ->
            ok = update_times_and_emit(Entry, #{atime => NewATime}, UserId)
    end.

%%--------------------------------------------------------------------
%% @doc Updates entry ctime to current time
%%--------------------------------------------------------------------
-spec update_ctime(fslogic_worker:file(), UserId :: onedata_user:id()) -> ok.
update_ctime(Entry, UserId) ->
    update_ctime(Entry, UserId, erlang:system_time(seconds)).

%%--------------------------------------------------------------------
%% @doc Updates entry ctime to given time
%%--------------------------------------------------------------------
-spec update_ctime(fslogic_worker:file(), UserId :: onedata_user:id(),
    CurrentTime :: file_meta:time()) -> ok.
update_ctime(Entry, UserId, CurrentTime) ->
    ok = update_times_and_emit(Entry, #{ctime => CurrentTime}, UserId).

%%--------------------------------------------------------------------
%% @doc Updates entry mtime and ctime to current time
%%--------------------------------------------------------------------
-spec update_mtime_ctime(fslogic_worker:file(), UserId :: onedata_user:id()) ->
    ok.
update_mtime_ctime(Entry, UserId) ->
    update_mtime_ctime(Entry, UserId, erlang:system_time(seconds)).

%%--------------------------------------------------------------------
%% @doc Updates entry mtime and ctime to given time
%%--------------------------------------------------------------------
-spec update_mtime_ctime(fslogic_worker:file(), UserId :: onedata_user:id(),
    CurrentTime :: file_meta:time()) -> ok.
update_mtime_ctime(Entry, UserId, CurrentTime) ->
    ok = update_times_and_emit(Entry, #{mtime => CurrentTime, ctime => CurrentTime}, UserId).

%%--------------------------------------------------------------------
%% @doc Updates entry with given map and emits times update event
%%--------------------------------------------------------------------
-spec update_times_and_emit(fslogic_worker:file(),
    TimesMap :: #{atom => file_meta:time()}, UserId :: onedata_user:id()) -> ok.
update_times_and_emit(Entry, TimesMap, UserId) ->
    {ok, UUID} = file_meta:update(Entry, TimesMap),
    spawn(fun() -> fslogic_event:emit_file_sizeless_attrs_update({uuid, UUID}) end),

    ok.