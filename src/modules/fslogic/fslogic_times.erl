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
-export([calculate_atime/1, update_atime/2, update_ctime/2,
    update_mtime_ctime/2]).

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
-spec calculate_atime(FileEntry :: fslogic_worker:file()) -> integer() | atom().
calculate_atime(FileEntry) ->
    {ok, #document{value = #file_meta{
        atime = ATime,
        mtime = MTime,
        ctime = CTime}}
    } = file_meta:get(FileEntry),
    CurrentTime = erlang:system_time(seconds),
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
    case fslogic_times:calculate_atime(Entry) of
        actual ->
            ok;
        NewATime ->
            ok = update_times_and_emit(Entry, #{atime => NewATime}, UserId)
    end.

%%--------------------------------------------------------------------
%% @doc Updates entry ctime
%%--------------------------------------------------------------------
-spec update_ctime(fslogic_worker:file(), UserId :: onedata_user:id()) -> ok.
update_ctime(Entry, UserId) ->
    CurrentTime = erlang:system_time(seconds),
    ok = update_times_and_emit(Entry, #{ctime => CurrentTime}, UserId).

%%--------------------------------------------------------------------
%% @doc Updates entry mtime and ctime
%%--------------------------------------------------------------------
-spec update_mtime_ctime(fslogic_worker:file(), UserId :: onedata_user:id()) ->
    ok.
update_mtime_ctime(Entry, UserId) ->
    CurrentTime = erlang:system_time(seconds),
    ok = update_times_and_emit(Entry, #{mtime => CurrentTime, ctime => CurrentTime}, UserId).

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Updates entry with given map and emits times update event
%%--------------------------------------------------------------------
-spec update_times_and_emit(fslogic_worker:file(),
    TimesMap :: #{atom => file_meta:time()}, UserId :: onedata_user:id()) -> ok.
update_times_and_emit(Entry, TimesMap, UserId) ->
    {ok, UUID} = file_meta:update(Entry, TimesMap),
    spawn(fun() -> fslogic_event:emit_file_sizeless_attrs_update({uuid, UUID}) end),

    RootSpaceUUID = fslogic_uuid:default_space_uuid(UserId),
    {ok, #document{key = DefaultSpaceUUID}} = fslogic_spaces:get_default_space(UserId),

    case UUID of
        RootSpaceUUID ->
            {ok, DefaultSpaceUUID} = file_meta:update(DefaultSpaceUUID, TimesMap),
            spawn(fun() -> fslogic_event:emit_file_sizeless_attrs_update({uuid, DefaultSpaceUUID}) end);
        DefaultSpaceUUID ->
            {ok, RootSpaceUUID} = file_meta:update(RootSpaceUUID, TimesMap),
            spawn(fun() -> fslogic_event:emit_file_sizeless_attrs_update({uuid, RootSpaceUUID}) end);
        _ ->
            ok
    end,
    ok.