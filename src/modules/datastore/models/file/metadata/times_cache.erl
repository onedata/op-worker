%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
% fixme
% fixme Key is file guid, value is times record
%%% @end
%%%-------------------------------------------------------------------
-module(times_cache). % fixme name % fixme move
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([init/0, destroy/0]).
-export([flush/0]).
-export([report_created/4, update/2, get/2, report_deleted/1]).


-define(MAX_CACHE_SIZE, 10000). % fixme app_config?? and add the variable from fslogic_worker

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> ok.
init() ->
    ?MODULE = ets:new(
        ?MODULE,
        [set, public, named_table, {read_concurrency, true}]
    ),
    ok.


-spec destroy() -> ok.
destroy() ->
    true = ets:delete(?MODULE),
    ok.


check_flush() ->
    case ets:info(?MODULE, size) of
        Size when Size >= ?MAX_CACHE_SIZE ->
            flush();
        _ ->
            ok
    end.


flush() ->
    ets:safe_fixtable(?MODULE, true),
    try
        flush_key(ets:first(?MODULE))
    catch Class:Reason:Stacktrace ->
        ?error_exception(Class, Reason, Stacktrace)
    end,
    ets:safe_fixtable(?MODULE, false),
    ok.


flush_key('$end_of_table') -> ok;
flush_key(Key) ->
    run_in_critical_section(Key, fun() ->
        case get_from_cache(Key) of
            {ok, Value} ->
                try
                    flush_value_insecure(Key, Value)
                catch Class:Reason:Stacktrace ->
                    ?error_exception(Class, Reason, Stacktrace)
                end;
            ?ERROR_NOT_FOUND ->
                ok
        end
    end),
    flush_key(ets:next(?MODULE, Key)).


flush_value_insecure(Key, Value) ->
    case times_model_api:update(Key, Value) of
        ok ->
            true = ets:delete(?MODULE, Key),
            ok;
        {error, not_found} ->
            case times_model_api:is_doc_deleted(Key) of
                true ->
                    true = ets:delete(?MODULE, Key),
                    ok;
                false ->
                    % race with dbsync - leave the value in the cache, it will be saved in the next flush
                    ok
            end
    end.


report_created(FileGuid, IgnoreInChanges, EventVerbosity, Times) ->
    % fixme explain not caching - creation is once per file so no need
    times_model_api:create(FileGuid, IgnoreInChanges, EventVerbosity, Times).
    
update(FileGuid, NewTimes) ->
    ok = put_in_cache(FileGuid, NewTimes),
    check_flush().

get(FileGuid, RequestedTimes) ->
    case get_from_cache(FileGuid) of
        {ok, CachedTimes} ->
            case are_times_in_record(RequestedTimes, CachedTimes) of
                true ->
                    {ok, CachedTimes};
                false ->
                    case times_model_api:get(FileGuid) of
                        {ok, DBTimes} ->
                            {ok, times:merge_records(CachedTimes, DBTimes)};
                        {error, _} = Error ->
                            Error
                    end
            end;
        {error, not_found} ->
            times_model_api:get(FileGuid)
    end.

report_deleted(FileGuid) ->
    % fixme explain not caching
    times_model_api:delete(FileGuid).

%%%===================================================================
%%% Internal functions
%%%===================================================================

are_times_in_record(TimesToCheck, TimesRecord) ->
    are_times_in_record(TimesToCheck, TimesRecord, true).

are_times_in_record(_, _, false) ->
    false;
are_times_in_record([], _, true) ->
    true;
are_times_in_record([atime | TimesToCheck], #times{atime = ATime} = TimesRecord, _) ->
    are_times_in_record(TimesToCheck, TimesRecord, ATime =/= 0);
are_times_in_record([ctime | TimesToCheck], #times{ctime = Ctime} = TimesRecord, _) ->
    are_times_in_record(TimesToCheck, TimesRecord, Ctime =/= 0);
are_times_in_record([mtime | TimesToCheck], #times{mtime = MTime} = TimesRecord, _) ->
    are_times_in_record(TimesToCheck, TimesRecord, MTime =/= 0);
are_times_in_record([creation_time | TimesToCheck], #times{creation_time = CreationTime} = TimesRecord, _) ->
    are_times_in_record(TimesToCheck, TimesRecord, CreationTime =/= 0).


get_from_cache(Key) ->
    case ets:lookup(?MODULE, Key) of
        [{Key, Times}] ->
            {ok, Times};
        [] ->
            {error, not_found}
    end.


put_in_cache(Key, NewTimes) ->
    run_in_critical_section(Key, fun() ->
        case get_from_cache(Key) of
            {ok, CachedTimes} ->
                case times:merge_records(NewTimes, CachedTimes) of
                    CachedTimes ->
                        ok;
                    FinalTimes ->
                        true = ets:insert(?MODULE, {Key, FinalTimes}),
                        ok
                end;
            ?ERROR_NOT_FOUND ->
                true = ets:insert(?MODULE, {Key, NewTimes}),
                ok
        end
    end).


run_in_critical_section(Key, Function) ->
    critical_section:run({?MODULE, Key}, Function).
