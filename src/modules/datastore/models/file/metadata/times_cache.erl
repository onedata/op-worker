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
-export([report_created/3, update/2, get/1, report_deleted/1]).


-define(MAX_CACHE_SIZE, 10000).

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
    flush_key(ets:first(?MODULE)),
    ets:safe_fixtable(?MODULE, false),
    ok.


flush_key('$end_of_table') -> ok;
flush_key(Key) ->
    run_in_critical_section(Key, fun() ->
        case get_from_cache(Key) of
            {ok, Value} ->
                flush_value_insecure(Key, Value);
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


report_created(FileGuid, IgnoreInChanges, Times) ->
    % fixme explain not caching - creation is once per file so no need
    times_model_api:create(FileGuid, IgnoreInChanges, Times).
    
update(FileGuid, NewTimes) ->
    ok = put_in_cache(FileGuid, NewTimes),
    check_flush().

get(FileGuid) ->
    case get_from_cache(FileGuid) of
        {ok, Times} ->
            {ok, Times};
        ?ERROR_NOT_FOUND ->
            times_model_api:get(FileGuid)
    end.

report_deleted(FileGuid) ->
    % fixme explain not caching
    times_model_api:delete(FileGuid).

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_from_cache(Key) ->
    case ets:lookup(?MODULE, Key) of
        [{Key, Times}] ->
            {ok, Times};
        [] ->
            ?ERROR_NOT_FOUND
    end.


put_in_cache(Key, NewTimes) ->
    run_in_critical_section(Key, fun() ->
        case get_from_cache(Key) of
            {ok, CachedTimes} ->
                FinalTimes = #times{
                    atime = max(NewTimes#times.atime, CachedTimes#times.atime),
                    ctime = max(NewTimes#times.ctime, CachedTimes#times.ctime),
                    mtime = max(NewTimes#times.mtime, CachedTimes#times.mtime)
                },
                case FinalTimes of
                    CachedTimes ->
                        ok;
                    _ ->
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
