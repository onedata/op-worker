%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module functions as a cache for times changes. It is based on ets.
%%% Changes for single file are aggregated. Cache is flushed periodically
%%% (which is managed by fslogic_worker) or when MAX_CACHE_SIZE is reached.
%%% After a value is successfully flushed and saved in datastore, it is removed
%%% from cache and event with times changes is produced.
%%% @end
%%%-------------------------------------------------------------------
-module(times_cache).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([init/0, destroy/0]).
-export([flush/0]).
-export([report_created/3, report_changed/2, acquire/2, report_deleted/1]).

-type key() :: file_id:file_guid().
-type value() :: times:record().

-define(MAX_CACHE_SIZE, op_worker:get_env(times_cache_max_size, 10000)).

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


-spec flush() -> ok.
flush() ->
    ets:safe_fixtable(?MODULE, true),
    try
        flush_keys_recursively(ets:first(?MODULE))
    catch Class:Reason:Stacktrace ->
        ?error_exception(Class, Reason, Stacktrace)
    end,
    ets:safe_fixtable(?MODULE, false),
    ok.


-spec report_created(key(), boolean(), value()) -> ok | {error, term()}.
report_created(FileGuid, IgnoreInChanges, Times) ->
    times:create(file_id:guid_to_uuid(FileGuid), file_id:guid_to_space_id(FileGuid), IgnoreInChanges, Times).
    

-spec report_changed(key(), value()) -> ok.
report_changed(Key, NewTimes) ->
    run_in_critical_section(Key, fun() ->
        case get_from_ets(Key) of
            {ok, CachedTimes} ->
                case times:merge_records(NewTimes, CachedTimes) of
                    CachedTimes -> ok;
                    FinalTimes -> put_in_ets(Key, FinalTimes)
                end;
            {error, not_found} ->
                put_in_ets(Key, NewTimes)
        end
    end),
    flush_if_full().


-spec acquire(key(), [times_api:times_type()]) -> {ok, value()} | {error, term()}.
acquire(FileGuid, RequestedTimes) ->
    case get_from_ets(FileGuid) of
        {ok, CachedTimes} ->
            case are_times_in_record(RequestedTimes, CachedTimes) of
                true ->
                    {ok, CachedTimes};
                false ->
                    case get_from_db(FileGuid) of
                        {ok, DatastoreTimes} ->
                            MergedTimes = times:merge_records(CachedTimes, DatastoreTimes),
                            put_in_ets(FileGuid, MergedTimes),
                            {ok, MergedTimes};
                        {error, _} = Error ->
                            Error
                    end
            end;
        {error, not_found} ->
            get_from_db(FileGuid)
    end.


-spec report_deleted(key()) -> ok | {error, term()}.
report_deleted(FileGuid) ->
    % no need to remove from cache, it will be done in the next flush
    times:delete(file_id:guid_to_uuid(FileGuid)).

%%%===================================================================
%%% Internal functions - flush
%%%===================================================================

%% @private
-spec flush_if_full() -> ok.
flush_if_full() ->
    case cache_size() >= ?MAX_CACHE_SIZE of
        true -> flush();
        _ -> ok
    end.


%% @private
-spec flush_keys_recursively(key() | '$end_of_table') -> ok.
flush_keys_recursively('$end_of_table') -> ok;
flush_keys_recursively(Key) ->
    run_in_critical_section(Key, fun() ->
        case get_from_ets(Key) of
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
    flush_keys_recursively(ets:next(?MODULE, Key)).


%% @private
-spec flush_value_insecure(key(), value()) -> ok.
flush_value_insecure(Key, Value) ->
    case update_in_db(Key, Value) of
        ok ->
            delete_from_ets(Key);
        {error, not_found} ->
            case times:is_deleted(Key) of
                true -> delete_from_ets(Key);
                false -> ok % race with dbsync - leave the value in the cache, it will be saved in the next flush
            end
    end.

%%%===================================================================
%%% Internal functions - times helpers
%%%===================================================================

%% @private
-spec get_from_db(key()) -> {ok, value()} | {error, term()}.
get_from_db(FileGuid) ->
    case times:get(file_id:guid_to_uuid(FileGuid)) of
        {ok, #document{value = Value}} -> {ok, Value};
        {error, _} = Error -> Error
    end.


%% @private
-spec update_in_db(key(), value()) -> ok | {error, term()}.
update_in_db(FileGuid, Value) ->
    case times:update(file_id:guid_to_uuid(FileGuid), Value) of
        ok ->
            fslogic_event_emitter:emit_sizeless_file_attrs_changed(file_ctx:new_by_guid(FileGuid));
        {error, no_change} ->
            ok;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec are_times_in_record([times_api:times_type()], value()) -> boolean().
are_times_in_record(TimesToCheck, TimesRecord) ->
    are_times_in_record(TimesToCheck, TimesRecord, true).

%% @private
-spec are_times_in_record([times_api:times_type()], value(), boolean()) -> boolean().
are_times_in_record(_, _, false) ->
    false;
are_times_in_record([], _, true) ->
    true;
are_times_in_record([?attr_creation_time | TimesToCheck], #times{creation_time = CreationTime} = TimesRecord, _) ->
    are_times_in_record(TimesToCheck, TimesRecord, CreationTime =/= 0);
are_times_in_record([?attr_atime | TimesToCheck], #times{atime = ATime} = TimesRecord, _) ->
    are_times_in_record(TimesToCheck, TimesRecord, ATime =/= 0);
are_times_in_record([?attr_mtime | TimesToCheck], #times{mtime = MTime} = TimesRecord, _) ->
    are_times_in_record(TimesToCheck, TimesRecord, MTime =/= 0);
are_times_in_record([?attr_ctime | TimesToCheck], #times{ctime = Ctime} = TimesRecord, _) ->
    are_times_in_record(TimesToCheck, TimesRecord, Ctime =/= 0).


%%%===================================================================
%%% Internal functions - ets helpers
%%%===================================================================

%% @private
-spec get_from_ets(key()) -> {ok, value()} | {error, not_found}.
get_from_ets(Key) ->
    case ets:lookup(?MODULE, Key) of
        [{Key, Times}] ->
            {ok, Times};
        [] ->
            {error, not_found}
    end.


%% @private
-spec delete_from_ets(key()) -> ok.
delete_from_ets(Key) ->
    true = ets:delete(?MODULE, Key),
    ok.


%% @private
-spec put_in_ets(key(), value()) -> ok.
put_in_ets(Key, Value) ->
    true = ets:insert(?MODULE, {Key, Value}),
    ok.


%% @private
-spec cache_size() -> non_neg_integer().
cache_size() ->
    ets:info(?MODULE, size).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec run_in_critical_section(key(), fun(() -> Term)) -> Term.
run_in_critical_section(Key, Function) ->
    critical_section:run({?MODULE, Key}, Function).
