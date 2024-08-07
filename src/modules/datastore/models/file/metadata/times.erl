%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon, Michał Stanisz
%%% @copyright (C) 2016-2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for holding files' times.
%%% @end
%%%-------------------------------------------------------------------
-module(times).
-author("Tomasz Lichon").
-author("Michał Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").


%% API
-export([create/4, update/2, get/1, delete/1]).
-export([is_deleted/1, merge_records/2]).
-export([ensure_synced/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2, resolve_conflict/3]).

-type key() :: file_meta:uuid().
-type record() :: #times{}.
-type doc() :: datastore_doc:doc(record()).
-type creation_time() :: time:seconds().
-type a_time() :: time:seconds().
-type m_time() :: time:seconds().
-type c_time() :: time:seconds().
-type time() :: creation_time() | a_time() | m_time() | c_time().

-export_type([record/0, time/0, a_time/0, c_time/0, m_time/0, creation_time/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    mutator => oneprovider:get_id_or_undefined()
}).


%%%===================================================================
%%% API
%%%===================================================================

-spec create(key(), od_space:id(), boolean(), record()) -> ok | {error, term()}.
create(Key, Scope, IgnoreInChanges, Times) ->
    ?extract_ok(datastore_model:create(?CTX, #document{
        key = Key, scope = Scope, value = Times, ignore_in_changes = IgnoreInChanges
    })).
    

-spec update(key(), record()) -> ok | {error, term()}.
update(Key, NewTimes) ->
    ?extract_ok(datastore_model:update(?CTX, Key, fun(PrevTimes) ->
        case merge_records(PrevTimes, NewTimes) of
            PrevTimes -> {error, no_change};
            FinalTimes -> {ok, FinalTimes}
        end
    end)).


-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).


-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).


-spec is_deleted(key()) -> boolean().
is_deleted(Key) ->
    case datastore_model:get(?CTX#{include_deleted => true}, Key) of
        {ok, #document{deleted = Deleted}} ->
            Deleted;
        {error, not_found} ->
            false
    end.

-spec merge_records(record(), record()) -> record().
merge_records(TimesA, TimesB) ->
    #times{
        creation_time = max(TimesA#times.creation_time, TimesB#times.creation_time),
        atime = max(TimesA#times.atime, TimesB#times.atime),
        mtime = max(TimesA#times.mtime, TimesB#times.mtime),
        ctime = max(TimesA#times.ctime, TimesB#times.ctime)
    }.


-spec ensure_synced(file_meta:uuid()) -> ok.
ensure_synced(Key) ->
    UpdateAns = datastore_model:update(?CTX#{ignore_in_changes => false}, Key, fun(Record) ->
        {ok, Record} % Return unchanged record, ignore_in_changes will be unset because of flag in CTX
    end),
    case UpdateAns of
        {ok, _} -> ok;
        {error, not_found} -> ok
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {atime, integer},
        {ctime, integer},
        {mtime, integer}
    ]};
get_record_struct(2) ->
    {record, [
        {creation_time, integer}, % new field
        {atime, integer},
        {mtime, integer},
        {ctime, integer}
    ]}.


-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, Times) ->
    {times,
        ATime,
        CTime,
        MTime
    } = Times,
    
    {2, #times{
        creation_time = lists:min([ATime, CTime, MTime]),
        atime = ATime,
        mtime = MTime,
        ctime = CTime
    }}.


-spec resolve_conflict(datastore_model:ctx(), doc(), doc()) ->
    {boolean(), doc()} | ignore | default.
resolve_conflict(_Ctx, RemoteDoc, LocalDoc) ->
    #document{value = LocalValue, revs = [LocalRev | _], deleted = LocalDeleted} = LocalDoc,
    #document{value = RemoteValue, revs = [RemoteRev | _], deleted = RemoteDeleted} = RemoteDoc,
    
    FinalValue = merge_records(RemoteValue, LocalValue),
    
    case datastore_rev:is_greater(RemoteRev, LocalRev) of
        true ->
            case {LocalDeleted, FinalValue} of
                {true, _} ->
                    case RemoteDeleted of
                        true -> {false, RemoteDoc};
                        false -> {true, RemoteDoc#document{deleted = true}}
                    end;
                {false, RemoteValue} ->
                    {false, RemoteDoc};
                _ ->
                    {true, RemoteDoc#document{value = FinalValue}}
            end;
        false ->
            case {RemoteDeleted, FinalValue} of
                {true, _} ->
                    case LocalDeleted of
                        true -> ignore;
                        false -> {true, LocalDoc#document{deleted = true}}
                    end;
                {false, LocalValue} ->
                    ignore;
                _ ->
                    {true, LocalDoc#document{value = FinalValue}}
            end
    end.