%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon % fixme
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding files' times. % fixme
%%% Note: this module operates on referenced uuids - all operations on hardlinks
%%% are treated as operations on original file. Thus, all hardlinks pointing on
%%% the same file share single times document.
%%% @end
%%%-------------------------------------------------------------------
-module(times).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([create2/4, update2/2, get2/1, delete2/1, ensure_synced/1]).
-export([is_deleted/1]).
-export([merge_records/2]). % fixme


%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2, resolve_conflict/3]).

% fixme
-type key() :: datastore:key().
-type record() :: #times{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type time() :: time:seconds().
-type a_time() :: time().
-type c_time() :: time().
-type m_time() :: time().
-type times() :: {a_time(), c_time(), m_time()}.

-export_type([record/0, time/0, a_time/0, c_time/0, m_time/0, times/0, diff/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    mutator => oneprovider:get_id_or_undefined()
}).


%%%===================================================================
%%% API
%%%===================================================================

create2(Key, Scope, IgnoreInChanges, Times) ->
    datastore_model:create(?CTX, #document{
        key = Key, scope = Scope, value = Times, ignore_in_changes = IgnoreInChanges
    }).
    
update2(Key, NewTimes) ->
    ?extract_ok(datastore_model:update(?CTX, Key, fun(PrevTimes) ->
        case merge_records(PrevTimes, NewTimes) of
            PrevTimes -> {error, no_change};
            FinalTimes -> {ok, FinalTimes}
        end
    end)).

get2(Key) -> % fixme name
    case datastore_model:get(?CTX, Key) of
        {ok, #document{value = Times}} ->
            {ok, Times};
        {error, _} = Error ->
            Error
    end.

delete2(Key) -> % fixme name
    datastore_model:delete(?CTX, Key).


-spec ensure_synced(file_meta:uuid()) -> ok.
ensure_synced(Key) ->
    UpdateAns = datastore_model:update(?CTX#{ignore_in_changes => false}, Key, fun(Record) ->
        {ok, Record} % Return unchanged record, ignore_in_changes will be unset because of flag in CTX
    end),
    case UpdateAns of
        {ok, _} -> ok;
        {error, not_found} -> ok
    end.


is_deleted(Key) ->
    case datastore_model:get(?CTX#{include_deleted => true}, Key) of
        {ok, #document{deleted = Deleted}} ->
            Deleted;
        {error, not_found} ->
            false
    end.


merge_records(TimesA, TimesB) ->
    #times{
        atime = max(TimesA#times.atime, TimesB#times.atime),
        ctime = max(TimesA#times.ctime, TimesB#times.ctime),
        mtime = max(TimesA#times.mtime, TimesB#times.mtime),
        creation_time = max(TimesA#times.creation_time, TimesB#times.creation_time)
    }.


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
        {atime, integer},
        {ctime, integer},
        {mtime, integer},
        {creation_time, integer} % new field
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
        atime = ATime,
        ctime = CTime,
        mtime = MTime,
        creation_time = lists:min([ATime, CTime, MTime])
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