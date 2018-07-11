%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements read-write lock used by file replica
%%% invalidation algorithm. It can be acquired simultaneously by many
%%% processes in context of reader and only by one in context of writer.
%%% Acquiring the lock is not blocking, if it is impossible to acquire
%%% lock in given situation, locking function returns an error.
%%% Lock are used per file and therefore lock ids are file_uuids.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_deletion_lock).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([
    acquire_read_lock/1, acquire_write_lock/1,
    release_read_lock/1, release_write_lock/1
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type id() :: file_meta:uuid().
-type lock() :: #replica_deletion_lock{}.
-type diff() :: datastore_doc:diff(lock()).
-type doc() :: datastore_doc:doc(lock()).

-define(CTX, #{
    model => ?MODULE,
    routing => global,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Acquires lock in read mode.
%% @end
%%-------------------------------------------------------------------
-spec acquire_read_lock(id()) -> ok | {error, term()}.
acquire_read_lock(FileUuid) ->
    case read_create_or_update(FileUuid, fun acquire_read_lock_diff/1) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

%%-------------------------------------------------------------------
%% @doc
%% Acquires lock in write mode.
%% @end
%%-------------------------------------------------------------------
-spec acquire_write_lock(id()) -> ok | {error, term()}.
acquire_write_lock(FileUuid) ->
    case write_create_or_update(FileUuid, fun acquire_write_lock_diff/1) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

%%-------------------------------------------------------------------
%% @doc
%% Releases lock in read mode.
%% @end
%%-------------------------------------------------------------------
-spec release_read_lock(id()) -> ok.
release_read_lock(FileUuid) ->
    {ok, _} = update(FileUuid, fun(Lock = #replica_deletion_lock{read = Read}) ->
        {ok, Lock#replica_deletion_lock{read = max(0, Read - 1)}}
    end),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Releases lock in write mode.
%% @end
%%-------------------------------------------------------------------
-spec release_write_lock(id()) -> ok.
release_write_lock(FileUuid) ->
    {ok, _} = update(FileUuid, fun(Lock = #replica_deletion_lock{write = Write}) ->
        {ok, Lock#replica_deletion_lock{write = max(0, Write - 1)}}
    end),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% @equiv create_or_update(Key, Diff, new_doc(Key, read)).
%% @end
%%-------------------------------------------------------------------
-spec read_create_or_update(id(), diff()) -> {ok, doc()} | {error, term()}.
read_create_or_update(Key, Diff) ->
    create_or_update(Key, Diff, new_doc(Key, read)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% @equiv create_or_update(Key, Diff, new_doc(Key, write)).
%% @end
%%-------------------------------------------------------------------
-spec write_create_or_update(id(), diff()) -> {ok, doc()} | {error, term()}.
write_create_or_update(Key, Diff) ->
    create_or_update(Key, Diff, new_doc(Key, write)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% @equiv datastore_model:update(?CTX, Key, Diff).
%% @end
%%-------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(Key, Diff) ->
    datastore_model:update(?CTX, Key, Diff).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% @equiv datastore_model:update(?CTX, Key, Diff, new_doc(Key)).
%% @end
%%-------------------------------------------------------------------
-spec create_or_update(id(), diff(), doc()) -> {ok, doc()} | {error, term()}.
create_or_update(Key, Diff, NewDoc) ->
    datastore_model:update(?CTX, Key, Diff, NewDoc).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Helper function for ?MODULE:acquire_read_lock/1.
%% @end
%%-------------------------------------------------------------------
-spec acquire_read_lock_diff(lock()) -> {ok, lock()}.
acquire_read_lock_diff(Lock = #replica_deletion_lock{read = Read, write = 0}) ->
    {ok, Lock#replica_deletion_lock{read = Read + 1}};
acquire_read_lock_diff(#replica_deletion_lock{}) ->
    {error, lock_not_available}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Helper function for ?MODULE:acquire_write_lock/1.
%% @end
%%-------------------------------------------------------------------
-spec acquire_write_lock_diff(lock()) -> {ok, lock()}.
acquire_write_lock_diff(Lock = #replica_deletion_lock{read = 0, write = 0}) ->
    {ok, Lock#replica_deletion_lock{write = 1}};
acquire_write_lock_diff(#replica_deletion_lock{}) ->
    {error, lock_not_available}.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns empty doc() with given Key.
%% @end
%%-------------------------------------------------------------------
-spec new_doc(id(), atom()) -> doc().
new_doc(Key, read) ->
    #document{
        key = Key,
        value = #replica_deletion_lock{read = 1}
    };
new_doc(Key, write) ->
    #document{
        key = Key,
        value = #replica_deletion_lock{write = 1}
    }.
%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {read, integer},
        {write, integer}
    ]}.