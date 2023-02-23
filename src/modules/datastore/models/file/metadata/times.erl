%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding files' times.
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
-export([get_or_default/1, get/1, create_or_update/2, delete/1,
    save/1, save/5, save_with_current_times/3, unset_ignore_in_changes/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

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
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Get times or return zeroes.
%% @end
%%--------------------------------------------------------------------
-spec get_or_default(file_meta:uuid()) -> {ok, times()} | {error, term()}.
get_or_default(FileUuid) ->
    case times:get(FileUuid) of
        {ok, #document{value = #times{
            atime = ATime, ctime = CTime, mtime = MTime
        }}} ->
            {ok, {ATime, CTime, MTime}};
        {error, not_found} ->
            {ok, {0, 0, 0}};
        Error ->
            Error
    end.

-spec save(file_meta:uuid(), od_space:id(), a_time(), m_time(), c_time()) -> ok | {error, term()}.
save(FileUuid, SpaceId, ATime, MTime, CTime) ->
    ?extract_ok(save(#document{
        key = FileUuid,
        value = #times{
            atime = ATime,
            mtime = MTime,
            ctime = CTime
        },
        scope = SpaceId}
    )).

%%--------------------------------------------------------------------
%% @doc
%% Saves permission cache.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, doc()} | {error, term()}.
save(#document{key = Key} = Doc) ->
    datastore_model:save(?CTX#{generated_key => true},
        Doc#document{key = fslogic_file_id:ensure_referenced_uuid(Key)}).

-spec save_with_current_times(file_meta:uuid(), od_space:id(), boolean()) -> {ok, time()} | {error, term()}.
save_with_current_times(FileUuid, SpaceId, IgnoreInChanges) ->
    Time = global_clock:timestamp_seconds(),
    SaveAns = datastore_model:save(
        ?CTX#{generated_key => true},
        #document{
            key = fslogic_file_id:ensure_referenced_uuid(FileUuid),
            value = #times{
                atime = Time,
                mtime = Time,
                ctime = Time
            },
            scope = SpaceId,
            ignore_in_changes = IgnoreInChanges
        }
    ),

    case SaveAns of
        {ok, _} -> {ok, Time};
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(doc(), diff()) ->
    {ok, doc()} | {error, term()}.
create_or_update(#document{key = Key, value = Default}, Diff) ->
    datastore_model:update(?CTX, fslogic_file_id:ensure_referenced_uuid(Key), Diff, Default).

%%--------------------------------------------------------------------
%% @doc
%% Returns permission cache.
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Uuid) ->
    datastore_model:get(?CTX, fslogic_file_id:ensure_referenced_uuid(Uuid)).

%%--------------------------------------------------------------------
%% @doc
%% Deletes permission cache.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(FileUuid) ->
    datastore_model:delete(?CTX, fslogic_file_id:ensure_referenced_uuid(FileUuid)).


-spec unset_ignore_in_changes(file_meta:uuid()) -> ok.
unset_ignore_in_changes(Key) ->
    {ok, _} = datastore_model:update(?CTX#{ignore_in_changes => false}, Key, fun(Record) ->
        {ok, Record} % Return unchanged record, ignore_in_changes will be unset because of flag in CTX
    end),
    ok.


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
        {atime, integer},
        {ctime, integer},
        {mtime, integer}
    ]}.