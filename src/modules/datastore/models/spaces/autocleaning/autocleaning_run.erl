%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model for holding information about auto-cleaning runs.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_run).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: binary().
-type status() :: active | completed | failed.
-type record() :: #autocleaning_run{}.
-type diff() :: datastore_doc:diff(record()).
-type doc() :: datastore_doc:doc(record()).
-type error() :: {error, term()}.

-export_type([id/0, status/0]).

%% API
-export([get/1, update/2, delete/1, delete/2, create/2,
    mark_completed/1, mark_failed/1, mark_released_file/2, set_index_token/2,
    get_index_token/1, get_bytes_to_release/1, get_released_bytes/1, is_finished/1, get_started_at/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-define(CTX, #{
    model => ?MODULE,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(id()) -> {ok, doc()} | error().
get(ARId) ->
    datastore_model:get(?CTX, ARId).

-spec update(id(), diff()) -> {ok, doc()} | error().
update(ARId, UpdateFun) ->
    datastore_model:update(?CTX, ARId, UpdateFun).

-spec delete(id()) -> ok.
delete(ARId) ->
    ok = datastore_model:delete(?CTX, ARId).

-spec delete(id(), od_space:id()) -> ok.
delete(ARId, SpaceId) ->
    delete(ARId, SpaceId, get_started_at(ARId)).

-spec delete(id(), od_space:id(), non_neg_integer()) -> ok.
delete(ARId, SpaceId, StartedAtTimestamp) ->
    autocleaning_run_links:delete_link(ARId, SpaceId, StartedAtTimestamp),
    delete(ARId).

-spec create(od_space:id(), non_neg_integer()) -> {ok, doc()} | {error, term()}.
create(SpaceId, BytesToRelease) ->
    NewDoc = #document{
        scope = SpaceId,
        value = #autocleaning_run{
            status = active,
            space_id = SpaceId,
            started_at = time_utils:cluster_time_seconds(),
            bytes_to_release = BytesToRelease
        }
    },
    datastore_model:create(?CTX, NewDoc).

-spec mark_released_file(undefined | id(), non_neg_integer()) -> ok.
mark_released_file(undefined, _) -> ok;
mark_released_file(ARId, Size) ->
    ok = ?extract_ok(update(ARId, fun(AC = #autocleaning_run{
        released_files = ReleasedFiles,
        released_bytes = ReleasedBytes
    }) ->
        {ok, AC#autocleaning_run{
            released_files = ReleasedFiles + 1,
            released_bytes = ReleasedBytes + Size
        }}
    end)).

-spec mark_failed(undefined | id()) -> ok | error().
mark_failed(undefined) -> ok;
mark_failed(ARId) ->
    case update(ARId, fun(AC) ->
        {ok, AC#autocleaning_run{
            stopped_at = time_utils:cluster_time_seconds(),
            status = failed
        }}
    end) of
        {ok, #document{value = #autocleaning_run{space_id = SpaceId}}} ->
            autocleaning:mark_run_finished(SpaceId);
        Error ->
            ?error_stacktrace("Fail to mark auto-cleaning run ~p as failed due to ~p",
                [ARId, Error]),
            Error
    end.

-spec mark_completed(undefined | id()) -> ok | error().
mark_completed(undefined) -> ok;
mark_completed(ARId) ->
    case update(ARId, fun(AC) ->
        {ok, AC#autocleaning_run{
            stopped_at = time_utils:cluster_time_seconds(),
            status = completed
        }}
    end) of
        {ok, #document{value = #autocleaning_run{space_id = SpaceId}}} ->
            autocleaning:mark_run_finished(SpaceId);
        Error ->
            ?error("Fail to mark auto-cleaning run ~p as completed due to ~p",
                [ARId, Error]),
            Error
    end.

-spec set_index_token(id(), file_popularity_view:index_token()) -> ok.
set_index_token(ARId, IndexToken) ->
    ok = ?extract_ok(update(ARId, fun(AC) ->
        {ok, AC#autocleaning_run{index_token = IndexToken}}
    end)).

-spec get_index_token(record()) -> file_popularity_view:index_token().
get_index_token(#autocleaning_run{index_token = IndexToken}) ->
    IndexToken.

-spec get_bytes_to_release(record()) -> non_neg_integer().
get_bytes_to_release(#autocleaning_run{bytes_to_release = BytesToRelease}) ->
    BytesToRelease.

-spec get_released_bytes(record()) -> non_neg_integer().
get_released_bytes(#autocleaning_run{released_bytes = ReleasedBytes}) ->
    ReleasedBytes.

-spec get_started_at(record() | id()) -> non_neg_integer().
get_started_at(#autocleaning_run{started_at = StartedAt}) ->
    StartedAt;
get_started_at(ARId) ->
    {ok, #document{value = AR}} = autocleaning_run:get(ARId),
    get_started_at(AR).

-spec is_finished(record()) -> boolean().
is_finished(#autocleaning_run{status = active}) -> false;
is_finished(#autocleaning_run{}) -> true.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
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
        {space_id, string},
        {started_at, integer},
        {stopped_at, integer},
        {status, atom},
        {released_bytes, integer},
        {bytes_to_release, integer},
        {released_files, integer},
        {index_token, {record, [
            {last_doc_id, string},
            {last_key, string},
            {end_key, string}
        ]}}
    ]}.