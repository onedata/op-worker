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
-include("modules/autocleaning/autocleaning.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/traverse/view_traverse.hrl").


-type id() :: binary().
-type status() :: ?ACTIVE | ?CANCELLING | ?COMPLETED | ?FAILED | ?CANCELLED.
-type record() :: #autocleaning_run{}.
-type diff() :: datastore_doc:diff(record()).
-type doc() :: datastore_doc:doc(record()).
-type error() :: {error, term()}.

-export_type([id/0, status/0, record/0, doc/0]).

%% API
-export([get/1, update/2, delete/1, delete/2, create/2,
    mark_cancelling/1, mark_finished/1, mark_finished/3, mark_released_file/2, set_view_traverse_token/2,
    get_view_traverse_token/1, get_bytes_to_release/1, get_released_bytes/1, get_status/1, get_started_at/1,
    update_counters/3, get_released_files/1, is_active/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0, upgrade_record/2]).

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
            status = ?ACTIVE,
            space_id = SpaceId,
            started_at = global_clock:timestamp_seconds(),
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

-spec update_counters(undefined | id(), non_neg_integer(), non_neg_integer()) -> ok.
update_counters(undefined, _, _) -> ok;
update_counters(ARId, ReleasedFiles, ReleasedBytes) ->
    ok = ?extract_ok(update(ARId, fun(AC) ->
        {ok, AC#autocleaning_run{
            released_files = ReleasedFiles,
            released_bytes = ReleasedBytes
        }}
    end)).

-spec mark_cancelling(undefined | id()) -> ok | error().
mark_cancelling(undefined) -> ok;
mark_cancelling(ARId) ->
    update(ARId, fun(AC) -> {ok, AC#autocleaning_run{status = ?CANCELLING}} end).

-spec mark_finished(undefined | id()) -> ok | error().
mark_finished(undefined) -> ok;
mark_finished(ARId) ->
    case update(ARId, fun(ACR = #autocleaning_run{started_at = StartedAt}) ->
        ACR2 = ACR#autocleaning_run{
            stopped_at = global_clock:monotonic_timestamp_seconds(StartedAt)
        },
        {ok, set_final_status(ACR2)}
    end) of
        {ok, #document{value = #autocleaning_run{space_id = SpaceId}}} ->
            autocleaning:mark_run_finished(SpaceId);
        Error ->
            ?error("Fail to mark auto-cleaning run ~p as finished due to ~p",
                [ARId, Error]),
            Error
    end.

-spec mark_finished(undefined | id(), non_neg_integer(), non_neg_integer()) -> ok | error().
mark_finished(undefined, _, _) -> ok;
mark_finished(ARId, FinalReleasedFiles, FinalReleasedBytes) ->
    case update(ARId, fun(ACR = #autocleaning_run{started_at = StartedAt}) ->
        ACR2 = ACR#autocleaning_run{
            stopped_at = global_clock:monotonic_timestamp_seconds(StartedAt),
            released_files = FinalReleasedFiles,
            released_bytes = FinalReleasedBytes
        },
        {ok, set_final_status(ACR2)}
    end) of
        {ok, #document{value = #autocleaning_run{space_id = SpaceId}}} ->
            autocleaning:mark_run_finished(SpaceId);
        Error ->
            ?error("Fail to mark auto-cleaning run ~p as finished due to ~p",
                [ARId, Error]),
            Error
    end.

-spec set_view_traverse_token(id(), view_traverse:token()) -> ok.
set_view_traverse_token(ARId, Token) ->
    ok = ?extract_ok(update(ARId, fun(AC) ->
        {ok, AC#autocleaning_run{view_traverse_token = Token}}
    end)).

-spec get_view_traverse_token(record()) -> view_traverse:token().
get_view_traverse_token(#autocleaning_run{view_traverse_token = Token}) ->
    Token.

-spec get_bytes_to_release(record()) -> non_neg_integer().
get_bytes_to_release(#autocleaning_run{bytes_to_release = BytesToRelease}) ->
    BytesToRelease.

-spec get_released_bytes(record()) -> non_neg_integer().
get_released_bytes(#autocleaning_run{released_bytes = ReleasedBytes}) ->
    ReleasedBytes.

-spec get_released_files(record()) -> non_neg_integer().
get_released_files(#autocleaning_run{released_files = ReleasedFiles}) ->
    ReleasedFiles.

-spec get_started_at(record() | id()) -> non_neg_integer().
get_started_at(#autocleaning_run{started_at = StartedAt}) ->
    StartedAt;
get_started_at(ARId) ->
    {ok, #document{value = AR}} = autocleaning_run:get(ARId),
    get_started_at(AR).

-spec get_status(record()) -> status().
get_status(#autocleaning_run{status = Status}) ->
    Status.


-spec is_active(id() | record() | doc()) -> boolean().
is_active(#autocleaning_run{status = ?ACTIVE}) ->
    true;
is_active(#autocleaning_run{}) ->
    false;
is_active(#document{value = AR = #autocleaning_run{}}) ->
    is_active(AR);
is_active(ARId) ->
    case autocleaning_run:get(ARId) of
        {ok, Doc} ->
            is_active(Doc);
        {error, not_found} ->
            false
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec set_final_status(record()) -> record().
set_final_status(ACR = #autocleaning_run{
    status = ?ACTIVE,
    released_bytes = ReleasedBytes,
    bytes_to_release = BytesToRelease
}) when ReleasedBytes >= BytesToRelease ->
    ACR#autocleaning_run{status = ?COMPLETED};
set_final_status(ACR = #autocleaning_run{status = ?ACTIVE}) ->
    ACR#autocleaning_run{status = ?FAILED};
set_final_status(ACR = #autocleaning_run{status = ?CANCELLING}) ->
    ACR#autocleaning_run{status = ?CANCELLED}.

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
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.

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
    ]};
get_record_struct(2) ->
    {record, [
        {space_id, string},
        {started_at, integer},
        {stopped_at, integer},
        {status, atom},
        {released_bytes, integer},
        {bytes_to_release, integer},
        {released_files, integer},
        {view_traverse_token, {record, [
            {offset, integer},
            {last_doc_id, string},
            {last_start_key, term}
        ]}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, SpaceId, StartedAt, StoppedAt, Status,
    ReleasedBytes, BytesToRelease, ReleasedFiles, _IndexToken
}) ->
    {2, {?MODULE, SpaceId, StartedAt, StoppedAt, Status, ReleasedBytes,
        BytesToRelease, ReleasedFiles,
        % index token was wrongly persisted, therefore we can ignore it
        #view_traverse_token{}
    }}.