%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Persistent model for tracking state of harvesting in each harvested
%%% space. Documents in this model are stored per space.
%%% It stores harvesting_progress:progress() structure, which stores
%%% the highest seen sequence for each pair {HarvesterId, IndexId} in given
%%% space.
%%%
%%% NOTE!!!
%%% If you introduce any changes in this module, please ensure that
%%% docs in {@link harvesting_stream} module are up to date.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_state).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/harvesting/harvesting.hrl").


%% API
-export([create/1, ensure_created/1, get/1, delete/1]).
-export([get_seen_seq/3]).
-export([set_seen_seq/3, set_aux_seen_seq/4]).
-export([delete_progress_entries/3]).

%% datastore_model callbacks
-export([get_record_struct/1]).

-type id() :: od_space:id().
-type record() :: #harvesting_state{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0, doc/0]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

-spec ensure_created(id()) -> ok.
ensure_created(SpaceId) ->
    case datastore_model:exists(?CTX, SpaceId) of
        {ok, true} ->
            ok;
        {ok, false} ->
            case create(SpaceId) of
                {ok, _} -> ok;
                {error, already_exists} -> ok
            end
    end.

-spec get(id()) -> {ok, doc()} | {error, term()}.
get(SpaceId) ->
    datastore_model:get(?CTX, SpaceId).

-spec delete(od_space:id()) -> ok | {error, term()}.
delete(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId).

-spec get_seen_seq(id() |doc() | record(), od_harvester:id(), od_harvester:index()) ->
    {ok, couchbase_changes:seq()} | {error, term()}.
get_seen_seq(#document{value = HarvestingState}, HarvesterId, IndexId) ->
    get_seen_seq(HarvestingState, HarvesterId, IndexId);
get_seen_seq(#harvesting_state{progress = Progress}, HarvesterId, IndexId) ->
    {ok, harvesting_progress:get(HarvesterId, IndexId, Progress)};
get_seen_seq(SpaceId, HarvesterId, IndexId) ->
    case harvesting_state:get(SpaceId) of
        {ok, Doc} ->
            get_seen_seq(Doc, HarvesterId, IndexId);
        {error, _} = Error ->
            Error
    end.

-spec set_aux_seen_seq(od_space:id(), od_harvester:id(),
    od_harvester:index() | [od_harvester:index()], couchbase_changes:seq()) -> ok | {error, term()}.
set_aux_seen_seq(SpaceId, HarvesterId, Indices, NewSeq) ->
    Destination = harvesting_destination:init(HarvesterId, Indices),
    set_seen_seq(SpaceId, Destination, NewSeq).

-spec set_seen_seq(id(), harvesting_destination:destination(),
    couchbase_changes:seq()) -> ok | {error, term()}.
set_seen_seq(SpaceId, Destination, NewSeq) ->
    ?extract_ok(update(SpaceId, fun(HS = #harvesting_state{
        progress = Progress
    }) ->
        Progress2 = harvesting_destination:fold(fun(HarvesterId, Indices, ProgressIn) ->
            lists:foldl(fun(IndexId, ProgressIn2) ->
                harvesting_progress:set(HarvesterId, IndexId, NewSeq, ProgressIn2)
            end, ProgressIn, Indices)
        end, Progress, Destination),
        {ok, HS#harvesting_state{
            progress = Progress2
        }}
    end)).


-spec delete_progress_entries(id(), od_harvester:id(),
    od_harvester:index() | [od_harvester:index()]) -> ok.
delete_progress_entries(SpaceId, HarvesterId, Indices) ->
    Result = ?extract_ok(update(SpaceId, fun(HS = #harvesting_state{progress = Progress}) ->
        {ok, HS#harvesting_state{
            progress = harvesting_progress:delete(HarvesterId, Indices, Progress)
        }}
    end)),
    case Result of
        ok -> ok;
        {error, not_found} -> ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create(id()) -> {ok, doc()} | {error, term()}.
create(SpaceId) ->
    datastore_model:create(?CTX, #document{
        key = SpaceId,
        value = #harvesting_state{progress = harvesting_progress:init()}
    }).

-spec update(id(), datastore:diff()) -> {ok, doc()} | {error, term()}.
update(SpaceId, Diff) ->
    datastore_model:update(?CTX, SpaceId, Diff).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {progress, #{string => integer}}
    ]}.