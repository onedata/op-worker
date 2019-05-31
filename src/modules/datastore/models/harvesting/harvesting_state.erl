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
%%% Stores maximal seen sequence for current main_harvesting_stream
%%% and harvesting_progress structure, which tracks progress of harvesting
%%% per each pair {HarvesterId, IndexId} in given space.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_state).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/harvesting/harvesting.hrl").


%% API
-export([create/1, ensure_created/1, get/1, delete/1]).
-export([get_seen_seq/1, get_seen_seq/2, get_seen_seq/3, get_main_seen_seq/1]).
-export([set_seen_seq/3, set_main_seq/2, set_aux_seen_seq/4]).
-export([delete_progress_entries/3]).

%% datastore_model callbacks
-export([get_record_struct/1]).

-type id() :: od_space:id().
-type record() :: #harvesting_state{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0]).

-define(CTX, #{model => ?MODULE}).

-define(MAIN_LABEL, main).
-define(AUX_LABEL(HarvesterId, IndexId), {HarvesterId, IndexId}).

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

delete(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId).

-spec get_main_seen_seq(id() | record() | doc()) -> {ok, couchbase_changes:seq()} | {error, term()}.
get_main_seen_seq(#document{value = HarvestingState}) ->
    get_main_seen_seq(HarvestingState);
get_main_seen_seq(#harvesting_state{main_seen_seq = MainSeq}) ->
    {ok, MainSeq};
get_main_seen_seq(SpaceId) ->
    case harvesting_state:get(SpaceId) of
        {ok, Doc} ->
            get_main_seen_seq(Doc);
        {error, _} = Error ->
            Error
    end.

-spec get_seen_seq(harvesting_stream:name()) ->
    {ok, couchbase_changes:seq()} | {error, term()}.
get_seen_seq(StreamName = ?MAIN_HARVESTING_STREAM(SpaceId)) ->
    case harvesting_state:get(SpaceId) of
        {ok, Doc} ->
            get_seen_seq(Doc, StreamName);
        {error, _} = Error ->
            Error
    end;
get_seen_seq(StreamName = ?AUX_HARVESTING_STREAM(SpaceId, _, _)) ->
    case harvesting_state:get(SpaceId) of
        {ok, Doc} ->
            get_seen_seq(Doc, StreamName);
        {error, _} = Error ->
            Error
    end.

-spec get_seen_seq(doc() | record(), harvesting_stream:name()) ->
    {ok, couchbase_changes:seq()}.
get_seen_seq(#document{value = HS}, StreamName) ->
    get_seen_seq(HS, StreamName);
get_seen_seq(#harvesting_state{main_seen_seq = Seq}, ?MAIN_HARVESTING_STREAM(_SpaceId)) ->
    {ok, Seq};
get_seen_seq(HarvestingState = #harvesting_state{},
    ?AUX_HARVESTING_STREAM(_SpaceId, HarvesterId, IndexId)
) ->
    get_seen_seq(HarvestingState, HarvesterId, IndexId).

-spec get_seen_seq(doc() | record(), od_harvester:id(), od_harvester:index()) ->
    {ok, couchbase_changes:seq()}.
get_seen_seq(#document{value = HarvestingState}, HarvesterId, IndexId) ->
    get_seen_seq(HarvestingState, HarvesterId, IndexId);
get_seen_seq(#harvesting_state{progress = Progress}, HarvesterId, IndexId) ->
    {ok, harvesting_progress:get(HarvesterId, IndexId, Progress)}.


-spec set_main_seq(id(), couchbase_changes:seq()) -> ok | {error, term()}.
set_main_seq(SpaceId, NewSeq) ->
    set_seen_seq_internal(SpaceId, harvesting_destination:init(), NewSeq, true).

-spec set_aux_seen_seq(od_space:id(), od_harvester:id(),
    od_harvester:index() | [od_harvester:index()], couchbase_changes:seq()) -> ok | {error, term()}.
set_aux_seen_seq(SpaceId, HarvesterId, Indices, NewSeq) ->
    Destination = harvesting_destination:init(HarvesterId, Indices),
    set_seen_seq_internal(SpaceId, Destination, NewSeq, false).

-spec set_seen_seq(id() | harvesting_stream:name(), harvesting_destination:destination(),
    couchbase_changes:seq()) -> ok | {error, term()}.
set_seen_seq(?MAIN_HARVESTING_STREAM(SpaceId), Destination, NewSeq) ->
    set_seen_seq_internal(SpaceId, Destination, NewSeq, true);
set_seen_seq(?AUX_HARVESTING_STREAM(SpaceId, _, _), Destination, NewSeq) ->
    set_seen_seq_internal(SpaceId, Destination, NewSeq, false);
set_seen_seq(SpaceId, Destination, NewSeq) when is_binary(SpaceId) ->
    set_seen_seq_internal(SpaceId, Destination, NewSeq, false).


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
        value = #harvesting_state{
            main_seen_seq = ?DEFAULT_HARVESTING_SEQ,
            progress = harvesting_progress:init()
        }
    }).

-spec update(id(), datastore_doc:diff()) -> {ok, doc()} | {error, term()}.
update(SpaceId, Diff) ->
    datastore_model:update(?CTX, SpaceId, Diff).

-spec set_seen_seq_internal(id(), harvesting_destination:destination(),
    couchbase_changes:seq(), boolean()) -> ok | {error, term()}.
set_seen_seq_internal(SpaceId, Destination, NewSeq, BumpMaxSeq) ->
    ?extract_ok(update(SpaceId, fun(HS = #harvesting_state{
        progress = Progress,
        main_seen_seq = MainSeenSeq
    }) ->
        Progress2 = harvesting_destination:fold(fun(HarvesterId, Indices, ProgressIn) ->
            lists:foldl(fun(IndexId, ProgressIn2) ->
                harvesting_progress:set(HarvesterId, IndexId, NewSeq, ProgressIn2)
            end, ProgressIn, Indices)
        end, Progress, Destination),
        {ok, HS#harvesting_state{
            progress = Progress2,
            main_seen_seq = case BumpMaxSeq of
                true -> NewSeq;
                false -> MainSeenSeq
            end
        }}
    end)).

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
        {main_seen_seq, integer},
        {progress, #{term => integer}}  % key is a tuple {string, string}
    ]}.