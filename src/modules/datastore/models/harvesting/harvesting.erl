%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Persistent model for harvesting_stream gen_server. Holds the map of
%%% last seen sequence number, for each index, so that the stream
%%% knows where to resume in case of a crash.
%%% Entries in this model are stored per pair {HarvesterId, SpaceId}.
%%% The model stores also maximal, already processed sequence number,
%%% out of custom_metadata documents, which allows to track
%%% progress of harvesting.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/harvesting/harvesting.hrl").


%% API
-export([create/1, ensure_created/1, get/1, delete/1]).
-export([main_identifier/1, aux_identifier/3, resolve_aux_identifier/1]).
-export([get_seen_seq/1, get_seen_seq/2, get_seen_seq/3]).
-export([get_main_seen_seq/1]).
-export([set_seen_seq/3, set_aux_seen_seq/4, set_main_seq/2]).
-export([delete_history_entries/3]).

%% datastore_model callbacks
-export([get_record_struct/1]).

-type id() :: od_space:id().
-type hid() :: #hid{}.
-type record() :: #harvesting{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, hid/0, record/0]).

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

-spec get(id() | hid()) -> {ok, doc()} | {error, term()}.
get(#hid{id = SpaceId}) ->
    harvesting:get(SpaceId);
get(SpaceId) ->
    datastore_model:get(?CTX, SpaceId).

delete(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId).


-spec main_identifier(id()) -> hid().
main_identifier(SpaceId) ->
    #hid{id = SpaceId, label = ?MAIN_LABEL}.

-spec aux_identifier(id(), od_harvester:id(), od_harvester:index()) -> hid().
aux_identifier(SpaceId, HarvesterId, IndexId) ->
    #hid{id = SpaceId, label = ?AUX_LABEL(HarvesterId, IndexId)}.

-spec resolve_aux_identifier(hid()) -> {od_harvester:id(), od_harvester:index()}.
resolve_aux_identifier(#hid{label = ?AUX_LABEL(HarvesterId, IndexId)}) ->
    {HarvesterId, IndexId}.


-spec get_main_seen_seq(id() | hid() | record() | doc()) ->
    {ok, couchbase_changes:seq()} | {error, term()}.
get_main_seen_seq(#harvesting{main_seen_seq = SeenSeq}) ->
    {ok, SeenSeq};
get_main_seen_seq(#hid{id = SpaceId}) ->
    get_main_seen_seq(SpaceId);
get_main_seen_seq(#document{value = Harvesting}) ->
    get_main_seen_seq(Harvesting);
get_main_seen_seq(SpaceId) ->
    case harvesting:get(SpaceId) of
        {ok, Doc} -> get_main_seen_seq(Doc);
        Error -> Error
    end.

-spec get_seen_seq(hid()) -> {ok, couchbase_changes:seq()}.
get_seen_seq(HI = #hid{id = SpaceId}) ->
    case harvesting:get(SpaceId) of
        {ok, Doc} ->
            get_seen_seq(Doc, HI);
        {error, not_found} ->
            {ok, ?DEFAULT_HARVESTING_SEQ}
    end.

-spec get_seen_seq(id() | record() | doc(), hid()) -> {ok, couchbase_changes:seq()}.
get_seen_seq(#document{value = HS}, HI) ->
    get_seen_seq(HS, HI);
get_seen_seq(#harvesting{main_seen_seq = Seq}, #hid{label = ?MAIN_LABEL}) ->
    {ok, Seq};
get_seen_seq(Harvesting = #harvesting{}, #hid{label = ?AUX_LABEL(HarvesterId, IndexId)}) ->
    get_seen_seq(Harvesting, HarvesterId, IndexId).

-spec get_seen_seq(id() | record() | doc(), od_harvester:id(),
    od_harvester:index()) -> {ok, couchbase_changes:seq()}.
get_seen_seq(#harvesting{history = History}, HarvesterId, IndexId) ->
    {ok, harvesting_history:get(HarvesterId, IndexId, History)};
get_seen_seq(#document{value = Harvesting}, HarvesterId, IndexId) ->
    get_seen_seq(Harvesting, HarvesterId, IndexId);
get_seen_seq(SpaceId, HarvesterId, IndexId) when is_binary(SpaceId) ->
    get_seen_seq(aux_identifier(SpaceId, HarvesterId, IndexId)).


-spec set_main_seq(hid(), couchbase_changes:seq()) -> ok | {error, term()}.
set_main_seq(#hid{id = SpaceId}, NewSeq) ->
    set_seen_seq_internal(SpaceId, harvesting_destination:init(), NewSeq, true).

-spec set_aux_seen_seq(od_space:id(), od_harvester:id(),
    od_harvester:index() | [od_harvester:index()], couchbase_changes:seq()) -> ok | {error, term()}.
set_aux_seen_seq(SpaceId, HarvesterId, Indices, NewSeq) ->
    Destination = harvesting_destination:init(HarvesterId, Indices),
    set_seen_seq_internal(SpaceId, Destination, NewSeq, false).

-spec set_seen_seq(hid(), harvesting_destination:destination(),
    couchbase_changes:seq()) -> ok | {error, term()}.
set_seen_seq(HI = #hid{id = SpaceId}, Destination, NewSeq) ->
    set_seen_seq_internal(SpaceId, Destination, NewSeq, is_main(HI)).

-spec delete_history_entries(id(), od_harvester:id(),
    od_harvester:index() | [od_harvester:index()]) -> ok.
delete_history_entries(SpaceId, HarvesterId, Indices) ->
    Result = ?extract_ok(update(SpaceId, fun(HS = #harvesting{history = History}) ->
        {ok, HS#harvesting{
            history = harvesting_history:delete(HarvesterId, Indices, History)
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
        value = #harvesting{
            main_seen_seq = ?DEFAULT_HARVESTING_SEQ,
            history = harvesting_history:init()
        }
    }).

-spec update(id(), datastore_doc:diff()) -> {ok, doc()} | {error, term()}.
update(SpaceId, Diff) ->
    datastore_model:update(?CTX, SpaceId, Diff).

-spec set_seen_seq_internal(id(), harvesting_destination:destination(),
    couchbase_changes:seq(), boolean()) -> ok | {error, term()}.
set_seen_seq_internal(SpaceId, Destination, NewSeq, BumpMaxSeq) ->
    ?extract_ok(update(SpaceId, fun(HS = #harvesting{
        history = History,
        main_seen_seq = MainSeenSeq
    }) ->
        History2 = harvesting_destination:fold(fun(HarvesterId, Indices, HistoryIn) ->
            lists:foldl(fun(IndexId, HistoryIn2) ->
                harvesting_history:set(HarvesterId, IndexId, NewSeq, HistoryIn2)
            end, HistoryIn, Indices)
        end, History, Destination),
        {ok, HS#harvesting{
            history = History2,
            main_seen_seq = case BumpMaxSeq of
                true -> NewSeq;
                false -> MainSeenSeq
            end
        }}
    end)).

-spec is_main(hid()) -> boolean().
is_main(#hid{label = ?MAIN_LABEL}) -> true;
is_main(#hid{label = ?AUX_LABEL(_, _)}) -> false.

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
        {history, #{term => integer}}  % key is a tuple {string, string}
    ]}.