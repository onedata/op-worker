%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Persistent model for harvest_stream gen_server. Holds the map of
%%% last seen sequence number, for each index, so that the stream
%%% knows where to resume in case of a crash.
%%% Entries in this model are stored per pair {HarvesterId, SpaceId}.
%%% The model stores also maximal, already processed sequence number,
%%% out of custom_metadata documents, which allows to track
%%% progress of harvesting.
%%% @end
%%%-------------------------------------------------------------------
-module(harvest_stream_state).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([get_seq/2, get_max_relevant_seq/1, set_seq/4, id/2]).

%% datastore_model callbacks
-export([get_record_struct/1]).

-type id() :: binary().
-type record() :: #harvest_stream_state{}.
-type doc() :: datastore_doc:doc(record()).

-type update_mode() :: relevant | ignored.

-export_type([id/0, record/0]).

-define(CTX, #{model => ?MODULE}).
-define(DEFAULT_SEQ, 0).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns last seen sequence number for given IndexId.
%% @end
%%--------------------------------------------------------------------
-spec get_seq(id(), od_harvester:index()) -> couchbase_changes:seq().
get_seq(Id, IndexId) ->
    case datastore_model:get(?CTX, Id) of
        {ok, #document{value = #harvest_stream_state{seen_seqs = Sequences}}} ->
            maps:get(IndexId, Sequences, ?DEFAULT_SEQ);
        {error, not_found} ->
            ?DEFAULT_SEQ
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns max successfully processed sequence number of relevant
%% (custom_metadata) document.
%% @end
%%--------------------------------------------------------------------
-spec get_max_relevant_seq(id()) -> couchbase_changes:seq().
get_max_relevant_seq(Id) ->
    case datastore_model:get(?CTX, Id) of
        {ok, #document{value = HSS}} ->
            HSS#harvest_stream_state.max_relevant_seq;
        {error, not_found} ->
            ?DEFAULT_SEQ
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets last seen sequence number, for given IndexId, to given value.
%% If UpdateMode =:= relevant, checks whether Seq is greater than
%% current Max and updates it if necessary.
%% If UpdateMode =:= ignored, the check is not performed
%% @end
%%--------------------------------------------------------------------
-spec set_seq(id(), od_harvester:index(),
    couchbase_changes:seq(), update_mode()) -> ok | {error, term()}.
set_seq(Id, IndexId, Seq, UpdateMode) ->
    ?extract_ok(datastore_model:update(?CTX, Id,
        fun(#harvest_stream_state{max_relevant_seq = MaxSeq, seen_seqs = Sequences}) ->
            {ok, #harvest_stream_state{
                max_relevant_seq = max_relevant_seq(Seq, MaxSeq, UpdateMode),
                seen_seqs = Sequences#{IndexId => Seq}
            }}
        end,
        default_doc(Id, IndexId, Seq, UpdateMode)
    )).

-spec id(od_harvester:id(), od_space:id()) -> id().
id(HarvesterId, SpaceId) ->
    datastore_utils:gen_key(HarvesterId, SpaceId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec default_doc(id(), od_harvester:index(), couchbase_changes:seq(),
    update_mode()) -> doc().
default_doc(Id, IndexId, Seq, UpdateMode) ->
    #document{
        key = Id,
        value = #harvest_stream_state{
            seen_seqs = #{IndexId => Seq},
            max_relevant_seq = max_relevant_seq(Seq, ?DEFAULT_SEQ, UpdateMode)
    }}.

-spec max_relevant_seq(couchbase_changes:seq(), couchbase_changes:seq(),
    update_mode()) -> couchbase_changes:seq().
max_relevant_seq(_NewSeq, CurrentMaxRelevantSeq, ignored) ->
    CurrentMaxRelevantSeq;
max_relevant_seq(NewSeq, CurrentMaxRelevantSeq, relevant) when NewSeq =< CurrentMaxRelevantSeq ->
    CurrentMaxRelevantSeq;
max_relevant_seq(NewSeq, _CurrentMaxRelevantSeq, relevant) ->
    NewSeq.

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
        {max_relevant_seq, integer},
        {seen_seqs, #{string => integer}}
    ]}.

