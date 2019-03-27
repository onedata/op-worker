%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Persistent model for harvest_stream gen_server. Holds the last change seq
%%% that was successfully processed, so that the stream know where to resume in
%%% case of a crash.
%%% @end
%%%-------------------------------------------------------------------
-module(harvest_stream_state).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([get_seq/2,get_max_metadata_seq/1,
    set_seq/3, set_seq_and_maybe_bump_max_metadata_seq/3, id/2]).

%% datastore_model callbacks
-export([get_record_struct/1]).

-type id() :: binary().
-type record() :: #harvest_stream_state{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0]).

-define(CTX, #{model => ?MODULE}).
-define(DEFAULT_SEQ, 0).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns last successfully processed sequence number.
%% @end
%%--------------------------------------------------------------------
-spec get_seq(id(), od_harvester:index()) -> couchbase_changes:seq().
get_seq(Id, IndexId) ->
    case datastore_model:get(?CTX, Id) of
        {ok, #document{value = #harvest_stream_state{sequences = Sequences}}} ->
            maps:get(IndexId, Sequences, ?DEFAULT_SEQ);
        {error, not_found} ->
            ?DEFAULT_SEQ
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns last successfully processed sequence number.
%% @end
%%--------------------------------------------------------------------
-spec get_max_metadata_seq(id()) -> couchbase_changes:seq().
get_max_metadata_seq(Id) ->
    case datastore_model:get(?CTX, Id) of
        {ok, #document{value = #harvest_stream_state{max_metadata_seq = MaxMetadataSeq}}} ->
            MaxMetadataSeq;
        {error, not_found} ->
            ?DEFAULT_SEQ
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets last successfully processed sequence number to given value.
%% @end
%%--------------------------------------------------------------------
-spec set_seq(id(), od_harvester:index(), couchbase_changes:seq()) ->
    ok | {error, term()}.
set_seq(Id, IndexId, Seq) ->
    ?extract_ok(datastore_model:update(?CTX, Id,
        fun(#harvest_stream_state{sequences = Sequences}) ->
            {ok, #harvest_stream_state{sequences = Sequences#{IndexId => Seq}}}
        end,
        default_doc(Id, IndexId, Seq, false)
    )).

%%--------------------------------------------------------------------
%% @doc
%% Sets last successfully processed sequence number to given value.
%% Increases
%% @end
%%--------------------------------------------------------------------
-spec set_seq_and_maybe_bump_max_metadata_seq(id(), od_harvester:index(), couchbase_changes:seq()) ->
    ok | {error, term()}.
set_seq_and_maybe_bump_max_metadata_seq(Id, IndexId, Seq) ->
    ?extract_ok(datastore_model:update(?CTX, Id,
        fun(#harvest_stream_state{max_metadata_seq = MaxSeq, sequences = Sequences}) ->
            {ok, #harvest_stream_state{
                max_metadata_seq = max(MaxSeq, Seq),
                sequences = Sequences#{IndexId => Seq}
            }}
        end,
        default_doc(Id, IndexId, Seq, true)
    )).

-spec id(od_harvester:id(), od_space:id()) -> id().
id(HarvesterId, SpaceId) ->
    datastore_utils:gen_key(HarvesterId, SpaceId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec default_doc(id(), od_harvester:index(), couchbase_changes:seq(),
    ShouldSetMaxMetadataSeq :: boolean()) -> doc().
default_doc(Id, IndexId, Seq, true) ->
    #document{
        key = Id,
        value = #harvest_stream_state{
            max_metadata_seq = Seq,
            sequences = #{IndexId => Seq}
        }
    };
default_doc(Id, IndexId, Seq, false) ->
    #document{
        key = Id,
        value = #harvest_stream_state{sequences = #{IndexId => Seq}}
    }.

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
        {max_metadata_seq, integer},
        {sequences, #{string => integer}}
    ]}.

