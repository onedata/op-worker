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
-export([get_seq/1, set_seq/2, id/2]).

%% datastore_model callbacks
-export([get_record_struct/1]).

-type id() :: harvest_stream:id().
-type record() :: #harvest_stream_state{}.

-export_type([id/0, record/0]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns last successfully processed sequence number.
%% @end
%%--------------------------------------------------------------------
-spec get_seq(id()) -> couchbase_changes:seq().
get_seq(Id) ->
    case datastore_model:get(?CTX, Id) of
        {ok, #document{value = #harvest_stream_state{seq = Seq}}} ->
            Seq;
        {error, not_found} ->
            1
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets last successfully processed sequence number to given value.
%% @end
%%--------------------------------------------------------------------
-spec set_seq(id(), couchbase_changes:seq()) ->
    ok | {error, term()}.
set_seq(Id, Seq) ->
    ?extract_ok(datastore_model:save(?CTX, #document{
        key = Id, value = #harvest_stream_state{seq = Seq}
    })).


-spec id(od_harvester:id(), od_space:id()) -> id().
id(HarvesterId, SpaceId) ->
    datastore_utils:gen_key(HarvesterId, SpaceId).

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
        {seq, integer}
    ]}.

