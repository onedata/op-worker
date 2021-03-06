%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Persistent state of DBSync worker. For each space it holds mapping from
%%% provider to a sequence number of the beginning of expected changes range.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_state).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([delete/1, get_seq/2, get_seq_and_timestamp/2, set_seq_and_timestamp/4]).

%% datastore_model callbacks
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes dbsync state for a space.
%% @end
%%--------------------------------------------------------------------
-spec delete(od_space:id()) -> ok | {error, term()}.
delete(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Returns sequence number of the beginning of expected changes range
%% from given space and provider.
%% @end
%%--------------------------------------------------------------------
-spec get_seq(od_space:id(), od_provider:id()) -> couchbase_changes:seq().
get_seq(SpaceId, ProviderId) ->
    {Seq, _Timestamp} = get_seq_and_timestamp(SpaceId, ProviderId),
    Seq.

%%--------------------------------------------------------------------
%% @doc
%% Returns sequence number and timestamp of the beginning of expected changes range
%% from given space and provider.
%% @end
%%--------------------------------------------------------------------
-spec get_seq_and_timestamp(od_space:id(), od_provider:id()) -> {couchbase_changes:seq(), datastore_doc:timestamp()}.
get_seq_and_timestamp(SpaceId, ProviderId) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = #dbsync_state{seq = Seq}}} ->
            maps:get(ProviderId, Seq, {1, 0});
        {error, not_found} ->
            {1, 0}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets sequence number and timestamp of the beginning of expected changes range
%% from given space and provider.
%% @end
%%--------------------------------------------------------------------
-spec set_seq_and_timestamp(od_space:id(), od_provider:id(), couchbase_changes:seq(),
    dbsync_changes:timestamp() | undefined) -> ok | {error, Reason :: term()}.
set_seq_and_timestamp(SpaceId, ProviderId, Number, Timestamp) ->
    {Diff, Default} = case Timestamp of
        undefined ->
            DiffFun = fun(#dbsync_state{seq = Seq} = State) ->
                % Use old timestamp (no doc with timestamp in applied range)
                {_, Timestamp2} = maps:get(ProviderId, Seq, {1, 0}),
                {ok, State#dbsync_state{seq = maps:put(ProviderId, {Number, Timestamp2}, Seq)}}
            end,
            {DiffFun, #dbsync_state{seq = #{ProviderId => {Number, 0}}}};
        _ ->
            DiffFun = fun(#dbsync_state{seq = Seq} = State) ->
                {ok, State#dbsync_state{seq = maps:put(ProviderId, {Number, Timestamp}, Seq)}}
            end,
            {DiffFun, #dbsync_state{seq = #{ProviderId => {Number, Timestamp}}}}
    end,

    case datastore_model:update(?CTX, SpaceId, Diff, Default) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

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
        {seq, #{string => integer}}
    ]};
get_record_struct(2) ->
    {record, [
        {seq, #{string => {integer, integer}}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, Map}
) ->
    Map2 = maps:map(fun(_ProviderId, Number) -> {Number, 0} end, Map),
    {2, {?MODULE, Map2}}.