%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Persistent state of DBSync worker. For each space it holds mapping from
%%% provider to a sequence number and timestamp of the beginning of expected
%%% changes range. It also holds information about correlation between
%%% sequences of different providers.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_state).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([delete/1, get_seq/2, get_sync_progress/1, get_sync_progress/2, set_sync_progress/4,
    get_seqs_correlations/1, custom_update/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type record() :: #dbsync_state{}.
-type sync_progress() :: #{od_provider:id() => {datastore_doc:seq(), datastore_doc:timestamp()}}.

-export_type([record/0, sync_progress/0]).

-define(CTX, #{model => ?MODULE}).
-define(DEFAULT_SEQ, 1).
-define(DEFAULT_TIMESTAMP, 0).

%%%===================================================================
%%% API
%%%===================================================================

-spec delete(od_space:id()) -> ok | {error, term()}.
delete(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId).

-spec get_seq(od_space:id(), od_provider:id()) -> datastore_doc:seq().
get_seq(SpaceId, ProviderId) ->
    {Seq, _Timestamp} = get_sync_progress(SpaceId, ProviderId),
    Seq.

-spec get_sync_progress(od_space:id()) -> sync_progress().
get_sync_progress(SpaceId) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = #dbsync_state{sync_progress = SyncProgress}}} ->
            SyncProgress;
        {error, not_found} ->
            #{}
    end.

-spec get_sync_progress(od_space:id(), od_provider:id()) -> {datastore_doc:seq(), datastore_doc:timestamp()}.
get_sync_progress(SpaceId, ProviderId) ->
    SyncProgressMap = get_sync_progress(SpaceId),
    maps:get(ProviderId, SyncProgressMap, {?DEFAULT_SEQ, ?DEFAULT_TIMESTAMP}).

-spec set_sync_progress(od_space:id(), od_provider:id(), datastore_doc:seq(),
    dbsync_changes:timestamp() | undefined) -> ok | {error, Reason :: term()}.
set_sync_progress(SpaceId, ProviderId, Seq, Timestamp) ->
    {Diff, Default} = case Timestamp of
        undefined ->
            DiffFun = fun(#dbsync_state{sync_progress = SyncProgress} = State) ->
                % Use old timestamp (no doc with timestamp in applied range)
                {_, Timestamp2} = maps:get(ProviderId, SyncProgress, {?DEFAULT_SEQ, ?DEFAULT_TIMESTAMP}),
                {ok, State#dbsync_state{sync_progress = maps:put(ProviderId, {Seq, Timestamp2}, SyncProgress)}}
            end,
            {DiffFun, #dbsync_state{sync_progress = #{ProviderId => {Seq, ?DEFAULT_TIMESTAMP}}}};
        _ ->
            DiffFun = fun(#dbsync_state{sync_progress = SyncProgress} = State) ->
                {ok, State#dbsync_state{sync_progress = maps:put(ProviderId, {Seq, Timestamp}, SyncProgress)}}
            end,
            {DiffFun, #dbsync_state{sync_progress = #{ProviderId => {Seq, Timestamp}}}}
    end,

    case datastore_model:update(?CTX, SpaceId, Diff, Default) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

-spec get_seqs_correlations(od_space:id()) -> dbsync_seqs_correlation:providers_correlations().
get_seqs_correlations(SpaceId) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = #dbsync_state{seqs_correlations = Correlations}}} ->
            Correlations;
        {error, not_found} ->
            #{}
    end.

-spec custom_update(od_space:id(), datastore_doc:diff(record())) ->
    {ok, datastore_doc:diff(record())} | {error, term()}.
custom_update(SpaceId, Diff) ->
    datastore_model:update(?CTX, SpaceId, Diff).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    3.

-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {seq, #{string => integer}}
    ]};
get_record_struct(2) ->
    {record, [
        {seq, #{string => {integer, integer}}}
    ]};
get_record_struct(3) ->
    {record, [
        % Field `seq` has been renamed to `sync_progress`
        {sync_progress, #{string => {integer, integer}}},
        % Added fields:
        {seqs_correlations, #{string => {record, [
            {local_of_last_remote, integer},
            {remote_consecutively_processed_max, integer},
            {remote_with_doc_processed_max, integer}
        ]}}},
        {correlation_persisting_seq, integer}
    ]}.

-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, Map}) ->
    Map2 = maps:map(fun(_ProviderId, Seq) -> {Seq, ?DEFAULT_TIMESTAMP} end, Map),
    {2, {?MODULE, Map2}};
upgrade_record(2, {?MODULE, Seq}) ->
    {
        3,
        {
            ?MODULE,
            % Renamed field:
            Seq,
            % Added fields:
            #{},
            0
        }
    }.