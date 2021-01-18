%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for processing correlations between sequences of
%%% different providers during particular moment in time.
%%% The module analyses correlation between sequence numbers of different
%%% providers to allow requesting of other providers changes.
%%% Knowing correlation between sequences numbers it is possible to calculate
%%% local sequences' range that fully covers requested range of changes of
%%% remote provider (correlation determined by this module is used by
%%% dbsync_seqs_correlations_history module to determine requests' ranges).
%%%
%%% Sequences correlations are determined basing on sequences number stored in
%%% dbsync state and information about pending sequence numbers
%%% (see dbsync_pending_seqs.erl). Correlation is described by 3 sequence numbers
%%% within #sequences_correlation{} record (see dbsync.hrl). These sequence numbers
%%% had following meaning at the time when the record has been created:
%%%     - local sequence number (value of counter used by couchbase driver to
%%%       update seq field in documents),
%%%     - maximal remote sequence number connected with document
%%%       already processed by local provider's dbsync_out_stream,
%%%     - maximal consecutively processed remote sequence number (note that remote
%%%       sequence numbers can be saved to local database in different order and not
%%%       all sequence numbers appear in stream so difference between two
%%%       consecutive sequence numbers can be greater than 1).
%%% 2 remote sequence numbers have to be used as remote sequence
%%% numbers processing consists of two stages.
%%%     - During the first stage dbsync_in_stream saves remote documents to database
%%%       gathering information about remote sequence numbers to be processed.
%%%     - After saving of such document in couchbase, it appears in dbsync_out_stream
%%%       and remote sequence number is considered as processed.
%%% Such two step processing is required as documents order can changes when flushing
%%% from memory to couchbase. Thus, determining local sequences range that fully covers
%%% remote sequences range, it is required to analyse information gathered during both
%%% stages (see dbsync_seqs_correlations_history module, especially save_correlation function).
%%%
%%% Current correlations are stored inside dbsync_state document (this
%%% module triggers update of the document). History of correlations is persisted
%%% using links managed by dbsync_seqs_correlations_history and dbsync_processed_seqs_history
%%% modules (saving links is also triggered by this module).
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_seqs_correlation).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/dbsync/dbsync.hrl").
-include("modules/datastore/datastore_models.hrl").

-export([set_sequences_correlation/3]).

-type correlation() :: #sequences_correlation{}.
-type providers_correlations() :: #{oneprovider:id() => correlation()}.
-type consecutively_processed_sequences() :: #{oneprovider:id() => datastore_doc:seq()}.

-export_type([providers_correlations/0, consecutively_processed_sequences/0]).

% Interval defining how often correlations are added to history
-define(HISTORY_PERSISTING_INTERVAL,  op_worker:get_env(seq_persisting_interval, 10000)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Updates correlation in dbsync_state document. Saves also correlation history link if needed.
%% @end
%%--------------------------------------------------------------------
-spec set_sequences_correlation(od_space:id(), datastore_doc:seq(),
    dbsync_out_stream_batch_cache:mutations()) -> ok.
set_sequences_correlation(SpaceId, LocalSeq, RemoteMutations) ->
    % get sync progress from doc before update as it is not possible
    % to get pending sequences from the inside of update function
    InitialSyncProgress = dbsync_state:get_sync_progress(SpaceId),
    LocalProviderID = oneprovider:get_id(),
    
    process_pending_links(SpaceId, RemoteMutations, InitialSyncProgress),
    ProviderIds = maps:keys(InitialSyncProgress) -- [LocalProviderID],
    ConsecutivelyProcessedSequences = get_consecutively_processed_sequences(SpaceId, ProviderIds, InitialSyncProgress),
    
    Diff = fun(CurrentCorrelation) ->
        lists:foldl(fun(ProviderId, Acc) ->
            CorrelationToUpdate = maps:get(ProviderId, CurrentCorrelation, #sequences_correlation{}),
            RemoteMutationsCache = maps:get(ProviderId, RemoteMutations, undefined),
            ConsecutivelyProcessedSequence = maps:get(ProviderId, ConsecutivelyProcessedSequences),
            UpdatedCorrelation = update_correlation_record(RemoteMutationsCache, ConsecutivelyProcessedSequence, CorrelationToUpdate),
            Acc#{ProviderId => UpdatedCorrelation}
        end, #{}, ProviderIds)
    end,

    update_state_doc_and_history(SpaceId, LocalSeq, Diff).

%%%===================================================================
%%% Helper functions
%%%===================================================================

-spec process_pending_links(od_space:id(), dbsync_out_stream_batch_cache:mutations(),
    dbsync_state:sync_progress()) -> ok.
process_pending_links(SpaceId, RemoteMutations, SyncProgress) ->
    lists:foreach(fun({RemoteProviderId, ProviderMutations}) ->
        {CurrentRemoteSeq, _Timestamp} = maps:get(RemoteProviderId, SyncProgress, {0, 0}),
        RemoteSeqs = dbsync_out_stream_batch_cache:get_sequences(ProviderMutations),
        dbsync_pending_seqs:process_dbsync_out_stream_seqs(SpaceId, RemoteProviderId, RemoteSeqs, CurrentRemoteSeq)
    end, maps:to_list(RemoteMutations)).

-spec get_consecutively_processed_sequences(od_space:id(), [oneprovider:id()], dbsync_state:sync_progress()) ->
    consecutively_processed_sequences().
get_consecutively_processed_sequences(SpaceId, ProviderIds, SyncProgress) ->
    maps:from_list(lists:map(fun(ProviderId) ->
        % TODO VFS-7035 - test races with link operations by dbsync_in_stream
        case dbsync_pending_seqs:get_first_pending_seq(SpaceId, ProviderId) of
            undefined ->
                % No pending links - all sequences are processed up tu current seq
                {Seq, _Timestamp} = maps:get(ProviderId, SyncProgress),
                {ProviderId, Seq};
            FirstNotAppliedSeq ->
                {ProviderId, FirstNotAppliedSeq - 1}
        end
    end, ProviderIds)).

-spec update_correlation_record(dbsync_out_stream_batch_cache:provider_mutations() | undefined,
    datastore_doc:remote_seq(), correlation()) -> correlation().
update_correlation_record(undefined = _ProviderMutations, RemoteConsecutivelyProcessedSequence, CorrelationRecord) ->
    % Out stream cache is undefined - update only information created using pending sequences links
    CorrelationRecord#sequences_correlation{remote_consecutively_processed_max = RemoteConsecutivelyProcessedSequence};
update_correlation_record(ProviderMutations, RemoteConsecutivelyProcessedSequence,
    #sequences_correlation{remote_with_doc_processed_max = MaxRemoteSequence}) ->
    NewRemoteSeqs = dbsync_out_stream_batch_cache:get_sequences(ProviderMutations),
    MaxNewRemoteSequence = lists:max(lists:map(fun(Info) -> Info#remote_sequence_info.seq end, NewRemoteSeqs)),
    #sequences_correlation{
        local_of_last_remote = dbsync_out_stream_batch_cache:get_last_local_seq(ProviderMutations),
        remote_consecutively_processed_max = RemoteConsecutivelyProcessedSequence,
        remote_with_doc_processed_max = max(MaxNewRemoteSequence, MaxRemoteSequence)}.

-spec update_state_doc_and_history(od_space:id(), datastore_doc:seq(),
    fun((providers_correlations()) -> providers_correlations())) -> ok.
update_state_doc_and_history(SpaceId, LocalSeq, Diff) ->
    UpdatedDiff = fun(#dbsync_state{seqs_correlations = CurrentCorrelation} = StateDoc) ->
        UpdatedCorrelation = Diff(CurrentCorrelation),

        case UpdatedCorrelation =:= CurrentCorrelation of
            true ->
                {error, nothing_changed};
            false ->
                StateDoc2 = update_persisting_seq(StateDoc, LocalSeq),
                {ok, StateDoc2#dbsync_state{seqs_correlations = UpdatedCorrelation}}
        end
    end,
    
    case dbsync_state:custom_update(SpaceId, UpdatedDiff) of
        {ok, #document{value = #dbsync_state{correlation_persisting_seq = PersistingSeq,
            seqs_correlations = UpdatedCorrelations}}} ->
            add_to_history(SpaceId, UpdatedCorrelations, LocalSeq, PersistingSeq);
        {error, nothing_changed} ->
            ok;
        {error, not_found} ->
            ok
    end.

-spec update_persisting_seq(dbsync_state:record(), datastore_doc:seq()) -> dbsync_state:record().
update_persisting_seq(State = #dbsync_state{correlation_persisting_seq = LastPersistingSeq}, LocalSeq) ->
    Interval = ?HISTORY_PERSISTING_INTERVAL,
    case LocalSeq - LastPersistingSeq >= Interval of
        true -> State#dbsync_state{correlation_persisting_seq = LocalSeq};
        false -> State
    end.

%%--------------------------------------------------------------------
%% @doc
%% Adds new entry to history if current local sequence is marked as sequence to be flushed.
%% @end
%%--------------------------------------------------------------------
-spec add_to_history(od_space:id(), dbsync_seqs_correlation:providers_correlations(), datastore_doc:seq(),
    datastore_doc:seq()) -> ok.
add_to_history(SpaceId, ProvidersCorrelations, CurrentLocalSeq, CurrentLocalSeq) ->
    dbsync_seqs_correlations_history:add(SpaceId, ProvidersCorrelations, CurrentLocalSeq),
    % Save sequences to process for current local sequence
    dbsync_processed_seqs_history:add(SpaceId, ProvidersCorrelations, CurrentLocalSeq);
add_to_history(_SpaceId, _ProvidersCorrelations, _CurrentLocalSeq, _FlushSeq) ->
    ok.