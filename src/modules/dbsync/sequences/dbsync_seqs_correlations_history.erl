%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible fo storing (using links) history of past correlations
%%% between sequences of different providers. This history is used
%%% to calculate start/stop sequences for dbsync streams when batch of
%%% changes of other (remote) provider is requested. Such history is saved
%%% for each provider separately using links. Links ids are remote sequence
%%% numbers saved in ascending and descending order as trees are searched
%%% using different orders to find beginning and ending of requested range
%%% (there is no guarantee that link for given remote sequence is saved so
%%% appropriate margin has to be used if the link does not exist - for
%%% beginning of the range we take its predecessor and for end of the range
%%% we use its successor). Each link's value encodes two sequence numbers,
%%% that describe correlation at the time of link creation:
%%%    - first is local sequence number of last processed document mutated
%%%      previously by remote provider,
%%%    - second is current local sequence number.
%%% As local sequence numbers are constantly ascending, second number encoded
%%% in link's value is always greater than the first. Usage of the second value
%%% allows reduction of sequences range when only changes of single provider
%%% are requested. Using second value, sequences between first and second value
%%% are skipped. It can be done because it is guaranteed that there are no
%%% documents mutated by particular remote provider in this range.
%%% Such range reduction is done when TrimSkipped parameter is  'true'
%%% during mapping (see map_remote_seq_to_local_start_seq function).
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_seqs_correlations_history).
-author("Michal Wrzeszcz").

-include("modules/dbsync/dbsync.hrl").

% API
-export([add/3]).
-export([map_remote_seq_to_local_start_seq/4, map_remote_seq_to_local_stop_params/3]).

-type encoded_local_sequences_range() :: binary().

% All links connected with single space are saved using following key
-define(KEY(SpaceId), <<SpaceId/binary, "###_correlations">>).
-define(CTX, dbsync_state:get_ctx()). % History is saved as part of dbsync_state model

% Macros used for encoding/decoding links' values
-define(ENCODED_SEQUENCE_RANGE_BEGINNING, "#last_mutation_sequence#").
-define(ENCODED_SEQUENCE_RANGE_ENDING, "#not_mutateded_until#").

%%%===================================================================
%%% Mapping remote <-> local sequences
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Finds local sequence number closest to remote number using descending sequences tree.
%% Parameter TrimSkipped allows reduction of sequences range when only changes of single
%% provider are requested (see main doc of the module).
%% @end
%%--------------------------------------------------------------------
-spec map_remote_seq_to_local_start_seq(od_space:id(), od_provider:id(),
    datastore_doc:remote_seq() | dbsync_processed_seqs_history:encoded_seqs(), boolean()) ->
    datastore_doc:seq().
map_remote_seq_to_local_start_seq(_SpaceId, _ProviderId, <<>>, _TrimSkipped) ->
    1; % Empty binary - start from the first sequence
map_remote_seq_to_local_start_seq(SpaceId, _ProviderId, RemoteSeqNums, TrimSkipped) when is_binary(RemoteSeqNums) ->
    % Several remote sequences encoded in binary - analyse them all
    SeqsMap = dbsync_processed_seqs_history:decode(RemoteSeqNums),
    maps:fold(fun
        (ProviderId, RemoteSeqNum, undefined) ->
            map_remote_seq_to_local_start_seq(SpaceId, ProviderId, RemoteSeqNum, TrimSkipped);
        (ProviderId, RemoteSeqNum, Acc) ->
            LocalSeq = map_remote_seq_to_local_start_seq(SpaceId, ProviderId, RemoteSeqNum, TrimSkipped),
            min(LocalSeq, Acc)
    end, undefined, SeqsMap);
map_remote_seq_to_local_start_seq(SpaceId, ProviderId, RemoteSeqNum, TrimSkipped) ->
    case oneprovider:get_id() of
        ProviderId ->
            RemoteSeqNum;
        _ ->
            EncodedSequence =
                dbsync_seqs_tree:get_next(descending, ?CTX, ?KEY(SpaceId), ProviderId, RemoteSeqNum, <<>>),
            get_local_sequence_from_encoded_range(EncodedSequence, TrimSkipped)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Finds (in sequences history) local sequence number closest to remote number using ascending sequences tree.
%% Additionally returns message to be included in final batch. Such message has to be created starting request
%% as it is not guaranteed that links required to create such message will be available handling last doc
%% of the request. If such links are not available, the message can be created using map that was used during
%% calculation of local sequence number. The map can evolve in time so only possibility to use the map is
%% right after use of map as it is frozen in local variable.
%% @end
%%--------------------------------------------------------------------
-spec map_remote_seq_to_local_stop_params(od_space:id(), od_provider:id(), datastore_doc:remote_seq()) ->
    {datastore_doc:seq(), dbsync_processed_seqs_history:encoded_seqs()}.
map_remote_seq_to_local_stop_params(SpaceId, ProviderId, RemoteSeqNum) ->
    % TODO VFS-7034 - should batch size be limited by dbsync_changes_max_request_size env variable?
    LocalProviderId = oneprovider:get_id(),
    case LocalProviderId of
        ProviderId ->
            SyncProgress = dbsync_state:get_sync_progress(SpaceId),
            case maps:get(ProviderId, SyncProgress, {0, 0}) of
                {LastNum, _} when LastNum =< RemoteSeqNum ->
                    EncodedProcessedSequences = dbsync_processed_seqs_history:get(SpaceId, LastNum),
                    {LastNum, EncodedProcessedSequences};
                _ ->
                    % TODO VFS-7031 - change from inclusive to exclusive end (+ 1 will not be used)
                    EncodedProcessedSequences = dbsync_processed_seqs_history:get(SpaceId, RemoteSeqNum),
                    {RemoteSeqNum + 1, EncodedProcessedSequences}
            end;
        _ ->
            case dbsync_seqs_tree:get_next(?CTX, ?KEY(SpaceId), ProviderId, RemoteSeqNum, <<>>) of
                <<>> ->
                    SyncProgress = dbsync_state:get_sync_progress(SpaceId),
                    {LocalSeq, _} = maps:get(LocalProviderId, SyncProgress, {0, 0}),
                    EncodedProcessedSequences = dbsync_processed_seqs_history:get(SpaceId, LocalSeq),
                    {LocalSeq, EncodedProcessedSequences};
                EncodedSequences ->
                    LocalSeq = get_local_sequence_from_encoded_range(EncodedSequences, false),
                    % TODO VFS-7031 - change from inclusive to exclusive end (+ 1 will not be used)
                    % Check also if batch ends on sequence until -1
                    % TODO VFS-7031 - update sequence number of provider ProviderId in EncodedProcessedSequences
                    % to RemoteSeqNum
                    EncodedProcessedSequences = dbsync_processed_seqs_history:get(SpaceId, LocalSeq),
                    {LocalSeq + 1, EncodedProcessedSequences}
            end
    end.

%%%===================================================================
%%% History saving API
%%%===================================================================

-spec add(od_space:id(), dbsync_seqs_correlation:correlations(), datastore_doc:seq()) -> ok.
add(SpaceId, SeqsCorrelation, CurrentLocalSeq) ->
    lists:foreach(fun({RemoteProviderID,
        #sequences_correlation{
            local_on_last_remote = LocalOnLastRemote,
            remote_with_doc_processed_max = RemoteWithDocProcessedMax,
            remote_continuously_processed_max = RemoteContinuouslyProcessedMax
        }}) ->
        save_correlation(SpaceId, RemoteProviderID, LocalOnLastRemote, CurrentLocalSeq,
            RemoteContinuouslyProcessedMax, RemoteWithDocProcessedMax)
    end, maps:to_list(SeqsCorrelation)).

%%%===================================================================
%%% History saving for single provider
%%%===================================================================

-spec save_correlation(od_space:id(), oneprovider:id(), datastore_doc:seq(),
    datastore_doc:seq(), datastore_doc:remote_seq(), datastore_doc:remote_seq()) -> ok.
save_correlation(SpaceId, ProviderId, LocalOnLastRemote, CurrentLocalSeq,
    RemoteContinuouslyProcessedMax, RemoteWithDocProcessedMax) ->
    % Add correlation used to find start sequence from which changes should be send after a request
    % for remote changes (used by map_remote_seq_to_local_start_seq fun).
    save_or_update_correlation(SpaceId, ProviderId, RemoteWithDocProcessedMax,
        LocalOnLastRemote, CurrentLocalSeq),
    case RemoteWithDocProcessedMax < RemoteContinuouslyProcessedMax of
        true ->
            % All remote sequences are saved up to value higher than maximal remote sequence that appeared
            % in dbsync_out_stream with document. Such situation is possible only when remote sequence counter
            % is updated applying changes from remote provider without application of documents (all documents
            % are ignored). In such a case additional correlation is saved allowing more precise determining of
            % local starting sequence in future.
            save_or_update_correlation(
                SpaceId, ProviderId, RemoteContinuouslyProcessedMax, LocalOnLastRemote, CurrentLocalSeq);
        false ->
            ok
    end,

    % Add correlation used to find stop sequence up to which changes should be send after a request
    % for remote changes (used by map_remote_seq_to_local_stop_params fun).
    dbsync_seqs_tree:add_new(?CTX, ?KEY(SpaceId), ProviderId, RemoteContinuouslyProcessedMax,
        encode_local_sequences_range(LocalOnLastRemote, CurrentLocalSeq)).

-spec save_or_update_correlation(od_space:id(), oneprovider:id(), datastore_doc:remote_seq(),
    datastore_doc:seq(), datastore_doc:seq()) -> ok.
save_or_update_correlation(SpaceId, ProviderId, RemoteSeq, LocalOnLastRemote, CurrentLocalSeq) ->
    % Only current local sequence can be modified in existing link. Such update means that no changes higher 
    % than remote sequence from remote provider have been processed since last update of link.
    % Local sequence of last remote mutation should not be changed. (TODO VFS-7035 - test it)
    UpdateFun = fun
        (undefined) ->
            encode_local_sequences_range(LocalOnLastRemote, CurrentLocalSeq);
        (PrevValue) ->
            PrevLocalOnLastRemote = get_local_sequence_from_encoded_range(PrevValue, false),
            encode_local_sequences_range(PrevLocalOnLastRemote, CurrentLocalSeq)
    end,
    dbsync_seqs_tree:overwrite(descending, ?CTX, ?KEY(SpaceId), ProviderId, RemoteSeq, UpdateFun).

%%%===================================================================
%%% Encoding/decoding link values
%%%===================================================================

-spec encode_local_sequences_range(datastore_doc:seq(), datastore_doc:seq()) -> encoded_local_sequences_range().
encode_local_sequences_range(LocalOnLastRemote, NotMutatedUntil) ->
    <<?ENCODED_SEQUENCE_RANGE_BEGINNING, (integer_to_binary(LocalOnLastRemote))/binary,
        ?ENCODED_SEQUENCE_RANGE_ENDING, (integer_to_binary(NotMutatedUntil))/binary>>.

-spec get_local_sequence_from_encoded_range(encoded_local_sequences_range(), boolean()) -> datastore_doc:seq().
get_local_sequence_from_encoded_range(<<>>, _TrimSkipped) ->
    1; % TODO VFS-7030 - maybe 0?
get_local_sequence_from_encoded_range(<<?ENCODED_SEQUENCE_RANGE_BEGINNING, EncodedLocalSequences/binary>>, false) ->
    [LocalSeq, _] = binary:split(EncodedLocalSequences, <<?ENCODED_SEQUENCE_RANGE_ENDING>>),
    binary_to_integer(LocalSeq);
get_local_sequence_from_encoded_range(<<?ENCODED_SEQUENCE_RANGE_BEGINNING, EncodedLocalSequences/binary>>, true) ->
    [_, LocalSeq] = binary:split(EncodedLocalSequences, <<?ENCODED_SEQUENCE_RANGE_ENDING>>),
    binary_to_integer(LocalSeq).