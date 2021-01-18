%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module for processing and storing information about pending sequences.
%%% Pending sequence is sequence number of remote provider that
%%% appeared in dbsync_in_stream but has not been yet processed by dbsync_out_stream.
%%% Typically, pending sequence is added by dbsync_in_stream and deleted by
%%% dbsync_out_stream. However, it is possible (after provider restart) that
%%% a sequence number appears in dbsync_out_stream before appearing in dbsync_in_stream.
%%% It is possible when document holding sequence number is saved before provider's
%%% stop but dbsync does not finish processing batch. Such sequence number is called
%%% raced sequence and is handled differently (it is added by function that processes
%%% dbsync_out_stream sequences instead of function processing dbsync_in_stream sequences
%%% as it is in case of pending sequences).
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_pending_seqs).
-author("Michal Wrzeszcz").

-include("modules/dbsync/dbsync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

%% API
-export([process_dbsync_in_stream_seqs/3, process_dbsync_out_stream_seqs/4]).
-export([get_first_pending_seq/2]).

% TODO VFS-7036 - export to silence dialyzer
-export([delete_raced_seqs/3]).

% As documents from different datastore models can use the same key,
% it is required to produce unique key used inside module.
% This concept is similar to datastore's unique key. However, datastore's
% unique key is used also for routing, thus lighter function for unique key's
% construction can be used here.
-type unique_key() :: binary().
-type link() :: {datastore_doc:remote_seq(), unique_key()}.

-define(KEY_SEPARATOR, "###").
-define(TREE_ID(ProviderID), <<ProviderID/binary, ?KEY_SEPARATOR, "dbsync_pending_seqs">>).
-define(RACED_TREE_ID(ProviderID), <<ProviderID/binary, ?KEY_SEPARATOR, "dbsync_raced_pending_seqs">>).
-define(CTX, dbsync_state:get_ctx()). % Pending sequences are saved as part of dbsync_state model

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Function that processes results of remote changes application. The function adds links that
%% represent remote sequences waiting to be processed by dbsync_out_stream (to connect remote
%% sequences with local sequences - see dbsync_seqs_correlation module). The function also deletes
%% information about sequences that have been overridden as they can be aggregated by couchbase
%% and not appear in dbsync_out_stream. In case of error during remote changes application, the
%% function handles them calling handle_dbsync_in_stream_errors function. Sequences previously
%% marked as raced are not saved as pending because they already appeared in dbsync_out_stream.
%% @end
%%--------------------------------------------------------------------
-spec process_dbsync_in_stream_seqs(od_space:id(), oneprovider:id(),
    dbsync_changes:dbsync_application_result()) -> ok.
process_dbsync_in_stream_seqs(SpaceId, ProviderId,
    #dbsync_application_result{
        successful = Applied,
        erroneous = AppliedWithError
    }
) ->
    Updated = Applied ++ AppliedWithError,

    PendingLinksToSave = lists:map(fun(#remote_mutation_info{
        key = Key,
        model = RecordType,
        new_seq = Seq
    }) ->
        {Seq, unique_key(Key, RecordType)}
    end, Updated),
    save_pending_seqs(SpaceId, ProviderId, PendingLinksToSave),

    RacedSeqs = lists:filtermap(fun(#remote_mutation_info{
        key = Key,
        model = Model,
        new_seq = Seq
    }) ->
        case is_raced_seq(SpaceId, ProviderId, Seq) of
            true -> {true, #remote_sequence_info{seq = Seq, key = Key, model = Model}};
            false -> false
        end
    end, Updated),

    OverriddenSeqs = lists:filtermap(fun
        (#remote_mutation_info{overridden_seq = undefined}) ->
            false;
        (#remote_mutation_info{
            key = Key,
            model = Model,
            overridden_seq = Seq
        }) ->
            {true, #remote_sequence_info{seq = Seq, key = Key, model = Model}}
    end, Applied),
    check_and_delete_pending_seqs(SpaceId, ProviderId, RacedSeqs ++ OverriddenSeqs),

    handle_dbsync_in_stream_errors(SpaceId, ProviderId, AppliedWithError).

%%--------------------------------------------------------------------
%% @doc
%% Function that removes pending sequences that appeared in dbsync_out_stream. If pending
%% sequence is not found, it is marked as raced sequence. It means that the sequence has
%% not appeared in process_dbsync_in_stream_seqs function, probably because of error, and
%% has to be treated differently when it finally appears in process_dbsync_in_stream_seqs
%% function.
%% @end
%%--------------------------------------------------------------------
-spec process_dbsync_out_stream_seqs(od_space:id(), oneprovider:id(),
    [dbsync_out_stream:remote_sequence_info()], datastore_doc:seq()) -> ok.
process_dbsync_out_stream_seqs(SpaceId, ProviderId, ProcessedRemoteSequences, LocalSeq) ->
    % TODO VFS-7036 - choose only for sequences lesser that current sequence in dbsync_state
    RacedSeqs = lists:filtermap(fun(#remote_sequence_info{seq = Seq}) ->
        case Seq >= LocalSeq of
            true -> {true, Seq};
            false -> false
        end
    end, ProcessedRemoteSequences),

    % TODO VFS-7036 - delete raced sequences when they are not needed anymore
    save_raced_seqs(SpaceId, ProviderId, RacedSeqs),
    check_and_delete_pending_seqs(SpaceId, ProviderId, ProcessedRemoteSequences).

%%%===================================================================
%%% Save/delete/get pending seqs to memory
%%%===================================================================

-spec get_first_pending_seq(od_space:id(), oneprovider:id()) -> datastore_doc:remote_seq() | undefined.
get_first_pending_seq(SpaceId, ProviderId) ->
    {ok, First} = datastore_model:fold_links(?CTX, SpaceId, ?TREE_ID(ProviderId),
        fun(#link{name = Seq}, _Acc) -> {ok, Seq} end, undefined, #{size => 1}),
    First.

-spec save_pending_seqs(od_space:id(), oneprovider:id(), [link()]) -> ok.
save_pending_seqs(SpaceId, ProviderId, Links) ->
    lists:foreach(fun(Link) ->
        case datastore_model:add_links(?CTX, SpaceId, ?TREE_ID(ProviderId), Link) of
            {ok, _} -> ok;
            {error, already_exists} -> ok
        end
    end, Links).

-spec check_and_delete_pending_seqs(od_space:id(), oneprovider:id(), [dbsync_out_stream:remote_sequence_info()]) -> ok.
check_and_delete_pending_seqs(SpaceId, ProviderId, RemoteSeqInfos) ->
    NotFoundSeqs = lists:foldl(fun(#remote_sequence_info{seq = Seq, key = Key, model = Model}, Acc) ->
        case datastore_model:get_links(?CTX, SpaceId, ?TREE_ID(ProviderId), Seq) of
            {ok, _} ->
                ok = datastore_model:delete_links(?CTX, SpaceId, ?TREE_ID(ProviderId), Seq),
                Acc;
            {error, not_found} ->
                Acc#{unique_key(Key, Model) => Seq}
        end
    end, #{}, RemoteSeqInfos),

    case maps:size(NotFoundSeqs) of
        0 ->
            ok;
        _ ->
            ToDel = get_overlapping_pending_seqs(SpaceId, ProviderId, NotFoundSeqs),
            delete_pending_seqs(SpaceId, ProviderId, ToDel)
    end.

-spec delete_pending_seqs(od_space:id(), oneprovider:id(), [datastore_doc:remote_seq()]) -> ok.
delete_pending_seqs(SpaceId, ProviderId, Seqs) ->
    lists:foreach(fun(Seq) ->
        ok = datastore_model:delete_links(?CTX, SpaceId, ?TREE_ID(ProviderId), Seq)
    end, Seqs).

-spec get_overlapping_pending_seqs(od_space:id(), oneprovider:id(), #{unique_key() => datastore_doc:remote_seq()}) ->
    [datastore_doc:remote_seq()].
get_overlapping_pending_seqs(SpaceId, ProviderId, SeqsWithErrorMap) ->
    {ok, Overlapping} = datastore_model:fold_links(?CTX, SpaceId, ?TREE_ID(ProviderId),
        fun(#link{name = Seq, target = Target}, Acc) ->
            case maps:get(Target, SeqsWithErrorMap, undefined) of
                undefined -> {ok, Acc};
                Number when Number >= Seq -> {ok, [Seq | Acc]};
                _ -> {ok, Acc}
            end
        end, [], #{}),

    lists:reverse(Overlapping).

%%%===================================================================
%%% Save/delete/check raced seqs
%%%===================================================================

-spec save_raced_seqs(od_space:id(), oneprovider:id(), [datastore_doc:remote_seq()]) -> ok.
save_raced_seqs(SpaceId, ProviderId, Seqs) ->
    lists:foreach(fun(Seq) ->
        case datastore_model:add_links(?CTX, SpaceId, ?RACED_TREE_ID(ProviderId), {Seq, Seq}) of
            {ok, _} -> ok;
            {error, already_exists} -> ok
        end
    end, Seqs).

-spec delete_raced_seqs(od_space:id(), oneprovider:id(), [datastore_doc:remote_seq()]) -> ok.
delete_raced_seqs(SpaceId, ProviderId, Seqs) ->
    lists:foreach(fun(Seq) ->
        ok = datastore_model:delete_links(?CTX, SpaceId, ?RACED_TREE_ID(ProviderId), Seq)
    end, Seqs).

-spec is_raced_seq(od_space:id(), oneprovider:id(), datastore_doc:remote_seq()) -> boolean().
is_raced_seq(SpaceId, ProviderId, Seq) ->
    case datastore_model:get_links(?CTX, SpaceId, ?RACED_TREE_ID(ProviderId), Seq) of
        {ok, _} -> true;
        {error, not_found} -> false
    end.

%%%===================================================================
%%% Helper functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Function that handles errors that appeared during remote changes application.
%% It gets unique keys of documents that have been applied with error and searches
%% for pending sequences that have previously appeared applying these remote documents.
%% Next, it removes found sequences that are lesser than sequences in applied docs
%% (error can prevent their removal with standard mechanism).
%% @end
%%--------------------------------------------------------------------
-spec handle_dbsync_in_stream_errors(od_space:id(), oneprovider:id(),
    [dbsync_changes:remote_mutation_info()]) -> ok.
handle_dbsync_in_stream_errors(SpaceId, ProviderId, AppliedWithError) ->
    SeqsWithErrorMap = maps:from_list(lists:map(fun(#remote_mutation_info{
        key = Key,
        model = RecordType,
        new_seq = Seq}
    ) ->
        {unique_key(Key, RecordType), Seq}
    end, AppliedWithError)),

    case maps:size(SeqsWithErrorMap) of
        0 ->
            ok;
        _ ->
            ToDel = get_overlapping_pending_seqs(SpaceId, ProviderId, SeqsWithErrorMap),
            delete_pending_seqs(SpaceId, ProviderId, ToDel)
    end.

-spec unique_key(datastore_doc:key(), atom()) -> unique_key().
unique_key(Key, RecordType) ->
    <<(atom_to_binary(RecordType, utf8))/binary, ?KEY_SEPARATOR, Key/binary>>.