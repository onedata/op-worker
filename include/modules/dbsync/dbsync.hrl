%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definitions and records used by modules
%%% connected with dbsync.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(DBSYNC_HRL).
-define(DBSYNC_HRL, 1).

%%%===================================================================
%%% Macros
%%%===================================================================

-define(DBSYNC_WORKER_SUP, dbsync_worker_sup).
-define(IN_STREAM_ID(Id), {dbsync_in_stream, Id}).
-define(OUT_STREAM_ID(Id), {dbsync_out_stream, Id}).

%%%===================================================================
%%% Records
%%%===================================================================

% Record describing result of application of dbsync documents batch.
% Sequences applied with error are sequences of documents which application
% on datastore returned remote_doc_already_exists error. Such sequences are
% additionally analyzed when processing by dbsync_pending_seqs module to find
% all pending sequences that could be lost as a result of the error.
-record(dbsync_application_result, {
    % TODO VFS-7036 - verify if lists have to be sorted
    successful = [] :: [dbsync_changes:remote_mutation_info()],
    erroneous = [] :: [dbsync_changes:remote_mutation_info()],
    min_erroneous_seq :: datastore_doc:remote_seq() | undefined
}).

% Record describing remote sequence. The sequence number is extended with
% information about document (key and model) that was connected with the sequence.
-record(remote_sequence_info, {
    key :: datastore_doc:key(),
    model :: datastore_model:model(),
    seq :: datastore_doc:remote_seq()
}).

% Record describing correlation between local sequence and sequences of other provider.
% Separate record is stored to describe correlation with each provider's sequences.
% The correlation is created by dbsync_seqs_correlation module processing remote sequence numbers.
% As the processing consists of two stages, 2 remote sequence numbers have to be stored - see
% dbsync_seqs_correlation module. `remote_continuously_processed_max` represents  maximal continuously
% processed remote sequence number. As order of sequences of other providers may be changed when
% flushing documents from memory to db, it is guaranteed that all remote sequences
% up to `remote_continuously_processed_max` field value have appeared in dbsync_out_stream or have been ignored.
% However, there can be also some sequences grater than this value that have already appeared. It is guaranteed
% that no sequence grater than `remote_with_doc_processed_max` field value has appeared in dbsync_out_stream.
% In case of this field `with_doc` means that this sequence number appeared in dbsync_out_stream with document
% as some sequence numbers can be connected to ignored documents (see dbsync_seqs_correlations_history:save_correlation
% function).
%
% E.g. if `remote_continuously_processed_max = 10` and `remote_with_doc_processed_max = 20` than document with remote
% sequence value `20` has already appeared in dbsync_out_stream but there is at least one sequence grater
% than `10` and smaller than `20` that has been saved to local database but has not appeared in dbsync_out_stream.
% If `local_on_last_remote` is 100 than it is guaranteed that document with local sequence 100 has been
% modified by the remote provider and all documents already processed by dbsync_out_stream with local sequences
% grater than 100 have not been modified by remote provider.
-record(sequences_correlation, {
    % TODO VFS-7030 - test if changes can be requested before any correlation appears.
    % Should default be 0 or 1?
    local_on_last_remote = 0 :: datastore_doc:seq(),
    remote_continuously_processed_max = 0 :: datastore_doc:remote_seq(),
    remote_with_doc_processed_max = 0 :: datastore_doc:remote_seq()
}).

-endif.
