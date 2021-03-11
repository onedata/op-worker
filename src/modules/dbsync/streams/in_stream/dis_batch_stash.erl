%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules handles stashing of changes batches of single distributor. It also decides whether
%%% some batches should be requested to fill hole in stash to allow application of stashed batches.
%%% As each dbsync_in_stream_worker can process batches sent by multiple distributors,
%%% this module should be used only via dis_batch_stash_registry module.
%%% @end
%%%-------------------------------------------------------------------
-module(dis_batch_stash). % dis = dbsync_in_stream
-author("Michal Wrzeszcz").

-include("proto/oneprovider/dbsync_messages2.hrl").

-export([
    init/1,
    handle_incoming_batch/3,
    poll_next_batch/1,
    set_expected_batch_since/2,
    get_expected_batch_since/1
]).

-record(stash, {
    expected_batch_since = ?DEFAULT_SEQ :: seq(), % beginning of next expected batch to appear
                                                  % (only batch that starts from this sequence can be applied)
    table :: dis_batch_stash_table:table()
}).

-type stash() :: #stash{}.
-type batch() :: dbsync_worker:internal_changes_batch().
-type seq() :: couchbase_changes:seq().
-type incoming_batch_handling_result() :: ?BATCH_READY(batch()) | ?CHANGES_STASHED | ?CHANGES_IGNORED.
-type handling_mode() :: ?CONSIDER_BATCH | ?FORCE_STASH_BATCH.

-export_type([stash/0, incoming_batch_handling_result/0, handling_mode/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(oneprovider:id()) -> stash().
init(ProviderId) ->
    #stash{table = dis_batch_stash_table:init(ProviderId)}.

%%--------------------------------------------------------------------
%% @doc
%% Analyses stash and incoming batch to decide if batch should be applied, stashed or ignored.
%% If batch is to be applied, extends it if possible (see dis_batch_stash_table:take_and_merge_if_contiguous/2)
%% @end
%%--------------------------------------------------------------------
-spec handle_incoming_batch(stash(), batch(), handling_mode()) -> incoming_batch_handling_result().
handle_incoming_batch(Stash, Batch = #internal_changes_batch{since = Since}, ?CONSIDER_BATCH) ->
    Table = Stash#stash.table,
    CurrentSeq = Stash#stash.expected_batch_since,
    case Since of
        CurrentSeq ->
            PreprocessedBatch = dis_batch_stash_table:take_and_merge_if_contiguous(Table, Batch),
            ?BATCH_READY(PreprocessedBatch);
        Higher when Higher > CurrentSeq ->
            dis_batch_stash_table:stash(Table, CurrentSeq, Batch),
            ?CHANGES_STASHED;
        _ ->
            % TODO VFS-7323 - check until - if it is greater than current sequence number, trim batch and apply
            ?CHANGES_IGNORED
    end;
handle_incoming_batch(Stash, Batch, ?FORCE_STASH_BATCH) ->
    dis_batch_stash_table:stash(Stash#stash.table, Stash#stash.expected_batch_since, Batch),
    ?CHANGES_STASHED.

%%--------------------------------------------------------------------
%% @doc
%% Tries to take changes to be applied and extends it if possible
%% (see dis_batch_stash_table:take_and_merge_if_contiguous/2). Returns changes request
%% range if some changes have to be requested before application of stashed changes is possible.
%% @end
%%--------------------------------------------------------------------
-spec poll_next_batch(stash()) -> batch() | ?EMPTY_STASH | ?MISSING_CHANGES(seq(), seq()).
poll_next_batch(Stash) ->
    Table = Stash#stash.table,
    CurrentSeq = Stash#stash.expected_batch_since,

    case dis_batch_stash_table:take_and_prune_older(Table, CurrentSeq) of
        ?EMPTY_STASH ->
            ?EMPTY_STASH;
        ?MISSING_CHANGES(MissingUpTo) ->
            ?MISSING_CHANGES(CurrentSeq, MissingUpTo);
        #internal_changes_batch{} = Batch ->
            % TODO VFS-7323 - If hole in table is found by take_and_merge_if_contiguous,
            % return information to schedule changes request immediately
            dis_batch_stash_table:take_and_merge_if_contiguous(Table, Batch)
    end.

-spec set_expected_batch_since(stash(), seq()) -> stash().
set_expected_batch_since(Stash, CurrentSeq) ->
    Stash#stash{expected_batch_since = CurrentSeq}.

-spec get_expected_batch_since(stash()) -> seq().
get_expected_batch_since(Stash) ->
    Stash#stash.expected_batch_since.