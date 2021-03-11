%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules handles stashing changes batches in ets table. Stashing means that changes batches
%%% that cannot be applied at the moment are stored in ets table to provide them later when it is
%%% possible to apply them. Record #internal_changes_batch{} is stored in ets and its field since
%%% is used as a key.
%%% @end
%%%-------------------------------------------------------------------
-module(dis_batch_stash_table). % dis = dbsync_in_stream
-author("Michal Wrzeszcz").

-include("proto/oneprovider/dbsync_messages2.hrl").

-export([
    init/1,
    stash/3,
    take_and_prune_older/2,
    take_and_merge_if_contiguous/2
]).

-type table() :: ets:tid().
-type batch() :: dbsync_worker:internal_changes_batch().
-type seq() :: couchbase_changes:seq().

-export_type([table/0]).

-define(TABLE_NAME_PREFIX, "changes_stash_").
-define(EMPTY_TABLE, '$end_of_table').
% Suggested span is used to determine range of sequences that can be stored in the table.
% Changes with sequences greater than stream's current sequence number + ?SUGGESTED_SPAN
% will not be stored (one exception to rule is possible - see stash function).
-define(SUGGESTED_SPAN, op_worker:get_env(dbsync_changes_stash_suggested_span, 100000)).
% Suggested batch size is used when taking and extending batch from the table.
% Extending of batch is stopped when ?SUGGESTED_BATCH_SIZE is exceeded
-define(SUGGESTED_BATCH_SIZE, op_worker:get_env(dbsync_changes_apply_max_size, 500)).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(oneprovider:id()) -> table().
init(ProviderId) ->
    Name = binary_to_atom(<<?TABLE_NAME_PREFIX, ProviderId/binary>>, utf8),
    ets:new(Name, [ordered_set, private, {keypos, #internal_changes_batch.since}]).

-spec stash(table(), seq(), batch()) -> ok.
stash(Table, CurrentSeq, Batch = #internal_changes_batch{until = Until}) ->
    case Until > CurrentSeq + ?SUGGESTED_SPAN of
        true ->
            case ets:first(Table) of
                ?EMPTY_TABLE ->
                    % If table is empty, save batch even if it exceeds max size
                    % (stashed batch range will be used to determine range of changes requests)
                    ets:insert(Table, Batch),
                    ok;
                _ ->
                    ok
            end;
        false ->
            insert_new_or_larger(Table, Batch)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Takes batch that begins from sequence provided in argument. Deletes all batches that contain 
%% older changes. If batch cannot be found, returns reason (empty stash or missing changes).
%% @end
%%--------------------------------------------------------------------
-spec take_and_prune_older(table(), seq()) -> batch() | ?EMPTY_STASH | ?MISSING_CHANGES(seq()).
take_and_prune_older(Table, Since) ->
    case ets:first(Table) of
        ?EMPTY_TABLE ->
            ?EMPTY_STASH;
        Since ->
            [Batch] = ets:lookup(Table, Since),
            ets:delete(Table, Since),
            Batch;
        BatchSince when BatchSince < Since ->
            ets:delete(Table, BatchSince),
            take_and_prune_older(Table, Since);
        BatchSince ->
            ?MISSING_CHANGES(BatchSince)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Takes batch from table and merges it with batch provided in argument if there is no
%% missing changes between both batches. The function repeats this procedure until table is empty,
%% batches cannot be merged or maximal batch size is reached.
%% @end
%%--------------------------------------------------------------------
-spec take_and_merge_if_contiguous(table(), batch()) -> batch().
take_and_merge_if_contiguous(Table, Batch = #internal_changes_batch{
    until = Until, 
    timestamp = Timestamp, 
    docs = Docs
}) ->
    case ets:first(Table) of
        ?EMPTY_TABLE ->
            Batch;
        Until ->
            case length(Docs) > ?SUGGESTED_BATCH_SIZE of
                true ->
                    Batch;
                _ ->
                    [NextBatch = #internal_changes_batch{
                        timestamp = NextTimestamp,
                        docs = NextDocs
                    }] = ets:lookup(Table, Until),
                    % TODO VFS-7206 use always timestamp from next batch when undefined/null/0 is removed from protocol
                    UpdatedTimestamp = utils:ensure_defined(NextTimestamp, Timestamp),
                    UpdatedBatch = NextBatch#internal_changes_batch{
                        docs = Docs ++ NextDocs, 
                        timestamp = UpdatedTimestamp
                    },
                    ets:delete(Table, Until),
                    take_and_merge_if_contiguous(Table, UpdatedBatch)
            end;
        Since when Since < Until ->
            ets:delete(Table, Since),
            take_and_merge_if_contiguous(Table, Batch);
        _ ->
            Batch
    end.

%%%===================================================================
%%% Helper functions
%%%===================================================================

-spec insert_new_or_larger(table(), batch()) -> ok.
insert_new_or_larger(Table, #internal_changes_batch{since = Since, until = Until} = Batch) ->
    case ets:insert_new(Table, Batch) of
        true ->
            ok;
        false ->
            [#internal_changes_batch{until = ExistingUntil}] = ets:lookup(Table, Since),
            case ExistingUntil < Until of
                true -> ets:insert(Table, Batch);
                false -> ok
            end,
            ok
    end.