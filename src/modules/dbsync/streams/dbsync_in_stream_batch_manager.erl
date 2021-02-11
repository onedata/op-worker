%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules handles stashing and requesting of changes batches by dbsync_in_stream_worker.
%%% Separate structure is created for each dbsync_in_stream_worker.
%%% As it is possible to stash batches from different distributors (stream
%%% send requests to different providers when producer of changes is dead),
%%% batches sent by different providers are stored in separate stashes implemented as
%%% ets tables (one ets per provider). Record #internal_changes_batch{} is stored in ets
%%% and its field since is used as a key.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_in_stream_batch_manager).
-author("Michal Wrzeszcz").

-include("modules/dbsync/dbsync.hrl").
-include("proto/oneprovider/dbsync_messages2.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

-export([
    init/2,
    handle_incoming_batch/4,
    take_batch_or_request_range/2,
    set_sequence/3,
    get_sequence/2
]).

-record(distributor_data, {
    current_seq = ?DEFAULT_SEQ :: seq(), % only batch that starts from this sequence can be applied
    stash :: stash()
}).

-type state() :: #{oneprovider:id() => distributor_data()}.
-type distributor_data() :: #distributor_data{}.
-type stash() :: ets:tid().
-type batch() :: dbsync_worker:internal_changes_batch().
-type seq() :: couchbase_changes:seq().
-type currently_processing_batch_until() :: seq() | undefined.
-type action_on_incoming_batch() :: ?APPLY | ?CHANGES_STASHED | ?CHANGES_IGNORED.

-export_type([state/0]).

-define(STASH_NAME_PREFIX, "changes_stash_").
% Stash size is used to determine range of sequences that can be stored in stash.
% Changes with sequences greater than stream's current sequence number + ?STASH_SUGGESTED_SPAN
% will not be stored (one exception to rule is possible - see stash_changes_batch function).
-define(STASH_SUGGESTED_SPAN, op_worker:get_env(dbsync_changes_stash_suggested_span, 100000)).
-define(BATCH_MAX_SIZE, op_worker:get_env(dbsync_changes_apply_max_size, 500)).
-define(REQUEST_MAX_SIZE, op_worker:get_env(dbsync_changes_max_request_size, 1000000)).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(oneprovider:id(), seq()) -> state().
init(ProviderId, CurrentSeq) ->
    {Data, State} = get_distributor_data(#{}, ProviderId),
    update_distributor_data(State, ProviderId, Data#distributor_data{current_seq = CurrentSeq}).

-spec handle_incoming_batch(state(), oneprovider:id(), batch(), currently_processing_batch_until()) ->
    {action_on_incoming_batch(), batch(), state()}.
handle_incoming_batch(State, ProviderId, Batch, CurrentlyProcessingBatchUntil) ->
    {Data, UpdatedState} = get_distributor_data(State, ProviderId),
    {Action, UpdatedBatch} = handle_incoming_batch(Data, Batch, CurrentlyProcessingBatchUntil),
    {Action, UpdatedBatch, UpdatedState}.

-spec take_batch_or_request_range(state(), od_provider:id()) ->
    {batch() | ?EMPTY_STASH | {?REQUEST_CHANGES, seq(), seq()}, state()}.
take_batch_or_request_range(State, ProviderId) ->
    {Data, UpdatedState} = get_distributor_data(State, ProviderId),
    {take_batch_or_request_range_internal(Data, ProviderId), UpdatedState}.

-spec set_sequence(state(), oneprovider:id(), seq()) -> {OldSequence :: seq(), state()}.
set_sequence(State, ProviderId, CurrentSeq) ->
    {Data, UpdatedState} = get_distributor_data(State, ProviderId),
    OldSequence = get_sequence(Data),
    UpdatedState2 = update_distributor_data(UpdatedState, ProviderId, set_sequence(Data, CurrentSeq)),
    {OldSequence, UpdatedState2}.

-spec get_sequence(state(), oneprovider:id()) -> seq().
get_sequence(State, ProviderId) ->
    get_sequence(get_distributor_data_if_exists(State, ProviderId)).

%%%===================================================================
%%% Internal functions operating on single distributor_data record
%%%===================================================================

-spec get_distributor_data(state(), oneprovider:id()) -> {distributor_data(), state()}.
get_distributor_data(State, ProviderId) ->
    case maps:get(ProviderId, State, undefined) of
        undefined ->
            Name = binary_to_atom(<<?STASH_NAME_PREFIX, ProviderId/binary>>, utf8),
            NewStash = ets:new(Name, [ordered_set, private, {keypos, #internal_changes_batch.since}]),
            Data = #distributor_data{stash = NewStash},
            {Data, State#{ProviderId => Data}};
        Data ->
            {Data, State}
    end.

-spec get_distributor_data_if_exists(state(), oneprovider:id()) -> distributor_data() | undefined.
get_distributor_data_if_exists(State, ProviderId) ->
    maps:get(ProviderId, State, undefined).


-spec update_distributor_data(state(), oneprovider:id(), distributor_data()) -> state().
update_distributor_data(State, ProviderId, Data) ->
    State#{ProviderId => Data}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Analysis stash and incoming batch to decide if batch should be applied, stashed or ignored.
%% If batch is to be applied, extends it if possible (see take_and_extend_if_applicable function)
%% @end
%%--------------------------------------------------------------------
-spec handle_incoming_batch(distributor_data(), batch(), currently_processing_batch_until()) ->
    {action_on_incoming_batch(), batch()}.
handle_incoming_batch(Data, Batch = #internal_changes_batch{since = Since}, undefined) ->
    Stash = Data#distributor_data.stash,
    CurrentSeq = Data#distributor_data.current_seq,
    case Since of
        CurrentSeq ->
            BatchToApply = take_and_extend_if_applicable(Stash, Batch),
            {?APPLY, BatchToApply};
        Higher when Higher > CurrentSeq ->
            stash_changes_batch(Stash, CurrentSeq, Batch),
            {?CHANGES_STASHED, Batch};
        _ ->
            % TODO VFS-7323 - check until - if it is greater than current sequence number, trim batch and apply
            {?CHANGES_IGNORED, Batch}
    end;
handle_incoming_batch(Data, Batch, _) ->
    % TODO VFS-7323 - maybe compare batch since and until with current_seq and currently_processing_batch_until
    % and ignore batch it they are too small
    stash_changes_batch(Data#distributor_data.stash, Data#distributor_data.current_seq, Batch),
    {?CHANGES_STASHED, Batch}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to take changes to be applied. Returns changes request range if some changes
%% have to be requested before application of stashed changes is possible.
%% @end
%%--------------------------------------------------------------------
-spec take_batch_or_request_range_internal(distributor_data(), od_provider:id()) ->
    batch() | ?EMPTY_STASH | {?REQUEST_CHANGES, seq(), seq()}.
take_batch_or_request_range_internal(Data, ProviderId) ->
    Stash = Data#distributor_data.stash,
    CurrentSeq = Data#distributor_data.current_seq,
    case ets:first(Stash) of
        '$end_of_table' ->
            ?EMPTY_STASH;
        CurrentSeq ->
            [Batch] = ets:lookup(Stash, CurrentSeq),
            ets:delete(Stash, CurrentSeq),
            % TODO VFS-7323 - If hole in stash is found here, return information
            % to schedule changes request immediatelly
            take_and_extend_if_applicable(Stash, Batch);
        Since when Since < CurrentSeq ->
            ets:delete(Stash, Since),
            take_batch_or_request_range_internal(Data, ProviderId);
        Since ->
            Until = min(Since, CurrentSeq + ?REQUEST_MAX_SIZE),
            {?REQUEST_CHANGES, CurrentSeq, Until}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets sequence number that determines which batches can or cannot be applied
%% (only batch that starts from this sequence can be applied).
%% @end
%%--------------------------------------------------------------------
-spec set_sequence(distributor_data(), seq()) -> distributor_data().
set_sequence(Data, CurrentSeq) ->
    Data#distributor_data{current_seq = CurrentSeq}.

-spec get_sequence(distributor_data() | undefined) -> seq().
get_sequence(undefined) ->
    ?DEFAULT_SEQ;
get_sequence(Data) ->
    Data#distributor_data.current_seq.

%%%===================================================================
%%% Internal functions operating on stash
%%%===================================================================

-spec is_empty(stash()) -> boolean().
is_empty(Stash) ->
    ets:first(Stash) =:= '$end_of_table'.

-spec insert_new_or_larger(stash(), batch()) -> ok.
insert_new_or_larger(Stash, #internal_changes_batch{since = Since, until = Until} = Batch) ->
    case ets:insert_new(Stash, Batch) of
        true ->
            ok;
        false ->
            [#internal_changes_batch{until = ExistingUntil}] = ets:lookup(Stash, Since),
            case ExistingUntil < Until of
                true -> ets:insert(Stash, Batch);
                false -> ok
            end,
            ok
    end.

-spec stash_changes_batch(stash(), seq(), batch()) -> ok.
stash_changes_batch(Stash, CurrentSeq, Batch = #internal_changes_batch{until = Until}) ->
    case Until > CurrentSeq + ?STASH_SUGGESTED_SPAN of
        true ->
            case is_empty(Stash) of
                true ->
                    % If stash is empty save batch even if it exceeds max size
                    % (stashed batch range will be used to determine range of changes requests)
                    ets:insert(Stash, Batch),
                    ok;
                false ->
                    ok
            end;
        false ->
            insert_new_or_larger(Stash, Batch)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Takes batch from stash and merges it with batch provided in argument if there is no
%% missing changes between both batches. The function repeats this procedure until stash is empty,
%% batches cannot be merged or maximal batch size is reached.
%% @end
%%--------------------------------------------------------------------
-spec take_and_extend_if_applicable(stash(), batch()) -> batch().
take_and_extend_if_applicable(Stash, Batch = #internal_changes_batch{
    until = Until, 
    timestamp = Timestamp, 
    docs = Docs
}) ->
    case ets:first(Stash) of
        '$end_of_table' ->
            Batch;
        Until ->
            case length(Docs) > ?BATCH_MAX_SIZE of
                true ->
                    Batch;
                _ ->
                    [NextBatch = #internal_changes_batch{
                        timestamp = NextTimestamp,
                        docs = NextDocs
                    }] = ets:lookup(Stash, Until),
                    % TODO VFS-7206 use always timestamp from next batch when undefined/null/0 is removed from protocol
                    UpdatedTimestamp = case NextTimestamp of
                        undefined -> Timestamp;
                        _ -> NextTimestamp
                    end,
                    UpdatedBatch = NextBatch#internal_changes_batch{
                        docs = Docs ++ NextDocs, 
                        timestamp = UpdatedTimestamp
                    },
                    ets:delete(Stash, Until),
                    take_and_extend_if_applicable(Stash, UpdatedBatch)
            end;
        _ ->
            Batch
    end.