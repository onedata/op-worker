%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules handles stashing of changes batches by dbsync_in_stream_worker.
%%% Separate structure is created for each dbsync_in_stream_worker.
%%% As it is possible to stash batches from different distributors (stream
%%% send requests to different providers when producer of changes is dead),
%%% batches sent by different providers are stored in separate ets tables
%%% (one ets per provider). Record #entry{} is stored in ets and its field
%%% since is used as a key.
%%%
%%% TODO 7248 - rename module when its full functionality is implemented
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_in_stream_stash).
-author("Michal Wrzeszcz").

-include("modules/dbsync/dbsync.hrl").
-include("modules/datastore/datastore_models.hrl").

-export([
    init/0,
    stash_changes_batch/7,
    take_and_extend_if_applicable/5,
    take_first_batch_if_applicable/3,
    get_first_matching_and_trim_stalled/3
]).

-type stash() :: #{oneprovider:id() => ets:tid()}.
-type batch_docs() :: dbsync_worker:batch_docs().
-type docs_range_with_timestamp() :: {batch_docs(), RangeEnd :: couchbase_changes:until(), dbsync_changes:timestamp()}.
-type stash_state() :: empty_stash | missing_changes.

-export_type([stash/0]).

-define(ETS_NAME_PREFIX, "changes_stash_").
% Stash size is used to determine range of sequences that can be stored in stash.
% Changes with sequences greater than stream's current sequence number + ?STASH_SUGGESTED_SPAN
% will not be stored (one exception to rule is possible - see stash_changes_batch function).
-define(STASH_SUGGESTED_SPAN, op_worker:get_env(dbsync_changes_stash_suggested_span, 100000)).
-define(BATCH_MAX_SIZE, op_worker:get_env(dbsync_changes_apply_max_size, 500)).
-define(REQUEST_MAX_SIZE, op_worker:get_env(dbsync_changes_max_request_size, 1000000)).

% Entry stored in ets describing batch's range (since, until), timestamp and docs
-record(entry, {
    since :: couchbase_changes:seq(),
    until :: couchbase_changes:seq(),
    timestamp :: dbsync_changes:timestamp(),
    docs :: batch_docs()
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> stash().
init() ->
    #{}.

%%--------------------------------------------------------------------
%% @doc
%% Saves changes batch in stash. If save operation would cause stash size
%% limit overflow, changes are dropped.
%% @end
%%--------------------------------------------------------------------
-spec stash_changes_batch(stash(), oneprovider:id(), couchbase_changes:seq(), couchbase_changes:since(),
    couchbase_changes:until(), dbsync_changes:timestamp(), batch_docs()) -> stash().
stash_changes_batch(Stash, ProviderId, CurrentSeq, Since, Until, Timestamp, Docs) ->
    {Ets, UpdatedStash} = get_ets(Stash, ProviderId),
    stash_changes_batch(Ets, CurrentSeq, Since, Until, Timestamp, Docs),
    UpdatedStash.

-spec take_and_extend_if_applicable(stash(), oneprovider:id(), batch_docs(), dbsync_changes:timestamp(),
    couchbase_changes:until()) -> {{docs_range_with_timestamp(), stash_state() | max_batch_size_reached}, stash()}.
take_and_extend_if_applicable(Stash, ProviderId, Docs, Timestamp, Until) ->
    {Ets, UpdatedStash} = get_ets(Stash, ProviderId),
    {take_and_extend_if_applicable(Ets, Docs, Timestamp, Until), UpdatedStash}.

-spec take_first_batch_if_applicable(stash(), oneprovider:id(), couchbase_changes:seq()) ->
    {docs_range_with_timestamp() | stash_state(), stash()}.
take_first_batch_if_applicable(Stash, ProviderId, Seq) ->
    {Ets, UpdatedStash} = get_ets(Stash, ProviderId),
    {take_first_batch_if_applicable(Ets, Seq), UpdatedStash}.

-spec get_first_matching_and_trim_stalled(stash(), oneprovider:id(), couchbase_changes:seq()) ->
    {couchbase_changes:seq() | empty_stash, stash()}.
get_first_matching_and_trim_stalled(Stash, ProviderId, Seq) ->
    {Ets, UpdatedStash} = get_ets(Stash, ProviderId),
    {get_first_matching_and_trim_stalled(Ets, Seq), UpdatedStash}.

%%%===================================================================
%%% Internal functions operating on single ets
%%%===================================================================

-spec get_ets(stash(), oneprovider:id()) -> {ets:tid(), stash()}.
get_ets(Stash, ProviderId) ->
    case maps:get(ProviderId, Stash, undefined) of
        undefined ->
            Name = binary_to_atom(<<?ETS_NAME_PREFIX, ProviderId/binary>>, utf8),
            NewEts = ets:new(Name, [ordered_set, private, {keypos, #entry.since}]),
            {NewEts, Stash#{ProviderId => NewEts}};
        Ets ->
            {Ets, Stash}
    end.

-spec stash_changes_batch(ets:tid(), couchbase_changes:seq(), couchbase_changes:since(),
    couchbase_changes:until(), dbsync_changes:timestamp(), batch_docs()) -> ok.
stash_changes_batch(Ets, CurrentSeq, Since, Until, Timestamp, Docs) ->
    Entry = #entry{
        since = Since,
        until = Until,
        timestamp = Timestamp,
        docs = Docs
    },
    case Until > CurrentSeq + ?STASH_SUGGESTED_SPAN of
        true ->
            case is_empty(Ets) of
                true ->
                    % If stash is empty save batch even if it exceeds max size
                    % (stashed batch range will be used to determine range of changes requests)
                    ets:insert(Ets, Entry),
                    ok;
                false ->
                    ok
            end;
        false ->
            insert_new_or_larger(Ets, Entry)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Takes batch from stash and merges it with batch provided in argument if there is no
%% missing changes between both batches. The function repeats this procedure until stash is empty,
%% batches cannot be merged or maximal batch size is reached.
%% @end
%%--------------------------------------------------------------------
-spec take_and_extend_if_applicable(ets:tid(), batch_docs(), dbsync_changes:timestamp(),
    couchbase_changes:until()) -> {docs_range_with_timestamp(), stash_state() | max_batch_size_reached}.
take_and_extend_if_applicable(Ets, Docs, Timestamp, Until) ->
    case ets:first(Ets) of
        '$end_of_table' ->
            {{Docs, Until, Timestamp}, empty_stash};
        Until ->
            case length(Docs) > ?BATCH_MAX_SIZE of
                true ->
                    {{Docs, Until, Timestamp}, max_batch_size_reached};
                _ ->
                    [#entry{
                        until = NextUntil,
                        timestamp = NextTimestamp,
                        docs = NextDocs
                    }] = ets:lookup(Ets, Until),
                    UsedTimestamp = case NextTimestamp of
                        undefined -> Timestamp;
                        _ -> NextTimestamp
                    end,
                    ets:delete(Ets, Until),
                    take_and_extend_if_applicable(Ets, Docs ++ NextDocs, UsedTimestamp, NextUntil)
            end;
        _ ->
            {{Docs, Until, Timestamp}, missing_changes}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Takes first batch from the stash if its beginning is equal to sequence provided in the argument.
%% @end
%%--------------------------------------------------------------------
-spec take_first_batch_if_applicable(ets:tid(), couchbase_changes:seq()) -> docs_range_with_timestamp() | stash_state().
take_first_batch_if_applicable(Ets, Seq) ->
    case ets:first(Ets) of
        '$end_of_table' ->
            empty_stash;
        Seq ->
            [#entry{
                until = Until,
                timestamp = Timestamp,
                docs = Docs
            }] = ets:lookup(Ets, Seq),
            ets:delete(Ets, Seq),
            {Docs, Until, Timestamp};
        _ ->
            missing_changes
    end.

-spec get_first_matching_and_trim_stalled(ets:tid(), couchbase_changes:seq()) -> couchbase_changes:seq() | empty_stash.
get_first_matching_and_trim_stalled(Ets, Seq) ->
    case ets:first(Ets) of
        '$end_of_table' ->
            empty_stash;
        Seq ->
            Seq;
        Since when Since < Seq ->
            ets:delete(Ets, Since),
            get_first_matching_and_trim_stalled(Ets, Seq);
        Since ->
            min(Since, Seq + ?REQUEST_MAX_SIZE)
    end.

%%%===================================================================
%%% Helper functions
%%%===================================================================

-spec is_empty(ets:tid()) -> boolean().
is_empty(Ets) ->
    ets:first(Ets) =:= '$end_of_table'.

-spec insert_new_or_larger(ets:tid(), #entry{}) -> ok.
insert_new_or_larger(Ets, #entry{since = Since, until = Until} = Entry) ->
    case ets:insert_new(Ets, Entry) of
        true ->
            ok;
        false ->
            [#entry{until = ExistingUntil}] = ets:lookup(Ets, Since),
            case ExistingUntil < Until of
                true -> ets:insert(Ets, Entry);
                false -> ok
            end,
            ok
    end.