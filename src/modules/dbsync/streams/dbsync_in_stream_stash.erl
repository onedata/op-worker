%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules handles caching of changes batches by dbsync_in_stream_worker.
%%% Separate structure is created for each dbsync_in_stream_worker.
%%% As it is possible to cache batches from different senders (stream
%%% send requests to different providers when producer of changes is dead),
%%% batches sent by different providers are stored in separate ets tables
%%% (one ets per provider).
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_in_stream_stash).
-author("Michal Wrzeszcz").

-include("modules/dbsync/dbsync.hrl").
-include("modules/datastore/datastore_models.hrl").

-export([init/0, stash_changes_batch/7, extend_batch_with_stashed_changes/5,
    get_changes_to_apply/3, get_request_upper_range/3]).

-type stash() :: #{oneprovider:id() => ets:tid()}.
-type batch() :: dbsync_worker:batch_docs().
-type extended_batch_description() :: {batch(), couchbase_changes:until(), dbsync_changes:timestamp()}.
-type stash_state() :: empty_stash | missing_changes.

-export_type([stash/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec init() -> stash().
init() ->
    #{}.

%%--------------------------------------------------------------------
%% @doc
%% Saves changes batch in a cache. If save operation would cause cache size
%% limit overflow, changes are dropped.
%% @end
%%--------------------------------------------------------------------
-spec stash_changes_batch(stash(), oneprovider:id(), couchbase_changes:seq(), couchbase_changes:since(),
    couchbase_changes:until(), dbsync_changes:timestamp(), batch()) -> stash().
stash_changes_batch(Stash, ProviderId, CurrentSeq, Since, Until, Timestamp, Docs) ->
    {Ets, UpdatedStash} = get_ets(Stash, ProviderId),
    stash_changes_batch(Ets, CurrentSeq, Since, Until, Timestamp, Docs),
    UpdatedStash.

-spec extend_batch_with_stashed_changes(stash(), oneprovider:id(), batch(), dbsync_changes:timestamp(),
    couchbase_changes:until()) -> {{extended_batch_description(), stash_state() | max_batch_size_reached}, stash()}.
extend_batch_with_stashed_changes(Stash, ProviderId, Docs, Timestamp, Until) ->
    {Ets, UpdatedStash} = get_ets(Stash, ProviderId),
    {extend_batch_with_stashed_changes(Ets, Docs, Timestamp, Until), UpdatedStash}.

-spec get_changes_to_apply(stash(), oneprovider:id(), couchbase_changes:seq()) ->
    {extended_batch_description() | stash_state(), stash()}.
get_changes_to_apply(Stash, ProviderId, Seq) ->
    {Ets, UpdatedStash} = get_ets(Stash, ProviderId),
    {get_changes_to_apply(Ets, Seq), UpdatedStash}.

-spec get_request_upper_range(stash(), oneprovider:id(), couchbase_changes:seq()) ->
    {couchbase_changes:seq() | empty_stash, stash()}.
get_request_upper_range(Stash, ProviderId, Seq) ->
    {Ets, UpdatedStash} = get_ets(Stash, ProviderId),
    {get_request_upper_range(Ets, Seq), UpdatedStash}.

%%%===================================================================
%%% Internal functions operating on single ets
%%%===================================================================

-spec get_ets(stash(), oneprovider:id()) -> {ets:tid(), stash()}.
get_ets(Stash, ProviderId) ->
    case maps:get(ProviderId, Stash, undefined) of
        undefined ->
            Name = binary_to_atom(<<"changes_stash_", ProviderId/binary>>, utf8),
            NewEts = ets:new(Name, [ordered_set, private]),
            {NewEts, Stash#{ProviderId => NewEts}};
        Ets ->
            {Ets, Stash}
    end.

-spec stash_changes_batch(ets:tid(), couchbase_changes:seq(), couchbase_changes:since(),
    couchbase_changes:until(), dbsync_changes:timestamp(), batch()) -> ok.
stash_changes_batch(Ets, CurrentSeq, Since, Until, Timestamp, Docs) ->
    Max = op_worker:get_env(dbsync_changes_stash_max_size, 100000),
    case Until > CurrentSeq + Max of
        true ->
            case ets:first(Ets) of
                '$end_of_table' ->
                    % Init ets after failure to have range for changes request
                    ets:insert(Ets, {{Since, Until}, {Timestamp, Docs}}),
                    ok;
                _ ->
                    ok
            end;
        false ->
            ets:insert(Ets, {{Since, Until}, {Timestamp, Docs}}),
            ok
    end.

-spec extend_batch_with_stashed_changes(ets:tid(), batch(), dbsync_changes:timestamp(),
    couchbase_changes:until()) -> {extended_batch_description(), stash_state() | max_batch_size_reached}.
extend_batch_with_stashed_changes(Ets, Docs, Timestamp, Until) ->
    case ets:first(Ets) of
        '$end_of_table' ->
            {{Docs, Until, Timestamp}, empty_stash};
        {Until, NextUntil} = Key ->
            MaxSize = op_worker:get_env(dbsync_changes_apply_max_size, 500),
            case length(Docs) > MaxSize of
                true ->
                    {{Docs, Until, Timestamp}, max_batch_size_reached};
                _ ->
                    {NextTimestamp, NextDocs} = ets:lookup_element(Ets, Key, 2),
                    UsedTimestamp = case NextTimestamp of
                        undefined -> Timestamp;
                        _ -> NextTimestamp
                    end,
                    ets:delete(Ets, Key),
                    extend_batch_with_stashed_changes(Ets, Docs ++ NextDocs, UsedTimestamp, NextUntil)
            end;
        _ ->
            {{Docs, Until, Timestamp}, missing_changes}
    end.

-spec get_changes_to_apply(ets:tid(), couchbase_changes:seq()) -> extended_batch_description() | stash_state().
get_changes_to_apply(Ets, Seq) ->
    case ets:first(Ets) of
        '$end_of_table' ->
            empty_stash;
        {Seq, Until} ->
            Key = {Seq, Until},
            {Timestamp, Docs} = ets:lookup_element(Ets, Key, 2),
            ets:delete(Ets, Key),
            {Docs, Until, Timestamp};
        _ ->
            missing_changes
    end.

-spec get_request_upper_range(ets:tid(), couchbase_changes:seq()) -> couchbase_changes:seq() | empty_stash.
get_request_upper_range(Ets, Seq) ->
    case ets:first(Ets) of
        '$end_of_table' ->
            empty_stash;
        {Since, _} when Since == Seq ->
            Seq;
        {Since, _} = Key when Since < Seq ->
            ets:delete(Ets, Key),
            get_request_upper_range(Ets, Seq);
        {Until, _} ->
            MaxSize = op_worker:get_env(dbsync_changes_max_request_size, 1000000),
            min(Until, Seq + MaxSize)
    end.