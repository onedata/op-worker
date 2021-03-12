%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides registry of dis_batch_stashes. Registry is necessary because
%%% it is possible to stash batches from different distributors (stream sends requests to
%%% different providers when producer of changes is dead). Batches sent by different providers
%%% are managed separately by module dis_batch_stash. This module provides API equivalent to
%%% dis_batch_stash creating instances as needed and then reusing them.
%%% Separate registry is created for each dbsync_in_stream_worker.
%%% @end
%%%-------------------------------------------------------------------
-module(dis_batch_stash_registry). % dis = dbsync_in_stream
-author("Michal Wrzeszcz").

-include("modules/dbsync/dbsync.hrl").

-export([
    init/2,
    handle_incoming_batch/4,
    poll_next_batch/2,
    set_expected_batch_since/3,
    get_expected_batch_since/2
]).

-type registry() :: #{oneprovider:id() => dis_batch_stash:stash()}.
-type batch() :: dbsync_worker:internal_changes_batch().
-type seq() :: couchbase_changes:seq().

-export_type([registry/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(oneprovider:id(), seq()) -> registry().
init(ProviderId, CurrentSeq) ->
    {Stash, Registry} = acquire_stash(#{}, ProviderId),
    update_stash(Registry, ProviderId, dis_batch_stash:set_expected_batch_since(Stash, CurrentSeq)).

%%--------------------------------------------------------------------
%% @doc
%% @equiv dis_batch_stash:handle_incoming_batch/3 but additionally handles finding stash in registry
%% @end
%%--------------------------------------------------------------------
-spec handle_incoming_batch(registry(), oneprovider:id(), batch(), dis_batch_stash:handling_mode()) ->
    {dis_batch_stash:incoming_batch_handling_result(), registry()}.
handle_incoming_batch(Registry, ProviderId, Batch, HandlingMode) ->
    {Stash, UpdatedRegistry} = acquire_stash(Registry, ProviderId),
    Result = dis_batch_stash:handle_incoming_batch(Stash, Batch, HandlingMode),
    {Result, UpdatedRegistry}.

%%--------------------------------------------------------------------
%% @doc
%% @equiv dis_batch_stash:poll_next_batch/1 but additionally handles finding stash in registry
%% @end
%%--------------------------------------------------------------------
-spec poll_next_batch(registry(), od_provider:id()) ->
    {batch() | ?EMPTY_STASH | ?MISSING_CHANGES_RANGE(seq(), seq()), registry()}.
poll_next_batch(Registry, ProviderId) ->
    {Stash, UpdatedRegistry} = acquire_stash(Registry, ProviderId),
    {dis_batch_stash:poll_next_batch(Stash), UpdatedRegistry}.

%%--------------------------------------------------------------------
%% @doc
%% Sets new value of expected_batch_since field (for chosen stash) and returns old one.
%% If the value is set for the the first time, default sequence number is returned.
%% @end
%%--------------------------------------------------------------------
-spec set_expected_batch_since(registry(), oneprovider:id(), seq()) -> {OldSequence :: seq(), registry()}.
set_expected_batch_since(Registry, ProviderId, CurrentSeq) ->
    {Stash, UpdatedRegistry} = acquire_stash(Registry, ProviderId),
    OldSequence = dis_batch_stash:get_expected_batch_since(Stash),
    UpdatedRegistry2 = update_stash(UpdatedRegistry, ProviderId,
        dis_batch_stash:set_expected_batch_since(Stash, CurrentSeq)),
    {OldSequence, UpdatedRegistry2}.

-spec get_expected_batch_since(registry(), oneprovider:id()) -> seq().
get_expected_batch_since(Registry, ProviderId) ->
    case maps:find(ProviderId, Registry) of
        error -> ?DEFAULT_SEQ;
        {ok, Stash} -> dis_batch_stash:get_expected_batch_since(Stash)
    end.

%%%===================================================================
%%% Internal functions operating on registry map.
%%%===================================================================

-spec acquire_stash(registry(), oneprovider:id()) -> {dis_batch_stash:stash(), registry()}.
acquire_stash(Registry, ProviderId) ->
    case maps:find(ProviderId, Registry) of
        error ->
            Stash = dis_batch_stash:init(ProviderId),
            {Stash, Registry#{ProviderId => Stash}};
        {ok, Stash} ->
            {Stash, Registry}
    end.


-spec update_stash(registry(), oneprovider:id(), dis_batch_stash:stash()) -> registry().
update_stash(Registry, ProviderId, Stash) ->
    Registry#{ProviderId => Stash}.