%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules handles caching of changes and mutations gathered by
%%% dbsync_out_stream (dbsync_out_stream processes changes and mutations
%%% in batches).
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_out_stream_batch_cache).
-author("Michal Wrzeszcz").

-include("modules/dbsync/dbsync.hrl").
-include("modules/datastore/datastore_models.hrl").

%% Cache record API
-export([empty/0, cache_change/2, get_changes/1, get_changes_num/1,
    cache_remote_mutations/2, get_remote_mutations/1]).
%% #provider_mutations{} helper record API
-export([get_sequences/1, get_last_local_seq/1]).

% Separate record is stored to describe each provider's mutations.
% The record holds mutations connected with single dbsync_out_stream batch
% (cache is cleared during batch flush).
-record(provider_mutations, {
    remote_sequences = [] :: [dbsync_out_stream:remote_sequence_info()],
    last_local_seq :: datastore_doc:seq() | undefined % Local sequence of last document that appeared
                                                      % in dbsync_out_stream and had remote mutation
                                                      % done by particular provider ;
                                                      % undefined when remote_sequences is empty list
}).

-record(cache, {
    changes = [] :: [datastore:doc()], % Warning: list of changes is reversed
    remote_mutations = #{} :: mutations()
}).

-type provider_mutations() :: #provider_mutations{}.
-type mutations() :: #{oneprovider:id() => provider_mutations()}.
-type cache() :: #cache{}.

-export_type([provider_mutations/0, mutations/0, cache/0]).

%%%===================================================================
%%% Cache record API
%%%===================================================================

-spec empty() -> cache().
empty() ->
    #cache{}.

-spec cache_change(datastore:doc(), cache()) -> cache().
cache_change(Doc, Cache = #cache{changes = Docs}) ->
    Cache#cache{changes = [Doc | Docs]}.

-spec get_changes(cache()) -> [datastore:doc()].
get_changes(#cache{changes = Docs}) ->
    lists:reverse(Docs).

-spec get_changes_num(cache()) -> non_neg_integer().
get_changes_num(#cache{changes = Docs}) ->
    length(Docs).

-spec cache_remote_mutations(datastore:doc(), cache()) -> cache().
cache_remote_mutations(#document{key = Key, value = Value, seq = Seq, remote_sequences = RemoteMutations},
    #cache{remote_mutations = Map} = Cache) ->
    % TODO VFS-7036 - update only sequences greater that smallest pending link
    UpdatedMap = maps:fold(fun(RemoteMutator, RemoteSeq, Acc) ->
        #provider_mutations{remote_sequences = RemoteSeqs} =
            maps:get(RemoteMutator, Acc, #provider_mutations{}),
        RemoteSequenceInfo = #remote_sequence_info{seq = RemoteSeq, key = Key, model = datastore_doc:model(Value)},
        Acc#{RemoteMutator => #provider_mutations{
            remote_sequences = [RemoteSequenceInfo | RemoteSeqs], last_local_seq = Seq}}
    end, Map, RemoteMutations),
    Cache#cache{remote_mutations = UpdatedMap}.

-spec get_remote_mutations(cache()) -> mutations().
get_remote_mutations(#cache{remote_mutations = RemoteMutations}) ->
    RemoteMutations.

%%%===================================================================
%%% #provider_mutations{} helper record API
%%%===================================================================

-spec get_sequences(provider_mutations()) -> [dbsync_out_stream:remote_sequence_info()].
get_sequences(#provider_mutations{remote_sequences = RemoteSequences}) ->
    RemoteSequences.

-spec get_last_local_seq(provider_mutations()) -> datastore_doc:seq() | undefined.
get_last_local_seq(#provider_mutations{last_local_seq = LastLocalSeq}) ->
    LastLocalSeq.