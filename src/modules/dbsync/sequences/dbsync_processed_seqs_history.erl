%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for storing (using links) mapping of local sequences
%%% to set of sequences of remote providers to be used in future to inform
%%% provider joining space about sequences from which it should start
%%% dbsync_in_streams after initial caching up of changes.
%%% TODO VFS-7036 - extend description when catching up protocol is integrated.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_processed_seqs_history).
-author("Michal Wrzeszcz").

-include("modules/dbsync/dbsync.hrl").

% API
-export([add/3, get/2, decode/1]).
% For tests
-export([provider_seq_to_binary/2]).

-type encoded_correlations() :: binary().
-export_type([encoded_correlations/0]).

% All links connected with single space are saved using following key
-define(KEY(SpaceId), <<SpaceId/binary, "###_processed_sequences">>).
% Links always are saved within tree - this module uses single tree for all links
-define(TREE_ID, <<"processed_sequences">>).
-define(CTX, dbsync_state:get_ctx()). % History is saved as part of dbsync_state model

% Macros used for encoding/decoding links` values
-define(PROVIDER_MARKER, "#provider#").
-define(SEQ_MARKER, "#seq#").

%%%===================================================================
%%% Add/get API
%%%===================================================================

-spec add(od_space:id(), dbsync_seqs_correlation:providers_correlations(), datastore_doc:seq()) -> ok.
add(SpaceId, ProvidersCorrelations, CurrentLocalSeq) ->
    dbsync_seqs_tree:overwrite(descending, ?CTX, ?KEY(SpaceId), ?TREE_ID,
        CurrentLocalSeq, encode(ProvidersCorrelations)).

-spec get(od_space:id(), datastore_doc:seq()) -> encoded_correlations().
get(SpaceId, SeqNum) ->
    dbsync_seqs_tree:get_next(descending, ?CTX, ?KEY(SpaceId), ?TREE_ID, SeqNum, <<>>).

%%%===================================================================
%%% Encoding/decoding link values
%%%===================================================================

-spec encode(dbsync_seqs_correlation:providers_correlations()) -> encoded_correlations().
encode(ProvidersCorrelations) ->
    maps:fold(fun
        (ProviderId, #sequences_correlation{remote_consecutively_processed_max = RemoteConsecutivelyProcessedMax}, Acc) ->
            <<Acc/binary, (provider_seq_to_binary(ProviderId, RemoteConsecutivelyProcessedMax))/binary>>
    end, <<>>, ProvidersCorrelations).

-spec decode(encoded_correlations()) -> dbsync_seqs_correlation:consecutively_processed_sequences().
decode(EncodedCorrelations) ->
    ProvidersSeqs = binary:split(EncodedCorrelations, <<?PROVIDER_MARKER>>, [global, trim_all]),
    maps:from_list(lists:map(fun(ProviderSeqBinary) ->
        [ProviderId, SeqBinary] = binary:split(ProviderSeqBinary, <<?SEQ_MARKER>>),
        {ProviderId, binary_to_integer(SeqBinary)}
    end, ProvidersSeqs)).

%%%===================================================================
%%% Helper functions
%%%===================================================================

-spec provider_seq_to_binary(oneprovider:id(), datastore_doc:seq()) -> binary().
provider_seq_to_binary(ProviderId, Seq) ->
    <<?PROVIDER_MARKER, ProviderId/binary, ?SEQ_MARKER, (integer_to_binary(Seq))/binary>>.