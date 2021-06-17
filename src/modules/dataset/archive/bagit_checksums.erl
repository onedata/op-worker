%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper for bagit module.
%%% It contains utility functions for calculating files' checksums
%%% when creating bagit archive.
%%% @end
%%%-------------------------------------------------------------------
-module(bagit_checksums).
-author("Jakub Kudzia").

-include("modules/dataset/bagit.hrl").

%% API
-export([init/1, update/2, finalize/1, get/2]).

-type algorithm() :: ?MD5 | ?SHA1 | ?SHA256 | ?SHA512.
-type algorithms() :: [algorithm()].
-type buffer() :: term(). % crypto:hash_state() is not an exported type
-type buffers() :: #{algorithm() => buffer()}.
-type checksum() :: binary().
-type checksums() :: #{algorithm() => checksum()}.

-export_type([algorithms/0, algorithm/0, checksums/0, buffers/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init(algorithms()) -> buffers().
init(Algorithms) ->
    lists:foldl(fun(Algorithm, Acc) ->
        Acc#{Algorithm => crypto:hash_init(ensure_compatible_algorithm_name(Algorithm))}
    end, #{}, Algorithms).


-spec update(buffers(), binary()) -> buffers().
update(Buffers, Data) ->
    maps:map(fun(_, HashState) ->
        crypto:hash_update(HashState, Data)
    end, Buffers).


-spec finalize(buffers()) -> checksums().
finalize(Buffers) ->
    maps:map(fun(_, HashState) ->
        hex_utils:hex(crypto:hash_final(HashState))
    end, Buffers).


-spec get(algorithm(), checksums()) -> checksum().
get(Algorithm, Checksums) ->
    maps:get(Algorithm, Checksums).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Maps checksum algorithm name to atom compatible with crypto module.
%% @end
%%--------------------------------------------------------------------
-spec ensure_compatible_algorithm_name(algorithm()) -> atom().
ensure_compatible_algorithm_name(?MD5) -> md5;
ensure_compatible_algorithm_name(?SHA1) -> sha;
ensure_compatible_algorithm_name(?SHA256) -> sha256;
ensure_compatible_algorithm_name(?SHA512) -> sha512.