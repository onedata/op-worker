%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Encoder and decoder functions for index structure from indexes model
%%% @end
%%%--------------------------------------------------------------------
-module(index_encoder).
-author("Tomasz Lichon").

%% API
-export([encode_value/2, decode_value/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Encode index to json string.
%% @end
%%--------------------------------------------------------------------
-spec encode_value(indexes:index(), index) -> binary().
encode_value(Value, index) ->
    json_utils:encode(Value).

%%--------------------------------------------------------------------
%% @doc
%% Decode index from json string
%% @end
%%--------------------------------------------------------------------
-spec decode_value(binary(), index) -> indexes:index().
decode_value(Value, index) ->
    Map = json_utils:decode(Value),
    maps:fold(fun
        (K, V, AccMap) when is_binary(K) ->
            AtomKey = binary_to_atom(K, utf8),
            maps:put(AtomKey, V, AccMap);
        (K, V, AccMap) ->
            maps:put(K, V, AccMap)
    end,#{}, Map).

%%%===================================================================
%%% Internal functions
%%%===================================================================
