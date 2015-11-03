%% ===================================================================
%% @author Piotr Ociepka
%% @copyright (C): 2015 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module implements commonly used functions, i. e. formatting JSON responses.
%% @end
%% ===================================================================
-module(cdmi_utils).
-author("Piotr Ociepka").

%% API
-export([encode_to_json/1]).

%% ====================================================================
%% @doc Converts Erlang term to SJON binary
%% ====================================================================
-spec encode_to_json(term()) -> binary().
encode_to_json(Term) ->
  Encoder = mochijson2:encoder([{utf8, true}]),
  iolist_to_binary(Encoder(Term)).
