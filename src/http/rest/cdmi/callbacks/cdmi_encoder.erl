%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Encoding and decoding functions.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_encoder).
-author("Tomasz Lichon").

-include("http/rest/cdmi/cdmi_errors.hrl").

%% API
-export([encode/2, decode/2, decode/3, base64_encoded_data_size/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Encodes data according to given ecoding
%%--------------------------------------------------------------------
-spec encode(Data :: binary(), Encoding :: binary()) -> binary().
encode(Data, Encoding) when Encoding =:= <<"base64">> ->
    base64:encode(Data);
encode(Data, _) ->
    Data.

%%--------------------------------------------------------------------
%% @doc
%% Decodes data according to given ecoding and range. If range is
%% defined, encoding should be always base64.
%% @end
%%--------------------------------------------------------------------
-spec decode(Data :: binary(), Encoding :: binary(), Range :: undefined | any()) -> binary().
decode(Data, Encoding, undefined) ->
    decode(Data, Encoding);
decode(Data, _Encoding, _Range) ->
    decode(Data, <<"base64">>).

%%--------------------------------------------------------------------
%% @doc Decodes data according to given ecoding
%%--------------------------------------------------------------------
-spec decode(Data :: binary(), Encoding :: binary()) -> binary().
decode(undefined, _Encoding) ->
    <<>>;
decode(Data, Encoding) when Encoding =:= <<"base64">> ->
    try base64:decode(Data)
    catch _:_ -> throw(?ERROR_INVALID_BASE64)
    end;
decode(Data, _) ->
    Data.

%%--------------------------------------------------------------------
%% @doc Retrns size of base64 encoded data.
%%--------------------------------------------------------------------
-spec base64_encoded_data_size(DataSize :: non_neg_integer()) -> non_neg_integer().
base64_encoded_data_size(DataSize) ->
    trunc(4 * utils:ceil(DataSize / 3.0)).

%%%===================================================================
%%% Internal functions
%%%===================================================================