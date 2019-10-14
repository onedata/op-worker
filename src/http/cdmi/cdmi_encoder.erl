%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% CDMI encoding and decoding functions.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_encoder).
-author("Tomasz Lichon").

-include_lib("ctool/include/errors.hrl").

%% API
-export([encode/2, decode/2, decode/3]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Encodes data according to given encoding
%%--------------------------------------------------------------------
-spec encode(Data :: binary(), Encoding :: binary()) -> binary().
encode(Data, <<"base64">>) -> base64:encode(Data);
encode(Data, _) -> Data.


%%--------------------------------------------------------------------
%% @doc
%% Decodes data according to given encoding and range. If range is
%% defined, encoding should be always base64.
%% @end
%%--------------------------------------------------------------------
-spec decode(Data :: binary(), Encoding :: binary(), Range :: undefined | any()) ->
    binary().
decode(Data, Encoding, undefined) ->
    decode(Data, Encoding);
decode(Data, _Encoding, _Range) ->
    decode(Data, <<"base64">>).


%%--------------------------------------------------------------------
%% @doc Decodes data according to given encoding
%%--------------------------------------------------------------------
-spec decode(Data :: binary(), Encoding :: binary()) -> binary().
decode(undefined, _Encoding) ->
    <<>>;
decode(Data, <<"base64">>) ->
    try
        base64:decode(Data)
    catch
        _:_ -> throw(?ERROR_BAD_DATA(<<"base64">>))
    end;
decode(Data, _) ->
    Data.
