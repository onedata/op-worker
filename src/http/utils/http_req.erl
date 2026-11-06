%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides various utilities for handling http requests.
%%% @end
%%%--------------------------------------------------------------------
-module(http_req).
-author("Bartosz Walkowicz").

-include("http/rest.hrl").

%% API
-export([
    send_response/2,
    send_error/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec send_response(#rest_resp{}, cowboy_req:req()) -> cowboy_req:req().
send_response(#rest_resp{code = Code, headers = Headers, body = Body}, Req) ->
    RespBody = case Body of
        {binary, Bin} -> Bin;
        Map -> json_utils:encode(Map)
    end,
    cowboy_req:reply(Code, Headers, RespBody, Req).


-spec send_error({error, term()}, cowboy_req:req()) ->
    cowboy_req:req().
send_error(Error, Req) ->
    send_response(rest_translator:error_response(Error), Req).
