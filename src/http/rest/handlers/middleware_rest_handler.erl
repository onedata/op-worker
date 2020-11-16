%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% The module handling the common RESTful logic delegating specifics to
%%% middleware.
%%% @end
%%%-------------------------------------------------------------------
-module(middleware_rest_handler).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([handle_request/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec handle_request(middleware:req(), cowboy_req:req()) -> cowboy_req:req().
handle_request(OpReq, Req) ->
    Result = middleware:handle(OpReq),
    RestResponse = rest_translator:response(OpReq, Result),
    http_req:send_response(RestResponse, Req).
