%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Copy of n2o_cowboy.erl from n2o.
%%% This is a cowboy handler module for handling HTTP request with n2o engine.
%%% Compared to original, this module has slight changes in the following functions:
%%% - request_body/1
%%% - reply/2
%%% @end
%%% @todo function headers
%%%--------------------------------------------------------------------
-module(n2o_handler).
-author("Lukasz Opiola").

-behaviour(cowboy_http_handler).

% cowboy_http_handler callbacks
-export([init/3, handle/2, terminate/3]).

% Bridge abstraction
-export([params/1, path/1, request_body/1, headers/1, header/3, response/2, reply/2]).
-export([cookies/1, cookie/2, cookie/3, cookie/5, delete_cookie/2, peer/1]).

% Handler state record
-record(state, {headers, body}).

%%%===================================================================
%%% cowboy_http_handler callbacks
%%%===================================================================
init(_Transport, Req, _Opts) ->
    {ok, Req, #state{}}.

handle(Req, State) ->
    {ok, NewReq} = wf_core:run(Req),
    {ok, NewReq, State}.

terminate(_Reason, _Req, _State) ->
    ok.

%%%===================================================================
%%% Cowboy Bridge Abstraction
%%%===================================================================

params(Req) -> {Params, _NewReq} = cowboy_req:qs_vals(Req), Params.
path(Req) -> {Path, _NewReq} = cowboy_req:path(Req), Path.
request_body(Req) -> opn_cowboy_bridge:apply(cowboy_req, body, [Req]).
headers(Req) -> cowboy_req:headers(Req).
header(Name, Value, Req) -> cowboy_req:set_resp_header(Name, Value, Req).
response(Html, Req) -> cowboy_req:set_resp_body(Html, Req).
reply(StatusCode, Req) -> opn_cowboy_bridge:apply(cowboy_req, reply, [StatusCode, Req]).
cookies(Req) -> element(1, cowboy_req:cookies(Req)).
cookie(Cookie, Req) -> gui_ctx:cookie(gui_str:to_binary(Cookie), Req).  % cowboy_req:cookie has a bug
cookie(Cookie, Value, Req) -> cookie(Cookie, Value, <<"/">>, 0, Req).
cookie(Name, Value, Path, TTL, Req) ->
    Options = [{path, Path}, {max_age, TTL}],
    cowboy_req:set_resp_cookie(Name, Value, Options, Req).
delete_cookie(Cookie, Req) -> cookie(Cookie, <<"">>, <<"/">>, 0, Req).
peer(Req) -> {{Ip, Port}, Req} = cowboy_req:peer(Req), {Ip, Port}.
