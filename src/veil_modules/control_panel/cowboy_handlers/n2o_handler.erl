%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This is a cowboy handler module for handling HTTP request with n2o engine.
%% It's basically copied from n2o, with several changes.
%% @end
%% ===================================================================

-module(n2o_handler).
-behaviour(cowboy_http_handler).

-include_lib("veil_modules/control_panel/common.hrl").

% Cowboy API
-export([init/3, handle/2, terminate/3]).

% Bridge abstraction
-export([params/1, path/1, request_body/1, headers/1, header/3, response/2, reply/2]).
-export([cookies/1, cookie/2, cookie/3, cookie/5, delete_cookie/2, peer/1]).

% Handler state record
-record(state, {headers, body}).


%% ====================================================================
%% cowboy_http_handler API
%% ====================================================================

%% init/3
%% ====================================================================
%% @doc Cowboy handler callback, called to initialize request handling flow.
%% @end
-spec init(Type :: any(), Req :: req(), Opts :: [term()]) -> {ok, NewReq :: term(), State :: term()}.
%% ====================================================================
init(_Transport, Req, _Opts) ->
    {ok, Req, #state{}}.


%% handle/2
%% ====================================================================
%% @doc Cowboy handler callback, called to process a HTTP request.
%% @end
-spec handle(Req :: term(), State :: term()) -> {ok, NewReq :: term(), State :: term()}.
%% ====================================================================
handle(Req, State) ->
    {ok, NewReq} = wf_core:run(Req),
    {ok, NewReq, State}.


%% terminate/3
%% ====================================================================
%% @doc Cowboy handler callback, called after a request is processed.
%% @end
-spec terminate(Reason :: term(), Req :: term(), State :: term()) -> ok.
%% ====================================================================
terminate(_Reason, _Req, _State) ->
    ok.


%% ====================================================================
%% Cowboy Bridge Abstraction
%% ====================================================================

params(Req) -> {Params, _NewReq} = cowboy_req:qs_vals(Req), Params.
path(Req) -> {Path, _NewReq} = cowboy_req:path(Req), Path.
request_body(Req) -> veil_cowboy_bridge:apply(cowboy_req, body, [Req]).
headers(Req) -> cowboy_req:headers(Req).
header(Name, Value, Req) -> cowboy_req:set_resp_header(Name, Value, Req).
response(Html, Req) -> cowboy_req:set_resp_body(Html, Req).
reply(StatusCode, Req) -> veil_cowboy_bridge:apply(cowboy_req, reply, [StatusCode, Req]).
cookies(Req) -> element(1, cowboy_req:cookies(Req)).
cookie(Cookie, Req) -> element(1, cowboy_req:cookie(wf:to_binary(Cookie), Req)).
cookie(Cookie, Value, Req) -> cookie(Cookie, Value, <<"/">>, 0, Req).
cookie(Name, Value, Path, TTL, Req) ->
    Options = [{path, Path}, {max_age, TTL}],
    cowboy_req:set_resp_cookie(Name, Value, Options, Req).
delete_cookie(Cookie, Req) -> cookie(Cookie, <<"">>, <<"/">>, 0, Req).
peer(Req) -> {{Ip, Port}, Req} = cowboy_req:peer(Req), {Ip, Port}.
