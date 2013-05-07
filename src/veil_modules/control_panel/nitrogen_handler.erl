%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This is the callback module for cowboy to handle requests
%% by passing them to the nitrogen engine
%% @end
%% ===================================================================

-module(nitrogen_handler).
-include_lib("nitrogen_core/include/wf.hrl").

-export([init/3, handle/2, terminate/3]).

-record(state, {headers, body}).

%% init/3
%% ====================================================================
%% @doc Initializes a request-response procedure
-spec init(Protocol, Request :: term(), Options :: term()) -> Result when
  Protocol :: {Transport :: term(), http},
  Result :: {ok, Request :: term(), #state{}}.
%% ====================================================================
init({Transport, http}, Req, Opts) ->
    Headers = proplists:get_value(headers, Opts, []),
    Body = proplists:get_value(body, Opts, "http_handler"),
    {ok, Req, #state{headers=Headers, body=Body}}.


%% handle/2
%% ====================================================================
%% @doc Handles a request producing a response with use of Nitrogen engine
-spec handle(Request, Options) -> Result when
  Result :: {ok, Response, Options},
  Request :: term(),
  Options :: term(),
  Response :: term().
%% ====================================================================
handle(Req, _Opts) ->
    DocRoot = "./site",
    RequestBridge = simple_bridge:make_request(cowboy_request_bridge,
                                               {Req, DocRoot}),

    ResponseBridge = simple_bridge:make_response(cowboy_response_bridge,
                                                 RequestBridge),

    %% Establishes the context with the Request and Response Bridges
    nitrogen:init_request(RequestBridge, ResponseBridge),
    
    {ok, NewReq} = nitrogen:run(),

    %% This will be returned back to cowboy
    {ok, NewReq, _Opts}.

%% terminate/3
%% ====================================================================
%% @doc Cowboy handler callback
-spec terminate(Reason, Request, State) -> Result when
  Result :: ok,
  Reason :: term(),
  Request :: term(),
  State :: term().
%% ====================================================================
terminate(_Reason, _Req, _State) ->
  ok.
