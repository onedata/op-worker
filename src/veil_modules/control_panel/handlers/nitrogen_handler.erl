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
-include_lib("logging.hrl").

-export([init/3, handle/2, terminate/3]).

-record(state, {headers, body}).

%% ====================================================================
%% API functions
%% ====================================================================

%% init/3
%% ====================================================================
%% @doc Initializes a request-response procedure
-spec init(Protocol, Request :: term(), Options :: term()) -> Result when
    Protocol :: {Transport :: term(), http},
    Result :: {ok, Request :: term(), #state{}}.
%% ====================================================================
init({_Transport, http}, Req, Opts) ->
    Headers = proplists:get_value(headers, Opts, []),
    Body = proplists:get_value(body, Opts, "http_handler"),
    {ok, Req, #state{headers = Headers, body = Body}}.


%% handle/2
%% ====================================================================
%% @doc Handles a request producing a response with use of Nitrogen engine
%% or a file stream response
%% @end
-spec handle(Request, Options) -> Result when
    Result :: {ok, Response, Options},
    Request :: term(),
    Options :: term(),
    Response :: term().
%% ====================================================================
handle(InitialReq, _Opts) ->
    % Pass the request to file_transfer_handler to see if it should handle it
    try
        ProcessedReq = case file_transfer_handler:maybe_handle_request(InitialReq) of
        % Request handled
                           {true, Req} ->
                               Req;

        % Request not handled
                           {false, Req} ->
                               {ok, DocRoot} = application:get_env(veil_cluster_node, control_panel_static_files_root),
                               RequestData = cowboy_request_bridge:init({Req, DocRoot}),
                               ReqBridge = simple_bridge_request_wrapper:new(cowboy_request_bridge, RequestData, false, [], [], none),
                               % Pass the request to multipart_bridge to check if it's a file upload request
                               % and dispatch the file if needed
                               RequestBridge = case veil_multipart_bridge:parse(ReqBridge) of
                                                   {ok, Params, Files} ->
                                                       ReqBridge:set_multipart(Params, Files);
                                                   {ok, not_multipart} ->
                                                       ReqBridge
                                               end,

                               % Make response bridge
                               ResponseBridge = simple_bridge:make_response(cowboy_response_bridge, RequestBridge),

                               % Establish the context with the Request and Response Bridges
                               nitrogen:init_request(RequestBridge, ResponseBridge),

                               % Process the request in nitrogen engine
                               {ok, NewReq} = nitrogen:run(),

                               NewReq
                       end,
        % This will be returned back to cowboy
        {ok, ProcessedReq, _Opts}
    catch Type:Message ->
        ?error_stacktrace("Error in nitrogen_handler - ~p:~p", [Type, Message]),
        {ok, ErrorReq} = cowboy_req:reply(500, InitialReq),
        {ok, ErrorReq, _Opts}
    end.

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
