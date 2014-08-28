%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module BLABLABLA
%% @end
%% ===================================================================
-module(veil_cowboy_bridge).
-behaviour(cowboy_http_handler).
-behaviour(cowboy_websocket_handler).

-include("veil_modules/control_panel/common.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% Communication between socket process and handling process
-export([apply/3, request_processing_loop/0, set_socket_pid/1, get_socket_pid/0]).

%% Cowboy handler API
-export([init/3, handle/2, terminate/3]).

%% Cowboy websocket handler API
-export([websocket_init/3, websocket_handle/3, websocket_info/3, websocket_terminate/3]).

%% Cowboy REST handler API
-export([rest_init/2, resource_exists/2, allowed_methods/2, content_types_provided/2]).
-export([content_types_accepted/2, delete_resource/2]).
%% REST handler specific funs
-export([get_resource/2, handle_urlencoded_data/2, handle_json_data/2, handle_multipart_data/2]).
%% CDMI handler specific funs
-export([get_cdmi_container/2, put_cdmi_container/2]).

%% ====================================================================
%% API
%% ====================================================================

%% apply/3
%% ====================================================================
%% @doc Used to interact with the socket process - issues application of
%% some code on the socket process, which then sends back the result.
%% @end
-spec apply(Module :: atom(), Fun :: function(), Args :: [term()]) -> term().
%% ====================================================================
apply(Module, Fun, Args) ->
    io:format(user, "apply ~p:~p/~p~n", [Module, Fun, length(Args)]),
    get_socket_pid() ! {apply, Module, Fun, Args},
    receive
        {result, Result} ->
            Result
    end.


%% ====================================================================
%% HTTP callbacks
%% ====================================================================

%% init/3
%% ====================================================================
%% @doc Cowboy handler callback, called to initialize request handling flow.
%% @end
-spec init(Type :: any(), Req :: req(), Opts :: [term()]) -> {ok, NewReq :: term(), State :: term()}.
%% ====================================================================
init(Type, Req, Opts) ->
    io:format("Got request at ~p~n", [node()]),
    spawn_handling_process(),
    ?debug("veil_cowboy_bridge: ~p", [Opts]),
    HandlerModule = proplists:get_value(handler_module, Opts),
    HandlerOpts = proplists:get_value(handler_opts, Opts, []),
    set_handler_module(HandlerModule),
    delegate(init, [Type, Req, HandlerOpts]).


%% handle/2
%% ====================================================================
%% @doc Cowboy handler callback, called to process a HTTP request.
%% @end
-spec handle(Req :: term(), State :: term()) -> {ok, NewReq :: term(), State :: term()}.
%% ====================================================================
handle(Req, State) ->
    delegate(handle, [Req, State]).


%% terminate/3
%% ====================================================================
%% @doc Cowboy handler callback, called after a request is processed.
%% @end
-spec terminate(Reason :: term(), Req :: term(), State :: term()) -> ok.
%% ====================================================================
terminate(Reason, Req, State) ->
    delegate(terminate, [Reason, Req, State]),
    terminate_handling_process().


%% ====================================================================
%% Websocket callbacks
%% ====================================================================

%% websocket_init/3
%% ====================================================================
%% @doc Cowboy handler callback, called right after protocol upgrade to websocket.
%% @end
-spec websocket_init(Transport :: term(), Req :: term(), Opts :: term()) -> ok.
%% ====================================================================
websocket_init(Transport, Req, Opts) ->
    HandlerOpts = proplists:get_value(handler_opts, Opts, []),
    delegate(websocket_init, [Transport, Req, HandlerOpts]).


%% websocket_handle/3
%% ====================================================================
%% @doc Cowboy handler callback, called when websocket process receives
%% data packet (event) from websocket client.
%% @end
-spec websocket_handle(Data :: term(), Req :: term(), State :: term()) -> ok.
%% ====================================================================
websocket_handle(Data, Req, State) ->
    delegate(websocket_handle, [Data, Req, State]).


%% websocket_info/3
%% ====================================================================
%% @doc Cowboy handler callback, called when websocket process receives an erlang message.
%% @end
-spec websocket_info(Info :: term(), Req :: term(), State :: term()) -> ok.
%% ====================================================================
websocket_info(Info, Req, State) ->
    delegate(websocket_info, [Info, Req, State]).


%% websocket_terminate/3
%% ====================================================================
%% @doc Cowboy handler callback, called on a websocket connection finalization.
%% @end
-spec websocket_terminate(Reason :: term(), Req :: term(), State :: term()) -> ok.
%% ====================================================================
websocket_terminate(Reason, Req, State) ->
    delegate(websocket_terminate, [Reason, Req, State]),
    terminate_handling_process().


%% ====================================================================
%% REST callbacks
%% ====================================================================

%% rest_init/2
%% ====================================================================
%% @doc Cowboy handler callback, called right after protocol upgrade to init the request context.
%% @end
-spec rest_init(Req :: req(), Opts :: term()) -> {ok, NewReq :: req(), State :: term()} | {shutdown, NewReq :: req()}.
%% ====================================================================
rest_init(Req, Opts) ->
    HandlerOpts = proplists:get_value(handler_opts, Opts, []),
    delegate(rest_init, [Req, HandlerOpts]).


%% allowed_methods/2
%% ====================================================================
%% @doc Cowboy callback function.
%% Returns methods that are allowed, based on version specified in URI.
%% @end
-spec allowed_methods(Req :: req(), State :: term()) -> {Result :: [binary()], NewReq :: req(), NewState :: term()}.
%% ====================================================================
allowed_methods(Req, State) ->
    delegate(allowed_methods, [Req, State]).


%% content_types_provided/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content types that can be provided. "application/json" is default.
%% It can be changed later by gui_utils:cowboy_ensure_header/3.
%% @end
-spec content_types_provided(Req :: req(), State :: term()) -> {Result :: [binary()], NewReq :: req(), NewState :: term()}.
%% ====================================================================
content_types_provided(Req, State) ->
    delegate(content_types_provided, [Req, State]).


%% resource_exists/2
%% ====================================================================
%% @doc Cowboy callback function
%% Determines if resource identified by URI exists.
%% @end
-spec resource_exists(Req :: req(), State :: term()) -> {Result :: boolean(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
resource_exists(Req, State) ->
    delegate(resource_exists, [Req, State]).


%% get_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests.
%% @end
-spec get_resource(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
get_resource(Req, State) ->
    delegate(get_resource, [Req, State]).


%% content_types_accepted/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content-types that are accepted by REST handler and what
%% functions should be used to process the requests.
%% @end
-spec content_types_accepted(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
content_types_accepted(Req, State) ->
    delegate(content_types_accepted, [Req, State]).


%% handle_urlencoded_data/2
%% ====================================================================
%% @doc Function handling "application/x-www-form-urlencoded" requests.
%% @end
-spec handle_urlencoded_data(Req :: req(), State :: term()) -> {Result :: boolean(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
handle_urlencoded_data(Req, State) ->
    delegate(handle_urlencoded_data, [Req, State]).


%% handle_json_data/2
%% ====================================================================
%% @doc Function handling "application/json" requests.
%% @end
-spec handle_json_data(Req :: req(), State :: term()) -> {Result :: boolean(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
handle_json_data(Req, State) ->
    delegate(handle_json_data, [Req, State]).


%% handle_multipart_data/2
%% ====================================================================
%% @doc Function handling "multipart/form-data" requests.
%% @end
-spec handle_multipart_data(Req :: req(), State :: term()) -> {Result :: boolean(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
handle_multipart_data(Req, State) ->
    delegate(handle_multipart_data, [Req, State]).


%% delete_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles DELETE requests.
%% @end
-spec delete_resource(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
delete_resource(Req, State) ->
    delegate(delete_resource, [Req, State]).


%% ====================================================================
%% CDMI callbacks
%% ====================================================================

%% get_cdmi_container/2
%% ====================================================================
%% @doc Callback function for cdmi container GET operation (create dir).
%% @end
-spec get_cdmi_container(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), term()}.
%% ====================================================================
get_cdmi_container(Req, State) ->
    delegate(get_cdmi_container, [Req, State]).

%% put_cdmi_container/2
%% ====================================================================
%% @doc Callback function for cdmi container PUT operation (create dir).
%% @end
-spec put_cdmi_container(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), State :: term()}.
%% ====================================================================
put_cdmi_container(Req, State) ->
    delegate(put_cdmi_container, [Req, State]).


%% ====================================================================
%% Internal functions
%% ====================================================================

spawn_handling_process() ->
    SocketPid = self(),
    MsgID = 0, %% This can be 0 as one socket process sends only one request
    gen_server:call(?Dispatcher_Name, {node_chosen, {control_panel, 1, SocketPid, MsgID, {spawn_handler, SocketPid}}}),
    receive
        {worker_answer, MsgID, Resp} ->
            set_handler_pid(Resp)
    after 5000 ->
        ?error("Cannot spawn handling process, timeout"),
        {error, timeout}
    end.


terminate_handling_process() ->
    get_handler_pid() ! terminate.


delegate(Fun, Args) ->
    HandlerModule = get_handler_module(),
    io:format("~p:~p/~p~n", [HandlerModule, Fun, length(Args)]),
    HandlerPid = get_handler_pid(),
    HandlerPid ! {apply, HandlerModule, Fun, Args},
    delegation_loop(HandlerPid).


delegation_loop(HandlerPid) ->
    receive
        {result, Result} ->
            Result;
        {apply, Module, Fun, Args} ->
            HandlerPid ! {result, erlang:apply(Module, Fun, Args)},
            delegation_loop(HandlerPid)
    end.


request_processing_loop() ->
    SocketProcessPid = get_socket_pid(),
    receive
        {apply, Module, Fun, Args} ->
            try
                io:format("RPL: ~p:~p/~p~n", [Module, Fun, length(Args)]),
                SocketProcessPid ! {result, erlang:apply(Module, Fun, Args)}
            catch
                T:M ->
                    ?error_stacktrace("Error in RPL - ~p:~p", [T, M])
            end,
            request_processing_loop();
        {flush, Actions} ->
            SocketProcessPid ! {flush, Actions},
            request_processing_loop();
        terminate ->
            ok;
        Unknown ->
            ?warning("Unknown message in request processing loop: ~p", [Unknown])
    end.


set_handler_module(Module) ->
    erlang:put(handler_module, Module).


get_handler_module() ->
    erlang:get(handler_module).


set_handler_pid(Pid) ->
    erlang:put(handler_pid, Pid).


get_handler_pid() ->
    erlang:get(handler_pid).


set_socket_pid(Pid) ->
    erlang:put(socket_pid, Pid).


get_socket_pid() ->
    erlang:get(socket_pid).