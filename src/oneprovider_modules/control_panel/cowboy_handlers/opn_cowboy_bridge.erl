%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module introduces a new abstraction layer over standard cowboy
%% handlers. It allows delegating tasks to control_panel instances on other
%% nodes. Delegation can be switched on and off for different handlers.
%% Nomenclature:
%% socket process - process, that has been spawned by cowboy to handle a request
%% handling process - process that has been spawned by control_panel to process
%% the request and send the answer to socket process.
%% @end
%% ===================================================================
-module(opn_cowboy_bridge).
-behaviour(cowboy_http_handler).
-behaviour(cowboy_websocket_handler).

-include("oneprovider_modules/control_panel/common.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

% Max time (ms) to wait for worker_host to reply
-define(handling_process_spawn_timeout, 5000).

%% Interaction between socket process and handling process
-export([apply/3, request_processing_loop/0, set_socket_pid/1, get_socket_pid/0]).

%% Cowboy handler API
-export([init/3, handle/2, terminate/3]).

%% Cowboy websocket handler API
-export([websocket_init/3, websocket_handle/3, websocket_info/3, websocket_terminate/3]).

%% Cowboy REST handler API
%% This is not the full cowboy API. If needed, more functions can be added to this bridge.
-export([rest_init/2, malformed_request/2, known_methods/2, allowed_methods/2, is_authorized/2, options/2, resource_exists/2]).
-export([content_types_provided/2, languages_provided/2, charsets_provided/2]).
-export([moved_permanently/2, moved_temporarily/2, content_types_accepted/2, delete_resource/2]).
-export([generate_etag/2, last_modified/2, expires/2, forbidden/2]).
%% REST handler specific funs
-export([get_resource/2, handle_urlencoded_data/2, handle_json_data/2, handle_multipart_data/2]).
%% CDMI handler specific funs
-export([get_cdmi_container/2, get_cdmi_object/2, get_binary/2, get_cdmi_capability/2]).
-export([put_cdmi_container/2, put_cdmi_object/2, put_binary/2]).
%% Static file handler specific funs
-export([get_file/2]).

%% This is an internal function, but must be exported to use ?MODULE: in recursion.
-export([delegation_loop/1]).

%% ====================================================================
%% API
%% ====================================================================

%% apply/3
%% ====================================================================
%% @doc Used to interact with the socket process - issues application of
%% some code on the socket process, which then sends back the result.
%% This is needed when some data is cached at socket process - for example
%% cowboy_req:reply or :body can be evaluated only on socket process.
%% If applied fun ends with an exception, it will be rethrown in the handling process.
%% @end
-spec apply(Module :: atom(), Fun :: function(), Args :: [term()]) -> term() | no_return().
%% ====================================================================
apply(Module, Fun, Args) ->
    case get_delegation() of
        false ->
            % This is the socket process (delegation flag is set)
            % so it can just evaluate code
            erlang:apply(Module, Fun, Args);
        _ ->
            % This is a spawned process, it must call the socket process to apply the fun
            get_socket_pid() ! {apply, Module, Fun, Args},
            receive
                {result, Result} ->
                    Result;
                {except, Except} ->
                    throw(Except)
            end
    end.


%% request_processing_loop/0
%% ====================================================================
%% @doc A loop that gets evaluated while handling process is processing
%% a request.
%% @end
-spec request_processing_loop() -> ok.
%% ====================================================================
request_processing_loop() ->
    SocketProcessPid = get_socket_pid(),
    receive
        {apply, Module, Fun, Args} ->
            try
                SocketProcessPid ! {result, erlang:apply(Module, Fun, Args)},
                ?MODULE:request_processing_loop()
            catch
                T:M ->
                    ?error_stacktrace("Error in request processing loop - ~p:~p", [T, M]),
                    ok
            end;
        {flush, Actions} ->
            SocketProcessPid ! {flush, Actions},
            ?MODULE:request_processing_loop();
        terminate ->
            ok;
        {'DOWN', _, _, _, _} ->
            ?debug("Socket process died, handler process is terminating"),
            ok;
        Unknown ->
            ?warning("Unknown message in request processing loop: ~p", [Unknown]),
            ok
    end.


%% set_socket_pid/1
%% ====================================================================
%% @doc Convenience function that stores the reference to socket pid in process dict.
%% @end
-spec set_socket_pid(Pid :: pid()) -> term().
%% ====================================================================
set_socket_pid(Pid) ->
    erlang:put(socket_pid, Pid).


%% get_socket_pid/0
%% ====================================================================
%% @doc Convenience function that retrieves the reference to socket pid from process dict.
%% @end
-spec get_socket_pid() -> pid().
%% ====================================================================
get_socket_pid() ->
    erlang:get(socket_pid).


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
    HandlerModule = proplists:get_value(handler_module, Opts),
    HandlerOpts = proplists:get_value(handler_opts, Opts, []),
    Delegation = proplists:get_value(delegation, Opts, true),
    set_handler_module(HandlerModule),
    set_delegation(Delegation),

    DoDelegate=
        fun() ->
            case delegate(init, [Type, Req, HandlerOpts], 3) of
                {upgrade, protocol, Module, Req2, HandlerOpts2} ->
                    Opts1 = proplists:delete(handler_opts, Opts),
                    {upgrade, protocol, Module, Req2, [{handler_opts, HandlerOpts2} | Opts1]};
                Other -> Other
            end
        end,

    case Delegation of
        true ->
            case spawn_handling_process() of
                ok ->
                    DoDelegate();
                _ ->
                    {shutdown, Req}
            end;
        false ->
            DoDelegate()
    end.


%% handle/2
%% ====================================================================
%% @doc Cowboy handler callback, called to process a HTTP request.
%% @end
-spec handle(Req :: term(), State :: term()) -> {ok, NewReq :: term(), State :: term()}.
%% ====================================================================
handle(Req, State) ->
    delegate(handle, [Req, State], 2).


%% terminate/3
%% ====================================================================
%% @doc Cowboy handler callback, called after a request is processed.
%% @end
-spec terminate(Reason :: term(), Req :: term(), State :: term()) -> ok.
%% ====================================================================
terminate(Reason, Req, State) ->
    delegate(terminate, [Reason, Req, State], 3),
    case get_delegation() of
        true ->
            terminate_handling_process();
        false ->
            ok
    end.


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
    delegate(websocket_init, [Transport, Req, HandlerOpts], 3).


%% websocket_handle/3
%% ====================================================================
%% @doc Cowboy handler callback, called when websocket process receives
%% data packet (event) from websocket client.
%% @end
-spec websocket_handle(Data :: term(), Req :: term(), State :: term()) -> ok.
%% ====================================================================
websocket_handle(Data, Req, State) ->
    delegate(websocket_handle, [Data, Req, State], 3).


%% websocket_info/3
%% ====================================================================
%% @doc Cowboy handler callback, called when websocket process receives an erlang message.
%% @end
-spec websocket_info(Info :: term(), Req :: term(), State :: term()) -> ok.
%% ====================================================================
websocket_info(Info, Req, State) ->
    delegate(websocket_info, [Info, Req, State], 3).


%% websocket_terminate/3
%% ====================================================================
%% @doc Cowboy handler callback, called on a websocket connection finalization.
%% @end
-spec websocket_terminate(Reason :: term(), Req :: term(), State :: term()) -> ok.
%% ====================================================================
websocket_terminate(Reason, Req, State) ->
    delegate(websocket_terminate, [Reason, Req, State], 3),
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
    delegate(rest_init, [Req, HandlerOpts], 2).


%% malformed_request/2
%% ====================================================================
%% @doc Cowboy callback function
%% Checks request validity.
%% @end
-spec malformed_request(Req :: req(), State :: term()) -> {Result :: boolean(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
malformed_request(Req, State) ->
    delegate(malformed_request, [Req, State], 2, true).


%% known_methods/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns methods resolvable by the handler.
%% @end
-spec known_methods(Req :: req(), State :: term()) -> {Result :: [binary()], NewReq :: req(), NewState :: term()}.
%% ====================================================================
known_methods(Req, State) ->
    delegate(known_methods, [Req, State], 2, true).


%% allowed_methods/2
%% ====================================================================
%% @doc Cowboy callback function.
%% Returns methods that are allowed, based on version specified in URI.
%% @end
-spec allowed_methods(Req :: req(), State :: term()) -> {Result :: [binary()], NewReq :: req(), NewState :: term()}.
%% ====================================================================
allowed_methods(Req, State) ->
    delegate(allowed_methods, [Req, State], 2, true).


%% is_authorized/2
%% ====================================================================
%% @doc Cowboy callback function.
%% Returns true or false if the client is authorized to perform such request.
%% @end
-spec is_authorized(Req :: req(), State :: term()) -> {Result :: boolean(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
is_authorized(Req, State) ->
    delegate(is_authorized, [Req, State], 2, true).


%% options/2
%% ====================================================================
%% @doc Cowboy callback function.
%% Returns options / requirements associated with a resource.
%% @end
-spec options(Req :: req(), State :: term()) -> {Result :: [term()], NewReq :: req(), NewState :: term()}.
%% ====================================================================
options(Req, State) ->
    delegate(options, [Req, State], 2, true).


%% content_types_provided/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content types that can be provided. "application/json" is default.
%% It can be changed later by gui_utils:cowboy_ensure_header/3.
%% @end
-spec content_types_provided(Req :: req(), State :: term()) -> {Result :: [binary()], NewReq :: req(), NewState :: term()}.
%% ====================================================================
content_types_provided(Req, State) ->
    delegate(content_types_provided, [Req, State], 2, true).


%% languages_provided/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns list of languages in which the response can be sent.
%% @end
-spec languages_provided(Req :: req(), State :: term()) -> {Result :: [binary()], NewReq :: req(), NewState :: term()}.
%% ====================================================================
languages_provided(Req, State) ->
    delegate(languages_provided, [Req, State], 2, true).


%% charsets_provided/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns list of charsets in which the response can be encoded.
%% @end
-spec charsets_provided(Req :: req(), State :: term()) -> {Result :: [binary()], NewReq :: req(), NewState :: term()}.
%% ====================================================================
charsets_provided(Req, State) ->
    delegate(charsets_provided, [Req, State], 2, true).


%% moved_permanently/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns false or {true, Location}.
%% @end
-spec moved_permanently(Req :: req(), State :: term()) -> {Result :: boolean(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
moved_permanently(Req, State) ->
    delegate(moved_permanently, [Req, State], 2, true).


%% moved_temporarily/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns false or {true, Location}.
%% @end
-spec moved_temporarily(Req :: req(), State :: term()) -> {Result :: boolean(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
moved_temporarily(Req, State) ->
    delegate(moved_temporarily, [Req, State], 2, true).


%% resource_exists/2
%% ====================================================================
%% @doc Cowboy callback function
%% Determines if resource identified by URI exists.
%% @end
-spec resource_exists(Req :: req(), State :: term()) -> {Result :: boolean(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
resource_exists(Req, State) ->
    delegate(resource_exists, [Req, State], 2, true).


%% generate_etag/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns an etag generated from a static file.
%% @end
-spec generate_etag(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
generate_etag(Req, State) ->
    delegate(generate_etag, [Req, State], 2, true).


%% last_modified/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns file's modification time.
%% @end
-spec last_modified(Req :: req(), State :: term()) -> {Result :: integer(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
last_modified(Req, State) ->
    delegate(last_modified, [Req, State], 2, true).


%% expires/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns time of expiration of a resource.
%% @end
-spec expires(Req :: req(), State :: term()) -> {Result :: integer(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
expires(Req, State) ->
    delegate(expires, [Req, State], 2, true).


%% forbidden/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns true if access to a resource is forbidden.
%% @end
-spec forbidden(Req :: req(), State :: term()) -> {Result :: boolean(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
forbidden(Req, State) ->
    delegate(forbidden, [Req, State], 2, true).


%% get_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles GET requests.
%% @end
-spec get_resource(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
get_resource(Req, State) ->
    delegate(get_resource, [Req, State], 2, true).


%% content_types_accepted/2
%% ====================================================================
%% @doc Cowboy callback function
%% Returns content-types that are accepted by REST handler and what
%% functions should be used to process the requests.
%% @end
-spec content_types_accepted(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
content_types_accepted(Req, State) ->
    delegate(content_types_accepted, [Req, State], 2, true).


%% handle_urlencoded_data/2
%% ====================================================================
%% @doc Function handling "application/x-www-form-urlencoded" requests.
%% @end
-spec handle_urlencoded_data(Req :: req(), State :: term()) -> {Result :: boolean(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
handle_urlencoded_data(Req, State) ->
    delegate(handle_urlencoded_data, [Req, State], 2).


%% handle_json_data/2
%% ====================================================================
%% @doc Function handling "application/json" requests.
%% @end
-spec handle_json_data(Req :: req(), State :: term()) -> {Result :: boolean(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
handle_json_data(Req, State) ->
    delegate(handle_json_data, [Req, State], 2).


%% handle_multipart_data/2
%% ====================================================================
%% @doc Function handling "multipart/form-data" requests.
%% @end
-spec handle_multipart_data(Req :: req(), State :: term()) -> {Result :: boolean(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
handle_multipart_data(Req, State) ->
    delegate(handle_multipart_data, [Req, State], 2).


%% delete_resource/2
%% ====================================================================
%% @doc Cowboy callback function
%% Handles DELETE requests.
%% @end
-spec delete_resource(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), NewState :: term()}.
%% ====================================================================
delete_resource(Req, State) ->
    delegate(delete_resource, [Req, State], 2).


%% ====================================================================
%% CDMI callbacks
%% ====================================================================

%% get_cdmi_container/2
%% ====================================================================
%% @doc Callback function for cdmi.
%% @end
-spec get_cdmi_container(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), term()}.
%% ====================================================================
get_cdmi_container(Req, State) ->
    delegate(get_cdmi_container, [Req, State], 2).


%% get_cdmi_object/2
%% ====================================================================
%% @doc Callback function for cdmi.
%% @end
-spec get_cdmi_object(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), State :: term()}.
%% ====================================================================
get_cdmi_object(Req, State) ->
    delegate(get_cdmi_object, [Req, State], 2).


%% get_binary/2
%% ====================================================================
%% @doc Callback function for cdmi.
%% @end
-spec get_binary(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), State :: term()}.
%% ====================================================================
get_binary(Req, State) ->
    delegate(get_binary, [Req, State], 2).


%% put_cdmi_container/2
%% ====================================================================
%% @doc Callback function for cdmi.
%% @end
-spec put_cdmi_container(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), State :: term()}.
%% ====================================================================
put_cdmi_container(Req, State) ->
    delegate(put_cdmi_container, [Req, State], 2).


%% put_cdmi_object/2
%% ====================================================================
%% @doc Callback function for cdmi.
%% @end
-spec put_cdmi_object(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), State :: term()}.
%% ====================================================================
put_cdmi_object(Req, State) ->
    delegate(put_cdmi_object, [Req, State], 2).


%% put_binary/2
%% ====================================================================
%% @doc Callback function for cdmi.
%% @end
-spec put_binary(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), State :: term()}.
%% ====================================================================
put_binary(Req, State) ->
    delegate(put_binary, [Req, State], 2).


%% get_cdmi_capability/2
%% ====================================================================
%% @doc Callback function for cdmi.
%% @end
-spec get_cdmi_capability(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), State :: term()}.
%% ====================================================================
get_cdmi_capability(Req, State) ->
    delegate(get_cdmi_capability, [Req, State], 2).


%% ====================================================================
%% Static file handler callbacks
%% ====================================================================

%% get_file/2
%% ====================================================================
%% @doc Callback function for static file handler.
%% @end
-spec get_file(Req :: req(), State :: term()) -> {Result :: term(), NewReq :: req(), State :: term()}.
%% ====================================================================
get_file(Req, State) ->
    delegate(get_file, [Req, State], 2).


%% ====================================================================
%% Internal functions
%% ====================================================================

%% spawn_handling_process/0
%% ====================================================================
%% @doc Contacts a control_panel instance via dispatcher to spawn a handling
%% process there, whoose pid will be reported back to socket process for
%% later communication.
%% @end
-spec spawn_handling_process() -> ok | {error, timeout}.
%% ====================================================================

spawn_handling_process() ->
    SocketPid = self(),
    MsgID = 0, %% This can be 0 as one socket process sends only one request
    gen_server:call(?Dispatcher_Name, {node_chosen, {control_panel, 1, SocketPid, MsgID, {spawn_handler, SocketPid}}}),
    receive
        {worker_answer, MsgID, Resp} ->
            set_handler_pid(Resp),
            ok
    after ?handling_process_spawn_timeout ->
        ?error("Cannot spawn handling process, timeout"),
        {error, timeout}
    end.


%% terminate_handling_process/0
%% ====================================================================
%% @doc Orders the handling process to terminate.
%% @end
-spec terminate_handling_process() -> terminate.
%% ====================================================================
terminate_handling_process() ->
    get_handler_pid() ! terminate.


%% delegate/3
%% ====================================================================
%% @doc Function used to delegate a cowboy callback. Depending on if the
%% delegation flag was set to true, this will contact a handling process
%% or cause the socket process to evaluate the callback.
%% FailWithNoCall flag allows returning a no_call atom to cowboy,
%% which tells it that the function is not implemented so it can
%% decide for itself what to do. This is useful with optional cowboy
%% callbacks (mostly REST).
%% @end
-spec delegate(Fun :: function(), Args :: [term()], Arity :: integer()) -> term().
%% ====================================================================
delegate(Fun, Args, Arity) ->
    delegate(Fun, Args, Arity, false).

-spec delegate(Fun :: function(), Args :: [term()], Arity :: integer(), FailWithNoCall :: boolean()) -> term().
%% ====================================================================
delegate(Fun, Args, Arity, FailWithNoCall) ->
    HandlerModule = get_handler_module(),
    case (FailWithNoCall) andalso (not erlang:function_exported(HandlerModule, Fun, Arity)) of
        true ->
            no_call;
        false ->
            case get_delegation() of
                true ->
                    HandlerPid = get_handler_pid(),
                    HandlerPid ! {apply, HandlerModule, Fun, Args},
                    delegation_loop(HandlerPid);
                false ->
                    erlang:apply(HandlerModule, Fun, Args)
            end
    end.


%% delegation_loop/1
%% ====================================================================
%% @doc Loop that is evaluated by socket process while it has spawned a handling process
%% and is delegating tasks.
%% @end
-spec delegation_loop(HandlerPid :: pid()) -> term().
%% ====================================================================
delegation_loop(HandlerPid) ->
    receive
        {result, Res} ->
            Res;
        {apply, Module, Fun, Args} ->
            Result = try
                {result, erlang:apply(Module, Fun, Args)}
                     catch T:M ->
                         {except, {T, M}}
                     end,
            HandlerPid ! Result,
            ?MODULE:delegation_loop(HandlerPid)
    end.


%% set_handler_module/1
%% ====================================================================
%% @doc Convenience function that stores the reference to handler module in process dict.
%% @end
-spec set_handler_module(Module :: atom()) -> term().
%% ====================================================================
set_handler_module(Module) ->
    erlang:put(handler_module, Module).


%% get_handler_module/0
%% ====================================================================
%% @doc Convenience function that retrieves the reference to handler module from process dict.
%% @end
-spec get_handler_module() -> atom().
%% ====================================================================
get_handler_module() ->
    erlang:get(handler_module).


%% set_handler_pid/1
%% ====================================================================
%% @doc Convenience function that stores the reference to handler pid in process dict.
%% @end
-spec set_handler_pid(Pid :: pid()) -> term().
%% ====================================================================
set_handler_pid(Pid) ->
    erlang:put(handler_pid, Pid).


%% get_handler_pid/0
%% ====================================================================
%% @doc Convenience function that retrieves the reference to handler pid from process dict.
%% @end
-spec get_handler_pid() -> pid().
%% ====================================================================
get_handler_pid() ->
    erlang:get(handler_pid).


%% set_delegation/1
%% ====================================================================
%% @doc Convenience function that stores the delegation flag in process dict.
%% @end
-spec set_delegation(Delegation :: boolean()) -> term().
%% ====================================================================
set_delegation(Delegation) ->
    erlang:put(delegation, Delegation).


%% get_delegation/0
%% ====================================================================
%% @doc Convenience function that retrieves the delegation flag from process dict.
%% @end
-spec get_delegation() -> boolean().
%% ====================================================================
get_delegation() ->
    erlang:get(delegation).