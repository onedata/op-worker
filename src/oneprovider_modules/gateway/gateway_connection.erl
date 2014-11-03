%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: @TODO: write me
%% @end
%% ===================================================================

-module(gateway_connection).
-author("Konrad Zemek").
-behavior(gen_server).

-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").

-record(gwcstate, {
    remote :: inet:ip_address(),
    connection_manager :: pid(),
    socket :: gen_tcp:socket(),
    waiting_requests :: ets:tid() %% @TODO: clear
}).

-define(CONNECTION_TIMEOUT, timer:seconds(10)).

-export([start_link/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================

-spec start_link(Remote, ConnectionManager) -> Result when
    Remote :: inet:ip_address(),
    ConnectionManager :: pid(),
    Result :: {ok,Pid} | ignore | {error,Error},
     Pid :: pid(),
     Error :: {already_started,Pid} | term().
start_link(Remote, ConnectionManager) ->
    gen_server:start_link(?MODULE, {Remote, ConnectionManager}, []).


-spec init({Remote, ConnectionManager}) -> Result when
    Remote :: inet:ip_address(),
    ConnectionManager :: pid(),
    Result :: {ok,State} | {ok,State,Timeout} | {ok,State,hibernate}
        | {stop,Reason} | ignore,
     State :: #gwcstate{},
     Timeout :: timeout(),
     Reason :: term().
init({Remote, ConnectionManager}) ->
    process_flag(trap_exit, true),
    TID = ets:new(waiting_requests, [private]),
    {ok, Socket} = gen_tcp:connect(Remote, ?gw_port, [binary, {packet, 4}, {active, once}], ?CONNECTION_TIMEOUT),
    {ok, #gwcstate{remote = Remote, socket = Socket, connection_manager = ConnectionManager, waiting_requests = TID}}.


-spec handle_call(Request, From, State) -> Result when
    Request :: term(),
    From :: {pid(),any()},
    State :: #gwcstate{},
    Result :: {reply,Reply,NewState} | {reply,Reply,NewState,Timeout}
        | {reply,Reply,NewState,hibernate}
        | {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,Reply,NewState} | {stop,Reason,NewState},
     Reply :: term(),
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: term().
handle_call(_Request, _From, State) ->
    ?log_call(_Request),
    {noreply, State}.


-spec handle_cast(Request, State) -> Result when
    Request :: term(),
    State :: #gwcstate{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,NewState},
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: term().
handle_cast(#fetch{request = Request, notify = Notify} = Action, #gwcstate{socket = Socket, waiting_requests = TID} = State) ->
    Data = gwproto_pb:encode_fetchrequest(Request),
    case gen_tcp:send(Socket, Data) of
        {error, Reason} ->
            Notify ! {fetch_send_error, Reason, Request},
            {stop, Reason, State};
        ok ->
            ets:insert(TID, {gateway:compute_request_hash(Data), Action}),
            {noreply, State}
    end;

handle_cast(_Request, State) ->
    ?log_call(_Request),
    {noreply, State}.


-spec handle_info(Info, State) -> Result when
    Info :: timeout | term(),
    State :: #gwcstate{},
    Result :: {noreply,NewState} | {noreply,NewState,Timeout}
        | {noreply,NewState,hibernate}
        | {stop,Reason,NewState},
     NewState :: term(),
     Timeout :: timeout(),
     Reason :: normal | term().
handle_info({tcp, Socket, Data}, #gwcstate{waiting_requests = TID} = State) ->
    ok = inet:setopts(Socket, [{active, once}]),
    try
        Reply = gwproto_pb:decode_fetchreply(Data),
        complete_request(TID, Reply)
    catch
        Error:Reason -> ?warning("~p: Couldn't decode reply: {~p, ~p}", [?MODULE, Error, Reason])
    end,

    {noreply, State};

handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};

handle_info({tcp_error, _Socket, Reason}, State) ->
    {stop, Reason, State};

handle_info(_Request, State) ->
    ?log_call(_Request),
    {noreply, State}.


-spec terminate(Reason, State) -> IgnoredResult when
    Reason :: normal | shutdown | {shutdown,term()} | term(),
    State :: #gwcstate{},
    IgnoredResult :: any().
terminate(Reason, #gwcstate{remote = Remote, socket = Socket, connection_manager = CM} = State) ->
    ?log_terminate(Reason, State),
    case Reason of
        normal -> ok;
        _ -> gen_tcp:close(Socket)
    end,
    gen_server:cast(CM, {connection_closed, Remote}).


-spec code_change(OldVsn, State, Extra) -> {ok, NewState} | {error, Reason} when
    OldVsn :: Vsn | {down, Vsn},
     Vsn :: term(),
    State :: #gwcstate{},
    Extra :: term(),
    NewState :: #gwcstate{},
    Reason :: term().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================


-spec complete_request(TID :: ets:tid(), Reply :: #fetchreply{}) -> ok.
complete_request(TID, #fetchreply{content = Content, request_hash = RequestHash}) ->
    case ets:lookup(TID, RequestHash) of
        [] -> ignore;
        [{_, #fetch{notify = Notify, request = Request}}] ->
            %% @TODO: actually complete the request
            ets:delete(TID, RequestHash),
            Notify ! {fetch_complete, Request}
    end,
    ?warning("Received ~p", [Content]),
    ok.
