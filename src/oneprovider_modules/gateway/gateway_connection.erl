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
    socket :: inet:socket(),
    waiting_requests :: ets:tid() %% @TODO: clear
}).

-define(CONNECTION_TIMEOUT, timer:seconds(10)).
-define(REQUEST_COMPLETION_TIMEOUT, timer:seconds(10)).

-export([start_link/3]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================

-spec start_link(Remote, Local, ConnectionManager) -> Result when
    Remote :: inet:ip_address(),
    Local :: inet:ip_address(),
    ConnectionManager :: pid(),
    Result :: {ok,Pid} | ignore | {error,Error},
     Pid :: pid(),
     Error :: {already_started,Pid} | term().
start_link(Remote, Local, ConnectionManager) ->
    gen_server:start_link(?MODULE, {Remote, Local, ConnectionManager}, []).


-spec init({Remote, Local, ConnectionManager}) -> Result when
    Remote :: inet:ip_address(),
    Local :: inet:ip_address(),
    ConnectionManager :: pid(),
    Result :: {ok,State} | {ok,State,Timeout} | {ok,State,hibernate}
        | {stop,Reason} | ignore,
     State :: #gwcstate{},
     Timeout :: timeout(),
     Reason :: term().
init({Remote, Local, ConnectionManager}) ->
    process_flag(trap_exit, true),
    TID = ets:new(waiting_requests, [private]),
    {ok, Socket} = gen_tcp:connect(Remote, ?gw_port, [binary, {packet, 4}, {active, once}, {ifaddr, Local}], ?CONNECTION_TIMEOUT),
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
handle_cast(#fetch{request = Request} = Action, #gwcstate{socket = Socket, waiting_requests = TID} = State) ->
    Data = gwproto_pb:encode_fetchrequest(Request),
    case gen_tcp:send(Socket, Data) of
        {error, Reason} -> {stop, Reason, State};
        ok ->
            Hash = gateway:compute_request_hash(Data),
            Timer = erlang:send_after(?REQUEST_COMPLETION_TIMEOUT, self(), {request_timeout, Hash}),
            ets:insert(TID, {Hash, Action, Timer}),
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

handle_info({request_timeout, Hash}, #gwcstate{waiting_requests = TID} = State) ->
    case ets:lookup(TID, Hash) of
        [] -> ignore;
        [{_, Action, _}] ->
            ets:delete(TID, Hash),
            notify(fetch_timeout, Action)
    end,
    {noreply, State};

handle_info(_Request, State) ->
    ?log_call(_Request),
    {noreply, State}.


-spec terminate(Reason, State) -> IgnoredResult when
    Reason :: normal | shutdown | {shutdown,term()} | term(),
    State :: #gwcstate{},
    IgnoredResult :: any().
terminate(Reason, #gwcstate{remote = Remote, socket = Socket, connection_manager = CM, waiting_requests = TID} = State) ->
    ?log_terminate(Reason, State),

    NotifyReason =
        case Reason of
            normal -> closed;
            _ ->
                gen_tcp:close(Socket),
                Reason
        end,

    gen_server:cast(CM, {connection_closed, Remote}),
    lists:foreach(
        fun({_, Action, _}) -> notify(fetch_send_error, NotifyReason, Action) end,
        ets:tab2list(TID)).


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
        [] -> ignore, ?warning("Ignored ~p", [Content]);
        [{_, Action, Timer}] ->
            erlang:cancel_timer(Timer),
            %% @TODO: actually complete the request
            ets:delete(TID, RequestHash),
            ?warning("Received ~p", [Content]),
            notify(fetch_complete, Action)
    end,
    ok.


-spec notify(What :: atom(), Action :: #fetch{}) -> ok.
notify(What, #fetch{notify = Notify, request = Request}) when is_atom(What) ->
    Notify ! {What, Request},
    ok.

-spec notify(What :: atom(), Reason :: term(), Action :: #fetch{}) -> ok.
notify(What, Reason, #fetch{notify = Notify, request = Request}) when is_atom(What) ->
    Notify ! {What, Reason, Request},
    ok.
