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
-include_lib("ctool/include/logging.hrl").

-record(gwcstate, {
    socket :: gen_tcp:socket()
}).

-define(CONNECTION_TIMEOUT, timer:seconds(10)).

-export([start_link/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================

-spec start_link(Remote, Callback) -> Result when
    Remote :: inet:ip_address(),
    Callback :: function(),
    Result :: {ok,Pid} | ignore | {error,Error},
     Pid :: pid(),
     Error :: {already_started,Pid} | term().
start_link(Remote, Callback) ->
    gen_server:start_link(?MODULE, {Remote, Callback}, []).


-spec init({Remote, Callback}) -> Result when
    Remote :: inet:ip_address(),
    Callback :: function(),
    Result :: {ok,State} | {ok,State,Timeout} | {ok,State,hibernate}
        | {stop,Reason} | ignore,
     State :: #gwcstate{},
     Timeout :: timeout(),
     Reason :: term().
init({Remote, Callback}) ->
    process_flag(trap_exit, true),
    {ok, Socket} = gen_tcp:connect(Remote, ?gw_port, [binary, {packet, 4}, {active, once}], ?CONNECTION_TIMEOUT),
    Callback(Socket),
    {ok, #gwcstate{socket = Socket}}.


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
handle_cast(#fetch{request = Request, notify = Notify}, #gwcstate{socket = Socket} = State) ->
    Data = gwproto_pb:encode_fetchrequest(Request),
    case gen_tcp:send(Socket, Data) of
        {error, Reason} ->
            Notify ! {fetch_send_error, Reason},
            {stop, Reason, State};
        ok ->
            {noreply, State}
    end;

handle_cast(_Request, State) ->
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
handle_info({tcp, Socket, Data}, State) ->
    ok = gen_tcp:setopts(Socket, [{active, once}]),
    ?warning("Received %p bytes", [size(Data)]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};

handle_info({tcp_error, _Socket, Reason}, State) ->
    {stop, Reason, State};

handle_info(_Request, State) ->
    {noreply, State}.


-spec terminate(Reason, State) -> IgnoredResult when
    Reason :: normal | shutdown | {shutdown,term()} | term(),
    State :: #gwcstate{},
    IgnoredResult :: any().
terminate(closed, _State) ->
    ok;
terminate(_Reason, #gwcstate{socket = Socket}) ->
    gen_tcp:close(Socket).


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
