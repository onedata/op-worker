%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module coordinates access to Round Robin Database.
%% @end
%% ===================================================================
-module(gr_channel).
-behaviour(gen_server).

-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0, push/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(state, {status = not_connected, url, pid}).

%%%===================================================================
%%% API
%%%===================================================================

%% start_link/1
%% ====================================================================
%% @doc Starts the server.
%% @end
-spec start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
%% ====================================================================
start_link() ->
    gen_server:start_link({local, ?GrChannel_Name}, ?MODULE, [], []).


%% push/1
%% ====================================================================
%% @doc Pushes message to Global Registry.
%% @end
-spec push(Msg :: term()) -> ok.
%% ====================================================================
push(Msg) ->
    gen_server:cast(?GrChannel_Name, {push, Msg}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
%% @end
-spec init(Args :: term()) -> Result when
    Result :: {ok, State}
    | {ok, State, Timeout}
    | {ok, State, hibernate}
    | {stop, Reason :: term()}
    | ignore,
    State :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
init(_) ->
    erlang:process_flag(trap_exit, true),
    {ok, ChannelURL} = application:get_env(?APP_Name, global_registry_channel_url),
    ?GrChannel_Name ! connect,
    {ok, #state{url = ChannelURL}}.


%% handle_call/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
%% @end
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
    Result :: {reply, Reply, NewState}
    | {reply, Reply, NewState, Timeout}
    | {reply, Reply, NewState, hibernate}
    | {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason, Reply, NewState}
    | {stop, Reason, NewState},
    Reply :: term(),
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity,
    Reason :: term().
%% ====================================================================
handle_call(_Request, _From, State) ->
    {reply, ok, State}.


%% handle_cast/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
%% @end
-spec handle_cast(Request :: term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_cast(connected, #state{status = not_connected} = State) ->
    ?info("Connection to Global Registry established successfully."),
    {noreply, State#state{status = connected}};
handle_cast({push, Msg}, #state{pid = Pid, status = connected} = State) ->
    Pid ! {push, Msg},
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.


%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
%% @end
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_info(connect, #state{url = URL, status = not_connected} = State) ->
    {ok, Delay} = application:get_env(?APP_Name, gr_channel_next_connection_attempt_delay),
    Opts = [{keyfile, gr_plugin:get_key_path()}, {certfile, gr_plugin:get_cert_path()}],
    case websocket_client:start_link(URL, gr_channel_handler, [self()], Opts) of
        {ok, Pid} ->
            {noreply, State#state{pid = Pid}};
        Other ->
            ?error("Cannot establish connection to Global Registry due to: ~p."
            " Reconnecting in ~p seconds...", [Other, Delay]),
            timer:send_after(timer:seconds(Delay), self(), connect),
            {noreply, State}
    end;
handle_info({'EXIT', Pid, Reason}, #state{pid = Pid} = State) ->
    ?error("Connection to Global Registry lost due to: ~p. Reconnecting...", [Reason]),
    ?GrChannel_Name ! connect,
    {noreply, State#state{pid = undefined, status = not_connected}};
handle_info(_Info, State) ->
    {noreply, State}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
%% @end
-spec terminate(Reason, State :: term()) -> Any :: term() when
    Reason :: normal
    | shutdown
    | {shutdown, term()}
    | term().
%% ====================================================================
terminate(_Reason, _State) ->
    ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
%% @end
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term()},
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
