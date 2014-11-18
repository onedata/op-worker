%% %% ===================================================================
%% %% @author Konrad Zemek
%% %% @copyright (C): 2014 ACK CYFRONET AGH
%% %% This software is released under the MIT license
%% %% cited in 'LICENSE.txt'.
%% %% @end
%% %% ===================================================================
%% %% @doc aggregator is responsible for aggregating messages from gateways
%% %% and notifying a process with job status
%% %% @end
%% %% ===================================================================
%%
-module(aggregator).
%% -author("Konrad Zemek").
%% -behavior(gen_server).
%%
%% -include("oneprovider_modules/gateway/gateway.hrl").
%%
%% -record(aggregator_state, {
%%     notify :: function() | atom() | pid(),
%%     file_id :: string(),
%%     offset :: non_neg_integer(),
%%     size :: pos_integer(),
%%     read = 0 :: non_neg_integer()
%% }).
%%
%% -export([start_link/4]).
%% %% gen_server callbacks
%% -export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
%%     code_change/3]).
%%
%% %% ====================================================================
%% %% API functions
%% %% ====================================================================
%%
%%
%% %% start_link/1
%% %% ====================================================================
%% %% @doc Starts gateway_dispatcher gen_server.
%% -spec start_link(Notify, FileId, Offset, Size) -> Result when
%%     Notify :: function() | atom() | pid(),
%%     FileId :: string(),
%%     Offset :: non_neg_integer(),
%%     Size :: pos_integer(),
%%     Result :: {ok,Pid} | ignore | {error,Error},
%%      Pid :: pid(),
%%      Error :: {already_started,Pid} | term().
%% %% ====================================================================
%% start_link(Notify, FileId, Offset, Size) ->
%%     gen_server:start_link(?MODULE, {Notify, FileId, Offset, Size}, []).
%%
%%
%% %% init/1
%% %% ====================================================================
%% %% @doc Initializes gateway dispatcher, including spinning up connection managers
%% %% for each entry in NetworkInterfaces list.
%% %% @end
%% %% @see gen_server
%% -spec init({Notify, FileId, Offset, Size}) -> Result when
%%     Notify :: function() | atom() | pid(),
%%     FileId :: string(),
%%     Offset :: non_neg_integer(),
%%     Size :: pos_integer(),
%%     Result :: {ok,State} | {ok,State,Timeout} | {ok,State,hibernate}
%%         | {stop,Reason} | ignore,
%%      State :: #aggregator_state{},
%%      Timeout :: timeout(),
%%      Reason :: term().
%% %% ====================================================================
%% init({Notify, FileId, Offset, Size}) ->
%%     process_flag(trap_exit, true),
%%     {ok, #aggregator_state{notify = Notify, file_id = FileId, offset = Offset,
%%         size = Size}}.
%%
%%
%% %% handle_call/3
%% %% ====================================================================
%% %% @doc Handles a call.
%% %% @see gen_server
%% -spec handle_call(Request, From, State) -> Result when
%%     Request :: term(),
%%     From :: {pid(),any()},
%%     State :: #aggregator_state{},
%%     Result :: {reply,Reply,NewState} | {reply,Reply,NewState,Timeout}
%%         | {reply,Reply,NewState,hibernate}
%%         | {noreply,NewState} | {noreply,NewState,Timeout}
%%         | {noreply,NewState,hibernate}
%%         | {stop,Reason,Reply,NewState} | {stop,Reason,NewState},
%%      Reply :: term(),
%%      NewState :: term(),
%%      Timeout :: timeout(),
%%      Reason :: term().
%% %% ====================================================================
%% handle_call(_Request, _From, State) ->
%%     ?log_call(_Request),
%%     {noreply, State}.
%%
%%
%% %% handle_cast/3
%% %% ====================================================================
%% %% @doc Handles a cast. Connection managers register themselves with the
%% %% dispatcher, and requests are distributed between registered managers.
%% %% @see gen_server
%% -spec handle_cast(Request, State) -> Result when
%%     Request :: term(),
%%     State :: #aggregator_state{},
%%     Result :: {noreply,NewState} | {noreply,NewState,Timeout}
%%         | {noreply,NewState,hibernate}
%%         | {stop,Reason,NewState},
%%      NewState :: term(),
%%      Timeout :: timeout(),
%%      Reason :: term().
%% %% ====================================================================
%% handle_cast(_Request, State) ->
%%     ?log_call(_Request),
%%     {noreply, State}.
%%
%%
%% %% handle_info/3
%% %% ====================================================================
%% %% @doc Handles messages. Mainly handles messages from socket in active mode.
%% %% @see gen_server
%% -spec handle_info(Info, State) -> Result when
%%     Info :: timeout | term(),
%%     State :: #aggregator_state{},
%%     Result :: {noreply,NewState} | {noreply,NewState,Timeout}
%%         | {noreply,NewState,hibernate}
%%         | {stop,Reason,NewState},
%%      NewState :: term(),
%%      Timeout :: timeout(),
%%      Reason :: normal | term().
%% %% ====================================================================
%% handle_info({fetch_complete, Num, #gw_fetch{offset = O}}, State) ->
%%     State#aggregator_state.notify ! {error, Reason};
%%
%% handle_info(_Request, State) ->
%%     ?log_call(_Request),
%%     {noreply, State}.
%%
%%
%% %% terminate/2
%% %% ====================================================================
%% %% @doc Cleans up any state associated with the dispatcher, including terminating
%% %% connection managers.
%% %% @end
%% %% @see gen_server
%% -spec terminate(Reason, State) -> IgnoredResult when
%%     Reason :: normal | shutdown | {shutdown,term()} | term(),
%%     State :: #aggregator_state{},
%%     IgnoredResult :: any().
%% %% ====================================================================
%% terminate(_Reason, State) ->
%%     ?log_terminate(_Reason, State),
%%     ok.
%%
%%
%% %% code_change/3
%% %% ====================================================================
%% %% @doc Performs any actions necessary on code change.
%% %% @see gen_server
%% -spec code_change(OldVsn, State, Extra) -> {ok, NewState} | {error, Reason} when
%%     OldVsn :: Vsn | {down, Vsn},
%%      Vsn :: term(),
%%     State :: #aggregator_state{},
%%     Extra :: term(),
%%     NewState :: #aggregator_state{},
%%     Reason :: term().
%% %% ====================================================================
%% code_change(_OldVsn, State, _Extra) ->
%%     {ok, State}.
%%
%%
%% %% ====================================================================
%% %% Internal functions
%% %% ====================================================================
