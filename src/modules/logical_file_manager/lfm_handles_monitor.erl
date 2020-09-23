%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Monitors cowboy processes (gui/REST handlers) that open file handles
%%% so that those handles can be closed when said processes unexpectedly
%%% dies (client abruptly closes connection).
%%% @end
%%%--------------------------------------------------------------------
-module(lfm_handles_monitor).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([register_open/1, register_close/1]).

-export([start_link/0, spec/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-record(state, {monitored_processes = #{} :: #{pid() => reference()}}).

-type state() :: #state{}.


-define(REGISTER_OPEN(__PROC, __FILE_HANDLE), {register_open, __PROC, __FILE_HANDLE}).
-define(REGISTER_CLOSE(__PROC, __FILE_HANDLE), {register_close, __PROC, __FILE_HANDLE}).


%%%===================================================================
%%% API
%%%===================================================================


-spec register_open(lfm:handle()) -> ok.
register_open(FileHandle) ->
    gen_server2:cast(?MODULE, ?REGISTER_OPEN(self(), FileHandle)).


-spec register_close(lfm:handle()) -> ok.
register_close(FileHandle) ->
    gen_server2:cast(?MODULE, ?REGISTER_OPEN(self(), FileHandle)).


-spec start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec spec() -> supervisor:child_spec().
spec() -> #{
    id => ?MODULE,
    start => {?MODULE, start_link, []},
    restart => permanent,
    shutdown => timer:seconds(10),
    type => worker,
    modules => [?MODULE]
}.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init(_Args) ->
    handles_per_process:release_all_dead_processes_handles(),
    {ok, #state{}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, wrong_request, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast(?REGISTER_OPEN(Pid, FileHandle), #state{monitored_processes = Processes} = State) ->
    handles_per_process:register_handle(Pid, FileHandle),
    NewState = case maps:is_key(Pid, Processes) of
        true -> State;
        false -> State#state{monitored_processes = Processes#{Pid => monitor(process, Pid)}}
    end,
    {noreply, NewState};
handle_cast(?REGISTER_CLOSE(Pid, FileHandle), #state{monitored_processes = Processes} = State) ->
    NewState = case handles_per_process:deregister_handle(Pid, FileHandle) of
        {ok, []} ->
            {MonitorRef, LeftoverProcesses} = maps:take(Pid, Processes),
            demonitor(MonitorRef, [flush]),

            State#state{monitored_processes = LeftoverProcesses};
        _ ->
            State
    end,
    {noreply, NewState};
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info({'DOWN', _MonitorRef, process, Pid, _Reason}, #state{
    monitored_processes = Processes
} = State) ->
    handles_per_process:release_all_process_handles(Pid),
    {noreply, State#state{monitored_processes = maps:remove(Pid, Processes)}};
handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: state()) -> term().
terminate(_Reason, _State) ->
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
