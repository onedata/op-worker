%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Monitors cowboy processes (gui/REST handlers) that open file handles
%%% so that all handles associated with process that unexpectedly dies
%%% (e.g. client abruptly closes connection) can be closed.
%%% @end
%%%--------------------------------------------------------------------
-module(lfm_handles_monitor).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([monitor_process/1, demonitor_process/1]).

-export([start_link/0, spec/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-record(state, {processes = #{} :: #{pid() => reference()}}).

-type state() :: #state{}.


-define(MONITOR_PROCESS(__PROC), {monitor_process, __PROC}).
-define(DEMONITOR_PROCESS(__PROC), {demonitor_process, __PROC}).

-define(DEFAULT_REQUEST_TIMEOUT, timer:minutes(1)).


%%%===================================================================
%%% API
%%%===================================================================


-spec monitor_process(pid()) -> ok.
monitor_process(Process) ->
    call_lfm_handles_monitor(?MONITOR_PROCESS(Process)).


-spec demonitor_process(pid()) -> ok.
demonitor_process(Process) ->
    call_lfm_handles_monitor(?DEMONITOR_PROCESS(Process)).


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
-spec init(Args :: term()) -> {ok, State :: state()}.
init(_Args) ->
    % In case when server died and was restarted try to release all handles for dead processes.
    process_handles:release_all_dead_processes_handles(),
    {ok, #state{}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: state()) ->
    {reply, Reply :: term(), NewState :: state()}.
handle_call(?MONITOR_PROCESS(Process), _From, #state{processes = Processes} = State) ->
    NewState = case maps:is_key(Process, Processes) of
        true -> State;
        false -> State#state{processes = Processes#{Process => monitor(process, Process)}}
    end,
    {reply, ok, NewState};
handle_call(?DEMONITOR_PROCESS(Process), _From, #state{processes = Processes} = State) ->
    NewState = case maps:take(Process, Processes) of
        error ->
            State;
        {MonitorRef, LeftoverProcesses} ->
            demonitor(MonitorRef, [flush]),
            State#state{processes = LeftoverProcesses}
    end,
    {reply, ok, NewState};
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
    {noreply, NewState :: state()}.
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
    {noreply, NewState :: state()}.
handle_info({'DOWN', _MonitorRef, process, Pid, _Reason}, #state{processes = Processes} = State) ->
    process_handles:release_all_process_handles(Pid),
    {noreply, State#state{processes = maps:remove(Pid, Processes)}};
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
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), State :: state()) ->
    ok.
terminate(_Reason, _State) ->
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(), Extra :: term()) ->
    {ok, NewState :: state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec call_lfm_handles_monitor(term()) -> ok | {error, term()}.
call_lfm_handles_monitor(Msg) ->
    try
        gen_server2:call(?MODULE, Msg, ?DEFAULT_REQUEST_TIMEOUT)
    catch
        exit:{noproc, _} ->
            ?error("Lfm handles monitor process does not exist"),
            {error, no_lfm_handles_monitor};
        exit:{normal, _} ->
            ?error("Exit of lfm handles monitor process"),
            {error, no_lfm_handles_monitor};
        exit:{timeout, _} ->
            ?error("Timeout of lfm handles monitor process"),
            ?ERROR_TIMEOUT;
        Type:Reason ->
            ?error_stacktrace("Cannot call lfm handles monitor due to ~p:~p", [Type, Reason]),
            {error, Reason}
    end.
