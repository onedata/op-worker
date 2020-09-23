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

-record(state, {
    monitored_processes = #{} :: #{pid() => reference()},
    handles_per_process = #{} :: #{pid() => #{lfm_context:handle_id() => lfm:handle()}}
}).

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
handle_cast(?REGISTER_OPEN(Pid, FileHandle), State) ->
    {noreply, add_process_handle(Pid, FileHandle, State)};
handle_cast(?REGISTER_CLOSE(Pid, FileHandle), State) ->
    {noreply, remove_process_handle(Pid, FileHandle, State)};
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
handle_info({'DOWN', _MonitorRef, process, Pid, _Reason}, State) ->
    {noreply, close_all_process_handles(Pid, State)};
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec add_process_handle(pid(), lfm:handle(), state()) -> state().
add_process_handle(Pid, FileHandle, #state{
    monitored_processes = MonitoredProcesses,
    handles_per_process = HandlesPerProcess
} = State) ->
    HandleId = lfm_context:get_handle_id(FileHandle),

    case maps:get(Pid, HandlesPerProcess, undefined) of
        undefined ->
            State#state{
                monitored_processes = MonitoredProcesses#{Pid => monitor(process, Pid)},
                handles_per_process = HandlesPerProcess#{Pid => #{HandleId => FileHandle}}
            };
        ProcessHandles ->
            State#state{handles_per_process = HandlesPerProcess#{
                Pid => ProcessHandles#{HandleId => FileHandle}
            }}
    end.


%% @private
-spec remove_process_handle(pid(), lfm:handle(), state()) -> state().
remove_process_handle(Pid, FileHandle, #state{
    monitored_processes = MonitoredProcesses,
    handles_per_process = HandlesPerProcess
} = State) ->
    case maps:take(Pid, HandlesPerProcess) of
        error ->
            % Possible race with {'DOWN', _, _, _, _} message
            State;
        {ProcessHandles, LeftoverHandlesPerProcess} ->
            HandleId = lfm_context:get_handle_id(FileHandle),

            case maps:remove(HandleId, ProcessHandles) of
                LeftoverProcessHandles when map_size(LeftoverProcessHandles) == 0 ->
                    {MonitorRef, LeftoverMonitoredProcesses} = maps:take(Pid, MonitoredProcesses),
                    demonitor(MonitorRef, [flush]),

                    State#state{
                        monitored_processes = LeftoverMonitoredProcesses,
                        handles_per_process = LeftoverHandlesPerProcess
                    };
                LeftoverProcessHandles ->
                    State#state{handles_per_process = LeftoverHandlesPerProcess#{
                        Pid => LeftoverProcessHandles
                    }}
            end
    end.


%% @private
-spec close_all_process_handles(pid(), state()) -> state().
close_all_process_handles(Pid, #state{handles_per_process = HandlesPerProcess} = State) ->
    case maps:take(Pid, HandlesPerProcess) of
        error ->
            State;
        {ProcessHandles, LeftoverHandlesPerProcess} ->
            lists:foreach(
                fun({_HandleId, FileHandle}) -> lfm:release(FileHandle) end,
                maps:to_list(ProcessHandles)
            ),
            State#state{handles_per_process = LeftoverHandlesPerProcess}
    end.
