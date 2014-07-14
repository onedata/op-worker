%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains functions used to set up and manage
%% asynchronous comet processes.
%% @end
%% ===================================================================

-module(gui_comet).
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

% Comet API
-export([spawn/1, init_comet/3, comet_supervisor/2, is_comet_process/0, flush/0]).


%% ====================================================================
%% API functions
%% ====================================================================

%% spawn/1
%% ====================================================================
%% @doc Spawns an asynchronous process connected to the calling process.
%% IMPORTANT! The calling process must be the websocket process of n2o framework.
%% In other words, it should be called from event/1 function of page module.
%% Allows flushing actions to the main process (async updates).
%% Every instance of comet will get a supervisor to make sure it won't go rogue
%% after the calling process has finished.
%% @end
-spec spawn(CometFun :: function()) -> {ok, pid()} | no_return().
%% ====================================================================
spawn(CometFun) ->
    % Get session ID so comet process can write/read from session memory
    #context{session = Session} = ?CTX,
    % Prevent comet and supervisor from killing the calling process on crash
    process_flag(trap_exit, true),
    % Spawn comet process, _link so it will die if the calling process craches
    CometPid = spawn_link(?MODULE, init_comet, [self(), CometFun, Session]),
    % Spawn comet supervisor, _link so it will die if the calling process craches
    spawn_link(?MODULE, comet_supervisor, [self(), CometPid]),
    {ok, CometPid}.


%% init_comet/2
%% ====================================================================
%% @doc Internal function used to initialize an asynchronous "comet" process.
%% @end
-spec init_comet(OwnerPid :: pid(), Fun :: fun(), SessionID :: term()) -> no_return().
%% ====================================================================
init_comet(OwnerPid, Fun, SessionID) ->
    put(ws_process, OwnerPid),
    Context = wf_context:init_context([]),
    wf_context:context(Context#context{session = SessionID}),
    Fun().


%% comet_supervisor/2
%% ====================================================================
%% @doc Internal function evaluated by comet supervisor. The supervisor will
%% kill the comet process whenever comet creator process finishes.
%% @end
-spec comet_supervisor(CallingPid :: pid(), CometPid :: pid()) -> no_return().
%% ====================================================================
comet_supervisor(CallingPid, CometPid) ->
    MonitorRef = erlang:monitor(process, CallingPid),
    receive
        {'DOWN', MonitorRef, _, _, _} -> exit(CometPid, kill)
    end.


%% is_comet_process/0
%% ====================================================================
%% @doc Returns true if calling process is a comet process.
%% @end
-spec is_comet_process() -> boolean().
%% ====================================================================
is_comet_process() ->
    get(ws_process) /= undefined.


%% flush/0
%% ====================================================================
%% @doc Flushes accumulated events to websocket process, causing page update.
%% @end
-spec flush() -> ok.
%% ====================================================================
flush() ->
    Actions = wf_context:actions(),
    wf_context:clear_actions(),
    case Actions of
        [] ->
            skip;
        undefined ->
            skip;
        _ ->
            get(ws_process) ! {flush, Actions}
    end,
    ok.