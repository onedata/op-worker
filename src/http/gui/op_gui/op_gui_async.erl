%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module allows for easy creation and management of asynchronous
%%% processes that can aid in pushing information about
%%% model changes to client side.
%%% @end
%%%-------------------------------------------------------------------
-module(op_gui_async).
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").
%% API
-export([spawn/1, kill_async_processes/0]).
-export([get_ws_process/0]).
-export([send/1, send/2]).
-export([push_created/2, push_created/3]).
-export([push_updated/2, push_updated/3]).
-export([push_deleted/2, push_deleted/3]).

% Keys in process dictionary used to store PIDs of processes.
-define(WEBSOCKET_PROCESS_KEY, ws_process).
-define(ASYNC_PROCESSES_KEY, async_processes).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates an asynchronous process that can communicate with the calling
%% process. Functions push_created/2, push_updated/2 and push_deleted/2 can be
%% called from the async process to push information through WebSocket
%% channel to the client about model changes.
%% @end
%%--------------------------------------------------------------------
-spec spawn(Fun :: fun()) -> {ok, Pid :: pid()}.
spawn(Fun) ->
    % Prevent async proc from killing the calling proc on crash
    process_flag(trap_exit, true),
    WSPid = resolve_websocket_pid(),
    SessionId = op_gui_session:get_session_id(),
    UserId = op_gui_session:get_user_id(),
    Host = op_gui_session:get_requested_host(),
    Pid = spawn_link(fun() ->
        put(?WEBSOCKET_PROCESS_KEY, WSPid),
        op_gui_session:set_session_id(SessionId),
        op_gui_session:set_user_id(UserId),
        op_gui_session:set_requested_host(Host),
        Fun()
    end),
    append_async_process(Pid),
    {ok, Pid}.


%%--------------------------------------------------------------------
%% @doc
%% Kills all async processes that have been spawned by the calling process.
%% @end
%%--------------------------------------------------------------------
-spec kill_async_processes() -> ok.
kill_async_processes() ->
    lists:foreach(
        fun(Pid) ->
            exit(Pid, kill)
        end, get_async_processes()),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Returns the parent websocket process for calling process or
%% undefined if this is not a gui async process.
%% @end
%%--------------------------------------------------------------------
-spec get_ws_process() -> pid() | undefined.
get_ws_process() ->
    get(?WEBSOCKET_PROCESS_KEY).


%%--------------------------------------------------------------------
%% @doc
%% Sends any message to client connected via WebSocket.
%% This variant can be used only from a process spawned by op_gui_async:spawn in
%% backend init callback.
%% @end
%%--------------------------------------------------------------------
-spec send(Message :: proplists:proplist()) -> ok.
send(Message) ->
    send(Message, get_ws_process()).


%%--------------------------------------------------------------------
%% @doc
%% Sends any message to client connected via WebSocket.
%% Pushes the data to given pid (that must be a websocket pid).
%% @end
%%--------------------------------------------------------------------
-spec send(Message :: proplists:proplist(), Pid :: pid()) -> ok.
send(Message, Pid) ->
    Pid ! {send, Message},
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Pushes information about record creation to the client via WebSocket
%% channel. The Data is a proplist that will be translated to JSON, it must
%% include <<"id">> field.
%% This variant can be used only from a process spawned by op_gui_async:spawn in
%% backend init callback.
%% @end
%%--------------------------------------------------------------------
-spec push_created(ResType :: binary(), Data :: proplists:proplist()) -> ok.
push_created(ResourceType, Data) ->
    push_created(ResourceType, Data, get_ws_process()).


%%--------------------------------------------------------------------
%% @doc
%% Pushes information about record creation to the client via WebSocket
%% channel. The Data is a proplist that will be translated to JSON, it must
%% include <<"id">> field.
%% Pushes the change to given pid (that must be a websocket pid).
%% @end
%%--------------------------------------------------------------------
-spec push_created(ResType :: binary(), Data :: proplists:proplist(),
    Pid :: pid()) -> ok.
push_created(ResourceType, Data, Pid) ->
    Pid ! {push_created, ResourceType, Data},
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Pushes information about model update to the client via WebSocket channel.
%% The Data is a proplist that will be translated to JSON, it must include
%% <<"id">> field. It might also be the updated data of many records.
%% This variant can be used only from a process spawned by op_gui_async:spawn in
%% backend init callback.
%% @end
%%--------------------------------------------------------------------
-spec push_updated(ResType :: binary(), Data :: proplists:proplist()) -> ok.
push_updated(ResourceType, Data) ->
    push_updated(ResourceType, Data, get_ws_process()).


%%--------------------------------------------------------------------
%% @doc
%% Pushes information about model update to the client via WebSocket channel.
%% The Data is a proplist that will be translated to JSON, it must include
%% <<"id">> field. It might also be the updated data of many records.
%% Pushes the change to given pid (that must be a websocket pid).
%% @end
%%--------------------------------------------------------------------
-spec push_updated(ResType :: binary(), Data :: proplists:proplist(),
    Pid :: pid()) -> ok.
push_updated(ResourceType, Data, Pid) ->
    Pid ! {push_updated, ResourceType, Data},
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Pushes information about record deletion from model to the client
%% via WebSocket channel.
%% This variant can be used only from a process spawned by op_gui_async:spawn in
%% backend init callback.
%% @end
%%--------------------------------------------------------------------
-spec push_deleted(ResType :: binary(), IdOrIds :: binary() | [binary()]) -> ok.
push_deleted(ResourceType, IdOrIds) ->
    push_deleted(ResourceType, IdOrIds, get_ws_process()).


%%--------------------------------------------------------------------
%% @doc
%% Pushes information about record deletion from model to the client
%% via WebSocket channel.
%% Pushes the change to given pid (that must be a websocket pid).
%% @end
%%--------------------------------------------------------------------
-spec push_deleted(ResType :: binary(), IdOrIds :: binary() | [binary()],
    Pid :: pid()) -> ok.
push_deleted(ResourceType, IdOrIds, Pid) ->
    Ids = case IdOrIds of
        Bin when is_binary(Bin) ->
            [Bin];
        List when is_list(List) ->
            List
    end,
    Pid ! {push_deleted, ResourceType, Ids},
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolves websocket pid - if this is the child of websocket process, returns
%% its parent, else returns self.
%% @end
%%--------------------------------------------------------------------
-spec resolve_websocket_pid() -> WSPid :: pid().
resolve_websocket_pid() ->
    Self = self(),
    case get_ws_process() of
        undefined ->
            % WS process not in process memory -> this is the websocket process
            Self;
        Self ->
            % WS process same as self() -> this is the websocket process
            Self;
        Other ->
            % WS process is different -> this is websocket process child
            Other
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns all async processes spawned by the calling process.
%% @end
%%--------------------------------------------------------------------
-spec get_async_processes() -> [pid()].
get_async_processes() ->
    case get(?ASYNC_PROCESSES_KEY) of
        undefined ->
            [];
        List when is_list(List) ->
            List
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Add the pid of an async process to the list of processes started by
%% the calling process.
%% @end
%%--------------------------------------------------------------------
-spec append_async_process(Pid :: pid()) -> ok.
append_async_process(Pid) ->
    put(?ASYNC_PROCESSES_KEY, [Pid | get_async_processes()]),
    ok.
