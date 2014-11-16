%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module implements {@link worker_plugin_behaviour} and
%% allows for communication with Global Registry.
%% @end
%% ===================================================================
-module(gr_channel).
-behaviour(worker_plugin_behaviour).

-include("registered_names.hrl").
-include("oneprovider_modules/gr_channel/gr_channel.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([connect/0, disconnect/0, push/1]).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/2, cleanup/0]).

-define(PROTOCOL_VERSION, 1).

%% ===================================================================
%% API
%% ===================================================================

%% connect/0
%% ====================================================================
%% @doc Connects to Global Registry.
%% @end
%% ====================================================================
-spec connect() -> ok.
%% ====================================================================
connect() ->
    gen_server:call(?Dispatcher_Name, {gr_channel, ?PROTOCOL_VERSION, connect}).


%% disconnect/0
%% ====================================================================
%% @doc Disconnects from Global Registry.
%% @end
%% ====================================================================
-spec disconnect() -> ok.
%% ====================================================================
disconnect() ->
    gen_server:call(?Dispatcher_Name, {gr_channel, ?PROTOCOL_VERSION, disconnect}).


%% push/1
%% ====================================================================
%% @doc Pushes message to Global Registry.
%% @end
%% ====================================================================
-spec push(Msg :: term()) -> ok.
%% ====================================================================
push(Msg) ->
    gen_server:call(?Dispatcher_Name, {gr_channel, ?PROTOCOL_VERSION, {push, Msg}}).


%% ===================================================================
%% worker_plugin_behaviour callbacks
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1.
%% @end
%% ====================================================================
-spec init(Args :: term()) -> ok | {error, Error :: any()}.
%% ====================================================================
init(_) ->
    {ok, URL} = application:get_env(?APP_Name, global_registry_channel_url),
    ets:new(?GR_CHANNEL_TABLE, [set, named_table, public]),
    ets:insert(?GR_CHANNEL_TABLE, #?GR_CHANNEL_STATE{status = not_connected, url = URL}).


%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1.
%% @end
%% ====================================================================
-spec handle(ProtocolVersion :: term(), Request) -> Result when
    Request :: ping | healthcheck | get_version |
    {update_state, list(), list()} |
    {get_worker, atom()} |
    get_nodes,
    Result :: ok | {error, Error} | pong | Version,
    Version :: term(),
    Error :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(_ProtocolVersion, connect) ->
    [State] = ets:lookup(?GR_CHANNEL_TABLE, ?GR_CHANNEL_STATE),
    case State of
        #?GR_CHANNEL_STATE{status = not_connected, url = URL} ->
            {ok, Delay} = application:get_env(?APP_Name, gr_channel_next_connection_attempt_delay),
            Opts = [{keyfile, gr_plugin:get_key_path()}, {certfile, gr_plugin:get_cert_path()}],
            case websocket_client:start_link(URL, gr_channel_handler, [self()], Opts) of
                {ok, Pid} ->
                    ets:insert(?GR_CHANNEL_TABLE, State#?GR_CHANNEL_STATE{pid = Pid});
                Other ->
                    ?error("Cannot establish connection to Global Registry due to: ~p."
                    " Reconnecting in ~p seconds...", [Other, Delay]),
                    timer:apply_after(timer:seconds(Delay), gen_server, call, [?Dispatcher_Name, {gr_channel, ?PROTOCOL_VERSION, connect}])
            end;
        _ -> ok
    end,
    ok;

handle(_ProtocolVersion, disconnect) ->
    [State] = ets:lookup(?GR_CHANNEL_TABLE, ?GR_CHANNEL_STATE),
    case State of
        #?GR_CHANNEL_STATE{status = connected, pid = Pid} -> Pid ! terminate;
        _ -> ok
    end;

handle(_ProtocolVersion, connected) ->
    ?info("Connection to Global Registry established successfully."),
    [State] = ets:lookup(?GR_CHANNEL_TABLE, ?GR_CHANNEL_STATE),
    ets:insert(?GR_CHANNEL_TABLE, State#?GR_CHANNEL_STATE{status = connected}),
    ok;

handle(_ProtocolVersion, {connection_lost, {normal, _}}) ->
    ?error("Connection to Global Registry closed."),
    [State] = ets:lookup(?GR_CHANNEL_TABLE, ?GR_CHANNEL_STATE),
    ets:insert(?GR_CHANNEL_TABLE, State#?GR_CHANNEL_STATE{status = not_connected, pid = undefined}),
    ok;

handle(_ProtocolVersion, {connection_lost, Reason}) ->
    ?error("Connection to Global Registry lost due to: ~p. Reconnecting...", [Reason]),
    [State] = ets:lookup(?GR_CHANNEL_TABLE, ?GR_CHANNEL_STATE),
    ets:insert(?GR_CHANNEL_TABLE, State#?GR_CHANNEL_STATE{status = not_connected, pid = undefined}),
    gen_server:call(?Dispatcher_Name, {gr_channel, ?PROTOCOL_VERSION, connect}),
    ok;

handle(_ProtocolVersion, {push, Msg}) ->
    [State] = ets:lookup(?GR_CHANNEL_TABLE, ?GR_CHANNEL_STATE),
    case State of
        #?GR_CHANNEL_STATE{status = connected, pid = Pid} -> Pid ! {push, Msg};
        _ -> ok
    end,
    ok.


%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%% ====================================================================
-spec cleanup() -> Result when
    Result :: ok.
%% ====================================================================
cleanup() ->
    [State] = ets:lookup(?GR_CHANNEL_TABLE, ?GR_CHANNEL_STATE),
    case State of
        #?GR_CHANNEL_STATE{status = connected, pid = Pid} -> Pid ! terminate;
        _ -> ok
    end,
    ets:delete(?GR_CHANNEL_TABLE),
    ok.


%% ====================================================================
%% Internal functions
%% ====================================================================
