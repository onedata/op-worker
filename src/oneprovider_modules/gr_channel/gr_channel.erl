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
    gen_server:call(?Dispatcher_Name, {?GR_CHANNEL_WORKER, ?PROTOCOL_VERSION, connect}).


%% disconnect/0
%% ====================================================================
%% @doc Disconnects from Global Registry.
%% @end
%% ====================================================================
-spec disconnect() -> ok.
%% ====================================================================
disconnect() ->
    gen_server:call(?Dispatcher_Name, {?GR_CHANNEL_WORKER, ?PROTOCOL_VERSION, disconnect}).


%% push/1
%% ====================================================================
%% @doc Pushes message to Global Registry.
%% @end
%% ====================================================================
-spec push(Msg :: term()) -> ok.
%% ====================================================================
push(Msg) ->
    gen_server:call(?Dispatcher_Name, {?GR_CHANNEL_WORKER, ?PROTOCOL_VERSION, {push, Msg}}).


%% ===================================================================
%% worker_plugin_behaviour callbacks
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1.
%% @end
%% ====================================================================
-spec init(Args :: term()) -> #?GR_CHANNEL_STATE{} | {error, Error :: any()}.
%% ====================================================================
init(_) ->
    {ok, Delay} = application:get_env(?APP_Name, gr_channel_next_connection_attempt_delay),
    timer:send_after(timer:seconds(Delay), ?GR_CHANNEL_WORKER, {timer, {asynch, ?PROTOCOL_VERSION, check_registration}}),
    #?GR_CHANNEL_STATE{status = disconnected}.


%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1.
%% @end
%% ====================================================================
-spec handle(ProtocolVersion :: term(), Request) -> Result when
    Request :: ping | healthcheck | get_version |
    {set_state, list(), list()} |
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
    case get_state() of
        #?GR_CHANNEL_STATE{status = connected} ->
            ok;
        _ ->
            {ok, URL} = application:get_env(?APP_Name, global_registry_channel_url),
            {ok, Delay} = application:get_env(?APP_Name, gr_channel_next_connection_attempt_delay),
            Opts = [{keyfile, gr_plugin:get_key_path()}, {certfile, gr_plugin:get_cert_path()}],
            case websocket_client:start_link(URL, gr_channel_handler, [], Opts) of
                {ok, Pid} ->
                    set_state(#?GR_CHANNEL_STATE{status = connecting, pid = Pid}),
                    gen_server:cast(?GR_CHANNEL_WORKER, {link_process, Pid});
                Other ->
                    ?error("Cannot establish connection to Global Registry due to: ~p."
                    " Reconnecting in ~p seconds...", [Other, Delay]),
                    timer:send_after(timer:seconds(Delay), ?GR_CHANNEL_WORKER, {timer, {asynch, ?PROTOCOL_VERSION, connect}})
            end
    end,
    ok;

handle(_ProtocolVersion, disconnect) ->
    State = get_state(),
    case State of
        #?GR_CHANNEL_STATE{status = connected, pid = Pid} ->
            set_state(State#?GR_CHANNEL_STATE{status = disconnecting}),
            Pid ! disconnect;
        #?GR_CHANNEL_STATE{status = connecting, pid = Pid} ->
            set_state(State#?GR_CHANNEL_STATE{status = disconnecting}),
            Pid ! disconnect;
        _ ->
            ok
    end;

handle(_ProtocolVersion, {connected, Pid}) ->
    ?info("Connection to Global Registry established successfully."),
    State = get_state(),
    case State of
        #?GR_CHANNEL_STATE{status = connecting, pid = Pid} ->
            set_state(State#?GR_CHANNEL_STATE{status = connected});
        _ ->
            ok
    end,
    ok;

handle(_ProtocolVersion, {push, Msg}) ->
    case get_state() of
        #?GR_CHANNEL_STATE{status = connected, pid = Pid} -> Pid ! {push, Msg};
        _ -> ok
    end,
    ok;

handle(_ProtocolVersion, check_registration) ->
    case gen_server:call({global, ?CCM}, get_provider_id) of
        {ok, ProviderId} when is_binary(ProviderId) ->
            gen_server:cast(?GR_CHANNEL_WORKER, {asynch, ?PROTOCOL_VERSION, connect});
        _ ->
            ok
    end;

handle(_ProtocolVersion, {'EXIT', Pid, Reason}) ->
    case get_state() of
        #?GR_CHANNEL_STATE{status = disconnecting, pid = Pid} ->
            ?info("Connection to Global Registry closed."),
            set_state(#?GR_CHANNEL_STATE{status = disconnected, pid = undefined});
        #?GR_CHANNEL_STATE{pid = Pid} ->
            ?error("Connection to Global Registry lost due to: ~p. Reconnecting...", [Reason]),
            set_state(#?GR_CHANNEL_STATE{status = disconnected, pid = undefined}),
            gen_server:cast(?GR_CHANNEL_WORKER, {asynch, ?PROTOCOL_VERSION, connect});
        _ ->
            ok
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
    ok.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% set_state/1
%% ====================================================================
%% @doc Sets gr_channel worker state.
%% @end
%% ====================================================================
-spec set_state(State :: #?GR_CHANNEL_STATE{}) -> ok.
%% ====================================================================
set_state(State) ->
    gen_server:call(?GR_CHANNEL_WORKER, {updatePlugInState, State}).


%% get_state/0
%% ====================================================================
%% @doc Gets gr_channel worker state.
%% @end
%% ====================================================================
-spec get_state() -> State :: #?GR_CHANNEL_STATE{}.
%% ====================================================================
get_state() ->
    gen_server:call(?GR_CHANNEL_WORKER, getPlugInState).