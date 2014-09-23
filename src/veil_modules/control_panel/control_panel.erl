%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour callbacks.
%% It is responsible for spawning processes which then process HTTP requests.
%% @end
%% ===================================================================

-module(control_panel).
-behaviour(worker_plugin_behaviour).

-include("veil_modules/control_panel/common.hrl").
-include("veil_modules/dao/dao_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_openid.hrl").

-define(REFRESH_CLIENTS_ETS, refresh_clients_ets).

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1 <br />
%% Sets up cowboy handlers for GUI and REST.
%% @end
-spec init(Args :: term()) -> Result when
    Result :: ok | {error, Error},
    Error :: term().
%% ====================================================================
init(_Args) ->
    % Schedule the clearing of expired sessions - a periodical job
    % This job will run on every instance of control_panel, but since it is rare and lightweight
    % it won't cause performance problems.
    Pid = self(),
    {ok, ClearingInterval} = application:get_env(veil_cluster_node, control_panel_sessions_clearing_period),
    erlang:send_after(ClearingInterval * 1000, Pid, {timer, {asynch, 1, {clear_expired_sessions, Pid}}}),
    ets:new(?REFRESH_CLIENTS_ETS, [named_table, public, bag]),
    ok.


%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1
-spec handle(ProtocolVersion :: term(), Request) -> Result when
    Request :: ping | healthcheck | get_version,
    Result :: ok | {ok, Response} | {error, Error} | pong | Version,
    Response :: term(),
    Version :: term(),
    Error :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(_ProtocolVersion, {spawn_handler, SocketPid}) ->
    Pid = spawn(
        fun() ->
            erlang:monitor(process, SocketPid),
            veil_cowboy_bridge:set_socket_pid(SocketPid),
            veil_cowboy_bridge:request_processing_loop()
        end),
    Pid;

handle(ProtocolVersion, {clear_expired_sessions, Pid}) ->
    NumSessionsCleared = gui_session_handler:clear_expired_sessions(),
    ?info("Expired GUI sessions cleared (~p tokens removed)", [NumSessionsCleared]),
    {ok, ClearingInterval} = application:get_env(veil_cluster_node, control_panel_sessions_clearing_period),
    erlang:send_after(ClearingInterval * 1000, Pid, {timer, {asynch, ProtocolVersion, {clear_expired_sessions, Pid}}}),
    ok;

handle(ProtocolVersion, {request_refresh, UserKey, Consumer}) ->
    try
        #veil_document{uuid = UserId} = UserDoc = user_logic:get_user(UserKey),

        ets:insert(?REFRESH_CLIENTS_ETS, {UserId, Consumer}),
        case ets:lookup(?REFRESH_CLIENTS_ETS, UserId) of %% @todo: race condition
            [{UserId, Consumer}] ->
                #veil_document{record = #user{access_expiration_time = ExpirationTime}} = UserDoc,
                TimeToExpiration = ExpirationTime - vcn_utils:time(),

                case TimeToExpiration < 60 of
                    true  ->
                        handle(ProtocolVersion, {refresh_access, UserId});
                    false ->
                        TimeToRefresh = timer:seconds(trunc(TimeToExpiration * 4 / 5)),
                        erlang:send_after(TimeToRefresh, control_panel, {timer, {asynch, ProtocolVersion, {run_scheduled_refresh, UserId}}}),
                        ok
                end;

            _ ->
                ok
        end
    catch
        Error -> Error
    end;

handle(_ProtocolVersion, {fuse_session_close, UserKey, ConnectionPid}) ->
    #veil_document{uuid = UserId} = user_logic:get_user(UserKey),
    ets:delete_object(?REFRESH_CLIENTS_ETS, {UserId, {fuse, ConnectionPid}}),
    ok;

handle(ProtocolVersion, {run_scheduled_refresh, UserId}) ->
    case ets:member(?REFRESH_CLIENTS_ETS, UserId) of
        true ->
            TimeToExpiration = refresh_access(UserId),
            TimeToRefresh = timer:seconds(trunc(TimeToExpiration * 4 / 5)),
            erlang:send_after(TimeToRefresh, control_panel, {timer, {asynch, ProtocolVersion, {refresh_access, UserId}}}),
            ok;

        false -> ok
    end;


handle(_ProtocolVersion, _Msg) ->
    ok.


%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0 <br />
%% Stops cowboy listener and terminates
%% @end
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
%% ====================================================================
cleanup() ->
    ets:delete(?REFRESH_CLIENTS_ETS),
    ok.


%% refresh_access/2
%% ====================================================================
%% @doc Refresh user's access and schedule a next refresh.
-spec refresh_access(UserDoc :: #veil_document{record :: #user{}}) ->
    {ok, ExpiresIn :: non_neg_integer()} | {error, Reason :: any()}.
%% ====================================================================
refresh_access(#veil_document{uuid = UserId} = UserDoc) ->
    case openid_utils:refresh_access(UserDoc) of
        {ok, ExpiresIn, NewAccessToken} ->
            Consumers = ets:lookup_element(?REFRESH_CLIENTS_ETS, UserId, 2),

            Context = wf_context:init_context([]),

            lists:foreach(fun(Consumer) ->
                case Consumer of
                    {gui_session, GuiSession} ->
                        case dao_lib:apply(dao_cookies, get_cookie, [GuiSession]) of
                            {ok, _} ->
                                wf_context:context(Context#context{session = GuiSession}),
                                vcn_gui_utils:set_access_token(NewAccessToken);

                            _ ->
                                ets:delete_object(?REFRESH_CLIENTS_ETS, {UserId, {gui_session, GuiSession}})
                        end;

                    {fuse, ConnectionPid} ->
                        ConnectionPid ! {new_access_token, NewAccessToken}
                end
            end, Consumers),

            {ok, ExpiresIn};

        Other ->
            Other
    end.
