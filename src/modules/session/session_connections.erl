%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Modules that handles session connections management.
%%% @end
%%%-------------------------------------------------------------------
-module(session_connections).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_random_connection/1, get_random_connection/2]).
-export([get_connections/1, get_connections/2]).
-export([get_new_record_and_update_fun/5, remove_connection/2]).
-export([ensure_connected/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns random connection associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_random_connection(session:id()) ->
    {ok, Con :: pid()} | {error, Reason :: empty_connection_pool | term()}.
get_random_connection(SessId) ->
    get_random_connection(SessId, false).

%%--------------------------------------------------------------------
%% @doc
%% Returns random connection associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_random_connection(session:id(), HideOverloaded :: boolean()) ->
    {ok, Con :: pid()} | {error, Reason :: empty_connection_pool | term()}.
get_random_connection(SessId, HideOverloaded) ->
    case get_connections(SessId, HideOverloaded) of
        {ok, []} -> {error, empty_connection_pool};
        {ok, Cons} -> {ok, utils:random_element(Cons)};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns connections associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_connections(session:id()) ->
    {ok, [Comm :: pid()]} | {error, term()}.
get_connections(SessId) ->
    get_connections(SessId, false).

%%--------------------------------------------------------------------
%% @doc
%% Returns connections associated with session. If HideOverloaded is set to true,
%% hides connections that have too long request queue and and removes invalid
%% connections.
%% @end
%%--------------------------------------------------------------------
-spec get_connections(session:id(), HideOverloaded :: boolean()) ->
    {ok, [Comm :: pid()]} | {error, term()}.
get_connections(SessId, HideOverloaded) ->
    case session:get(SessId) of
        {ok, #document{value = #session{proxy_via = ProxyVia}}} when is_binary(ProxyVia) ->
            ProxyViaSession = session_utils:get_provider_session_id(outgoing, ProxyVia),
%%            ensure_connected(ProxyViaSession),
            get_connections(ProxyViaSession, HideOverloaded);
        {ok, #document{value = #session{connections = Cons, watcher = SessionWatcher}}} ->
            case HideOverloaded of
                false ->
                    {ok, Cons};
                true ->
                    NewCons = lists:foldl( %% Foreach connection
                        fun(Pid, AccIn) ->
                            case utils:process_info(Pid, message_queue_len) of
                                undefined ->
                                    %% Connection died, removing from session
                                    ok = session_connections:remove_connection(SessId, Pid),
                                    AccIn;
                                {message_queue_len, QueueLen} when QueueLen > 15 ->
                                    session_watcher:send(SessionWatcher,
                                        {overloaded_connection, Pid}),
                                    AccIn;
                                _ ->
                                    [Pid | AccIn]
                            end
                        end, [], Cons),
                    {ok, NewCons}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns new record with update function.
%% @end
%%--------------------------------------------------------------------
-spec get_new_record_and_update_fun(NewCons :: list(),
    ProxyVia :: session:id() | undefined, SessType :: session:type(),
    Auth :: session:auth() | undefined, Iden :: session:identity()) ->
    {session:record(), UpdateFun :: datastore_doc:diff(session:record())}.
get_new_record_and_update_fun(NewCons, ProxyVia, SessType, Auth, Iden) ->
    Sess = #session{status = active, identity = Iden, auth = Auth,
        connections = NewCons, type = SessType, proxy_via = ProxyVia},
    Diff = fun
        (#session{status = inactive}) ->
            % TODO VFS-5126 - possible race with closing (creation when cleanup
            % is not finished)
            {error, not_found};
        (#session{identity = ValidIden, connections = Cons} = ExistingSess) ->
            case Iden of
                ValidIden ->
                    {ok, ExistingSess#session{
                        connections = NewCons ++ Cons
                    }};
                _ ->
                    {error, {invalid_identity, Iden}}
            end
    end,
    {Sess, Diff}.

%%--------------------------------------------------------------------
%% @doc
%% Removes connection from session and if it was the last connection schedules
%% session removal.
%% @end
%%--------------------------------------------------------------------
-spec remove_connection(session:id(), Con :: pid()) ->
    ok | {error, term()}.
remove_connection(SessId, Con) ->
    Diff = fun(#session{connections = Cons} = Sess) ->
        NewCons = lists:filter(fun(C) -> C =/= Con end, Cons),
        {ok, Sess#session{connections = NewCons}}
    end,
    case session:update(SessId, Diff) of
        {ok, _} -> ok;
        Other -> Other
    end.


%%--------------------------------------------------------------------
%% @doc
%% Ensures that there is at least one outgoing connection for given session.
%% @end
%%--------------------------------------------------------------------
-spec ensure_connected(session:id() | pid()) ->
    ok | no_return().
ensure_connected(Conn) when is_pid(Conn) ->
    ok;
ensure_connected(SessId) ->
    case session_connections:get_random_connection(SessId, true) of
        {error, _} ->
            ProviderId = case session:get(SessId) of
                {ok, #document{value = #session{proxy_via = ProxyVia}}} when is_binary(
                    ProxyVia) ->
                    ProxyVia;
                _ ->
                    session_utils:session_id_to_provider_id(SessId)
            end,

            case oneprovider:get_id() of
                ProviderId ->
                    ?warning("Provider attempted to connect to itself, skipping connection."),
                    erlang:error(connection_loop_detected);
                _ ->
                    ok
            end,

            {ok, Domain} = provider_logic:get_domain(ProviderId),
            Hosts = case provider_logic:resolve_ips(ProviderId) of
                {ok, IPs} -> [list_to_binary(inet:ntoa(IP)) || IP <- IPs];
                _ -> [Domain]
            end,
            lists:foreach(
                fun(Host) ->
                    Port = https_listener:port(),
                    critical_section:run([?MODULE, ProviderId, SessId], fun() ->
                        % check once more to prevent races
                        case session_connections:get_random_connection(SessId, true) of
                            {error, _} ->
                                outgoing_connection:start(ProviderId, SessId,
                                    Domain, Host, Port, ranch_ssl, timer:seconds(5));
                            _ ->
                                ensure_connected(SessId)
                        end
                    end)
                end, Hosts),
            ok;
        {ok, Pid} ->
            case utils:process_info(Pid, initial_call) of
                undefined ->
                    ok = session_connections:remove_connection(SessId, Pid),
                    ensure_connected(SessId);
                _ ->
                    ok
            end
    end.