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
-export([
    get_random_connection/1, get_connections/1,
    get_connection_manager/1,
    get_random_conn_and_conn_manager/1
]).
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
    case get_connections(SessId) of
        {ok, []} -> {error, empty_connection_pool};
        {ok, Cons} -> {ok, utils:random_element(Cons)};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns connections associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_connections(session:id()) -> {ok, [Conn :: pid()]} | {error, term()}.
get_connections(SessId) ->
    case get_effective_session(SessId) of
        {ok, #session{connections = Cons}} ->
            {ok, Cons};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns connection manager for session.
%% @end
%%--------------------------------------------------------------------
-spec get_connection_manager(session:id()) ->
    {ok, Con :: pid()} | {error, Reason :: term()}.
get_connection_manager(SessId) ->
    case get_effective_session(SessId) of
        {ok, #session{connection_manager = undefined}} ->
            {error, no_connection_manager};
        {ok, #session{connection_manager = ConnManager}} ->
            {ok, ConnManager};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns connection manager for session and random connection.
%% @end
%%--------------------------------------------------------------------
-spec get_random_conn_and_conn_manager(session:id()) ->
    {ok, Con :: pid(), ConnManager :: pid()} | {error, Reason :: term()}.
get_random_conn_and_conn_manager(SessId) ->
    case get_effective_session(SessId) of
        {ok, #session{connections = []}} ->
            {error, empty_connection_pool};
        {ok, #session{connection_manager = undefined}} ->
            {error, no_connection_manager};
        {ok, #session{connections = Cons, connection_manager = ConnManager}} ->
            {ok, utils:random_element(Cons), ConnManager};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns effective session, that is session, which is not proxied.
%% @end
%%--------------------------------------------------------------------
get_effective_session(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{proxy_via = ProxyVia}}} when is_binary(ProxyVia) ->
            ProxyViaSession = session_utils:get_provider_session_id(outgoing, ProxyVia),
            get_effective_session(ProxyViaSession);
        {ok, #document{value = Sess}} ->
            {ok, Sess};
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
    case get_random_connection(SessId) of
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
                        case get_random_connection(SessId) of
                            {error, _} ->
                                connection:connect_to_provider(
                                    ProviderId, SessId, Domain, Host, Port,
                                    ranch_ssl, timer:seconds(5)
                                );
                            _ ->
                                ensure_connected(SessId)
                        end
                    end)
                end, Hosts),
            ok;
        {ok, Pid} ->
            case utils:process_info(Pid, initial_call) of
                undefined ->
                    ok = remove_connection(SessId, Pid),
                    ensure_connected(SessId);
                _ ->
                    ok
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
