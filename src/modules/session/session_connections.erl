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
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    get_random_connection/1, get_connections/1,
    set_async_request_manager/2, get_async_req_manager/1
]).
-export([
    get_new_record_and_update_fun/5,
    add_connection/2, remove_connection/2
]).
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
    {ok, Con :: pid()} | {error, Reason :: no_connections | term()}.
get_random_connection(SessId) ->
    case get_connections(SessId) of
        {ok, []} ->
            {error, no_connections};
        {ok, Cons} ->
            {ok, utils:random_element(Cons)};
        {error, _Reason} = Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns connections associated with session.
%% @end
%%--------------------------------------------------------------------
-spec get_connections(session:id()) -> {ok, [Conn :: pid()]} | {error, term()}.
get_connections(SessId) ->
    case get_proxy_session(SessId) of
        {ok, #session{connections = Cons}} ->
            {ok, Cons};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets async_request_manager property of session.
%% @end
%%--------------------------------------------------------------------
-spec set_async_request_manager(session:id(), ConnManager :: pid()) ->
    ok | datastore:update_error().
set_async_request_manager(SessionId, AsyncReqManager) ->
    ?extract_ok(session:update(SessionId, fun(Session = #session{}) ->
        {ok, Session#session{async_request_manager = AsyncReqManager}}
    end)).

%%--------------------------------------------------------------------
%% @doc
%% Returns connection manager for session.
%% @end
%%--------------------------------------------------------------------
-spec get_async_req_manager(session:id()) ->
    {ok, Con :: pid()} | {error, Reason :: term()}.
get_async_req_manager(SessId) ->
    case get_proxy_session(SessId) of
        {ok, #session{async_request_manager = undefined}} ->
            {error, no_async_req_manager};
        {ok, #session{async_request_manager = AsyncReqManager}} ->
            {ok, AsyncReqManager};
        Error ->
            Error
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


-spec add_connection(session:id(), Con :: pid()) ->
    ok | {error, term()}.
add_connection(SessId, Con) ->
    ?extract_ok(session:update(SessId, fun(#session{connections = Cons} = Sess) ->
        {ok, Sess#session{connections = [Con | Cons]}}
    end)).


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

            session_manager:reuse_or_create_session(SessId, provider_outgoing, #user_identity{
                provider_id = session_utils:session_id_to_provider_id(SessId)
            }, undefined, []);
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

%%--------------------------------------------------------------------
%% @doc
%% Returns effective session, that is session, which is not proxied.
%% @end
%%--------------------------------------------------------------------
-spec get_proxy_session(session:id()) -> {ok, #session{}} | {error, term()}.
get_proxy_session(SessId) ->
    case session:get(SessId) of
        {ok, #document{value = #session{proxy_via = ProxyVia}}} when is_binary(ProxyVia) ->
            ProxyViaSession = session_utils:get_provider_session_id(outgoing, ProxyVia),
            get_proxy_session(ProxyViaSession);
        {ok, #document{value = Sess}} ->
            {ok, Sess};
        Error ->
            Error
    end.
