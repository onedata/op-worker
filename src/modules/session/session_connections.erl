%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module that handles session connections management.
%%% @end
%%%-------------------------------------------------------------------
-module(session_connections).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([register/2, deregister/2, list/1]).
-export([
    set_async_request_manager/2, get_async_req_manager/1,
    ensure_connected/1
]).

-type error() :: {error, Reason :: term()}.


%%%===================================================================
%%% API
%%%===================================================================


-spec register(session:id(), Conn :: pid()) -> ok | error().
register(SessId, Conn) ->
    Diff = fun(#session{connections = Cons} = Sess) ->
        {ok, Sess#session{connections = [Conn | Cons]}}
    end,
    ?extract_ok(session:update(SessId, Diff)).


-spec deregister(session:id(), Conn :: pid()) -> ok | error().
deregister(SessId, Conn) ->
    Diff = fun(#session{connections = Cons} = Sess) ->
        NewCons = lists:filter(fun(C) -> C =/= Conn end, Cons),
        {ok, Sess#session{connections = NewCons}}
    end,
    ?extract_ok(session:update(SessId, Diff)).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of effective connections for specified session.
%% @end
%%--------------------------------------------------------------------
-spec list(session:id()) -> {ok, [Conn :: pid()]} | error().
list(SessId) ->
    case get_proxy_session(SessId) of
        {ok, #session{connections = Cons}} ->
            {ok, Cons};
        Error ->
            Error
    end.


-spec set_async_request_manager(session:id(), pid()) -> ok | error().
set_async_request_manager(SessionId, AsyncReqManager) ->
    ?extract_ok(session:update(SessionId, fun(#session{} = Session) ->
        {ok, Session#session{async_request_manager = AsyncReqManager}}
    end)).


%%--------------------------------------------------------------------
%% @doc
%% Returns effective async request manager for specified session.
%% @end
%%--------------------------------------------------------------------
-spec get_async_req_manager(session:id()) -> {ok, pid()} | error().
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
%% Ensures that there is at least one outgoing connection for given session.
%% @end
%%--------------------------------------------------------------------
-spec ensure_connected(session:id()) -> ok | no_return().
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

            session_manager:reuse_or_create_outgoing_provider_session(
                SessId, #user_identity{provider_id = ProviderId}
            );
        {ok, Pid} ->
            case utils:process_info(Pid, initial_call) of
                undefined ->
                    ok = deregister(SessId, Pid),
                    ensure_connected(SessId);
                _ ->
                    ok
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_random_connection(session:id()) -> {ok, Conn :: pid()} | error().
get_random_connection(SessId) ->
    case list(SessId) of
        {ok, []} ->
            {error, no_connections};
        {ok, Cons} ->
            {ok, utils:random_element(Cons)};
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns effective session, that is session, which is not proxied.
%% @end
%%--------------------------------------------------------------------
-spec get_proxy_session(session:id()) -> {ok, #session{}} | error().
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
