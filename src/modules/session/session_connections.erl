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
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([register/2, deregister/2, list/1]).
-export([set_async_request_manager/2, get_async_req_manager/1]).
-export([get_peer_provider_id/1, ensure_connected/1]).

-type error() :: {error, Reason :: term()}.


%%%===================================================================
%%% API
%%%===================================================================


-spec register(session:id(), Conn :: pid()) -> ok | error().
register(SessionId, Conn) ->
    Diff = fun(#session{connections = Cons} = Sess) ->
        {ok, Sess#session{connections = [Conn | Cons]}}
    end,
    ?extract_ok(session:update_doc_and_time(SessionId, Diff)).


-spec deregister(session:id(), Conn :: pid()) -> ok | error().
deregister(SessionId, Conn) ->
    Diff = fun(#session{connections = Cons} = Sess) ->
        NewCons = lists:filter(fun(C) -> C =/= Conn end, Cons),
        {ok, Sess#session{connections = NewCons}}
    end,
    ?extract_ok(session:update_doc_and_time(SessionId, Diff)).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of effective connections for specified session.
%% @end
%%--------------------------------------------------------------------
-spec list(session:id()) -> {ok, [Conn :: pid()]} | error().
list(SessionId) ->
    case session:get(SessionId) of
        {ok, #document{value = #session{status = initializing}}} ->
            {error, uninitialized_session};
        {ok, #document{value = #session{connections = Cons}}} ->
            {ok, Cons};
        Error ->
            Error
    end.


-spec set_async_request_manager(session:id(), pid()) -> {ok, session:doc()} | error().
set_async_request_manager(SessionId, AsyncReqManager) ->
    session:update(SessionId, fun(#session{} = Session) ->
        {ok, Session#session{async_request_manager = AsyncReqManager}}
    end).


%%--------------------------------------------------------------------
%% @doc
%% Returns effective async request manager for specified session.
%% @end
%%--------------------------------------------------------------------
-spec get_async_req_manager(session:id()) -> {ok, pid()} | error().
get_async_req_manager(SessionId) ->
    case session:get(SessionId) of
        {ok, #document{value = #session{async_request_manager = undefined}}} ->
            {error, no_async_req_manager};
        {ok, #document{value = #session{async_request_manager = AsyncReqManager}}} ->
            {ok, AsyncReqManager};
        Error ->
            Error
    end.


-spec get_peer_provider_id(session:id()) -> oneprovider:id().
get_peer_provider_id(SessionId) ->
    session_utils:session_id_to_provider_id(SessionId).


%%--------------------------------------------------------------------
%% @doc
%% Ensures that outgoing session to peer provider is started.
%% @end
%%--------------------------------------------------------------------
-spec ensure_connected(session:id()) -> {ok, session:id()} | error() | no_return().
ensure_connected(SessionId) ->
    ProviderId = get_peer_provider_id(SessionId),

    case oneprovider:is_self(ProviderId) of
        true ->
            ?warning("Provider attempted to connect to itself, "
                     "skipping connection."),
            erlang:error(connection_loop_detected);
        false ->
            ok
    end,

    session_manager:reuse_or_create_outgoing_provider_session(
        SessionId, ?SUB(?ONEPROVIDER, ProviderId)
    ).
