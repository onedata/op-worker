%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(session_connections).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([get_random_connection/1, get_random_connection/2]).
-export([get_connections/1, get_connections/2]).
-export([remove_connection/2]).

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
            ProxyViaSession = session_manager:get_provider_session_id(outgoing, ProxyVia),
            provider_communicator:ensure_connected(ProxyViaSession),
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
                                    SessionWatcher ! {overloaded_connection, Pid},
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