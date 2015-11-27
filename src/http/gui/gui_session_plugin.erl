%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C): 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc: This module implements session_logic_behaviour and exports an
%%% API for persisting GUI sessions.
%%% @end
%%%-------------------------------------------------------------------

-module(gui_session_plugin).
-behaviour(gui_session_plugin_behaviour).


-include("global_definitions.hrl").
-include_lib("gui/include/gui.hrl").
-include_lib("ctool/include/logging.hrl").


%% session_logic_behaviour API
-export([init/0, cleanup/0]).
-export([create_session/2, update_session/3, lookup_session/1, delete_session/1]).
-export([clear_expired_sessions/0, get_cookie_ttl/0]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Initializes the session_logic module. Any setup such as ets creation
%% should be performed in this function.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ok.


%%--------------------------------------------------------------------
%% @doc Performs any cleanup, such as deleting the previously created ets tables.
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> ok.
cleanup() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Creates a new session under SessionId key.
%% The session is valid up to given moment (Expires).
%% Expires is expressed in number of seconds since epoch.
%% CustomArgs are the args that are passed to g_session:log_in/1 function,
%% they are application specific arguments that are needed to create a session.
%% @end
%%--------------------------------------------------------------------
-spec create_session(Expires, CustomArgs) ->
    {ok, SessionId} | {error, term()} when
    Expires :: integer(), CustomArgs :: [term()], SessionId :: binary().
create_session(Expires, [#auth{} = Auth]) ->
    case session_manager:create_gui_session(random_id(), Auth, Expires) of
        {ok, SessionId} ->
            {ok, SessionId};
        {error, Error} ->
            {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Saves session data under SessionID key. Updates the session memory,
%% the entry is valid up to given moment (Expires).
%% If there is no record of session
%% with id SessionID, error atom should be returned.
%% Expires is expressed in number of seconds since epoch.
%% @end
%%--------------------------------------------------------------------
-spec update_session(SessionID, Memory, Expires) -> ok | {error, term()}
    when SessionID :: binary(),
    Memory :: [{Key :: binary(), Value :: binary}],
    Expires :: integer().
update_session(SessionId, Expires, Memory) ->
    case session:update(SessionId, #{memory => Memory, expires => Expires}) of
        {ok, _} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Lookups a session by given SessionID key.
%% On success, returns a tuple - expiration time and session memory,
%% or undefined if given session does not exist.
%% @end
%%--------------------------------------------------------------------
-spec lookup_session(SessionId :: binary()) -> {Expires, Memory} | undefined
    when Expires :: integer(), Memory :: [{Key :: binary(), Value :: binary}].
lookup_session(SessionId) ->
    case SessionId of
        undefined ->
            undefined;
        _ ->
            case session:get(SessionId) of
                {ok, #document{value = #session{
                    expires = Expires, memory = Memory}}} ->
                    {Expires, Memory};
                _ ->
                    undefined
            end
    end.


%%--------------------------------------------------------------------
%% @doc Deletes a session by SessionId key.
%% @end
%%--------------------------------------------------------------------
-spec delete_session(SessionID :: binary()) -> ok | {error, term()}.
delete_session(SessionId) ->
    case SessionId of
        undefined ->
            ok;
        _ ->
            case session:delete(SessionId) of
                ok -> ok;
                {error, _} = Error -> Error
            end
    end.


%%--------------------------------------------------------------------
%% @doc Deletes all sessions that have expired. Every session is saved with a
%% Expires arg, that marks when it expires (in secs since epoch).
%% The clearing should be performed based on this.
%% Returns number of expired sessions.
%% @end
%%--------------------------------------------------------------------
-spec clear_expired_sessions() -> integer().
clear_expired_sessions() ->
    {ok, AllSessions} = session:list(),
    _DeletedSessions = lists:foldl(
        fun(#document{key = Key, value = #session{expires = Expires}}, Acc) ->
            case Expires > now_seconds() of
                true ->
                    session:delete(Key),
                    Acc + 1;
                false ->
                    Acc
            end
        end, 0, AllSessions).


%%--------------------------------------------------------------------
%% @doc Returns cookies time to live in seconds.
%% @end
%%--------------------------------------------------------------------
-spec get_cookie_ttl() -> integer() | no_return().
get_cookie_ttl() ->
    case application:get_env(?APP_NAME, gui_cookie_ttl) of
        {ok, Val} when is_integer(Val) ->
            Val;
        _ ->
            throw("No cookie TTL specified in env")
    end.


%%--------------------------------------------------------------------
%% @doc Generates a random, 44 chars long, base64 encoded session id.
%% @end
%%--------------------------------------------------------------------
-spec random_id() -> binary().
random_id() ->
    base64:encode(
        <<(erlang:md5(term_to_binary(now())))/binary,
        (erlang:md5(term_to_binary(make_ref())))/binary>>).


now_seconds() ->
    {Megaseconds, Seconds, _} = now(),
    Megaseconds * 1000000 + Seconds.