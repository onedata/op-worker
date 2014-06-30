%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements session_logic_behaviour and exports an
%% API for persisting GUI sessions.
%% @end
%% ===================================================================

-module(session_logic).
-behaviour(session_logic_behaviour).
-include("veil_modules/control_panel/common.hrl").
-include_lib("ctool/include/logging.hrl").

%% session_logic_behaviour API
-export([init/0, cleanup/0]).
-export([save_session/3, lookup_session/1, delete_session/1, clear_expired_sessions/0]).
-export([get_cookie_ttl/0]).

% ETS name for cookies
-define(SESSION_ETS, cookies).

%% ====================================================================
%% API functions
%% ====================================================================

%% init/0
%% ====================================================================
%% @doc Initializes the session_logic module. Any setup such as ets creation
%% should be performed in this function.
%% @end
-spec init() -> ok.
%% ====================================================================
init() ->
    % Ets table needed for session storing.
    ets:new(?SESSION_ETS, [named_table, public, bag, {read_concurrency, true}]),
    ok.


%% cleanup/0
%% ====================================================================
%% @doc Performs any cleanup, such as deleting the previously created ets tables.
%% @end
-spec cleanup() -> ok.
%% ====================================================================
cleanup() ->
    ets:delete(?SESSION_ETS),
    ok.


%% save_session/3
%% ====================================================================
%% @doc Saves session data under SessionID key (Props), the entry is valid up to given moment (Till).
%% If Till arg is undefined, the one currently associated with SessionID will be used.
%% If there is no record of session with id SessionID and Till is unspecified, exception will be thrown.
%% Till is expressed in number of seconds since epoch.
%% @end
-spec save_session(SessionID :: binary(), Props :: [tuple()], ValidTill :: integer() | undefined) -> ok | no_return().
%% ====================================================================
save_session(SessionID, Props, TillArg) ->
    Till = case TillArg of
               undefined ->
                   case ets:lookup(?SESSION_ETS, SessionID) of
                       [{SessionID, _, CurrentTill}] ->
                           CurrentTill;
                       _ ->
                           throw("session expiration not specified")
                   end;
               _ ->
                   TillArg
           end,

    delete_session(SessionID),
    ets:insert(?SESSION_ETS, {SessionID, Props, Till}),
    ok.


%% lookup_session/1
%% ====================================================================
%% @doc Lookups a session by given SessionID key. On success, returns a proplist -
%% session data, or undefined if given session does not exist.
%% NOTE! If SessionID exists, but has expired, it should be automatically
%% removed and undefined should be returned.
%% @end
-spec lookup_session(SessionID :: binary()) -> Props :: [tuple()] | undefined.
%% ====================================================================
lookup_session(SessionID) ->
    case SessionID of
        undefined ->
            undefined;
        _ ->
            case ets:lookup(?SESSION_ETS, SessionID) of
                [{SessionID, Props, Till}] ->
                    % Check if the session isn't outdated
                    {Megaseconds, Seconds, _} = now(),
                    Now = Megaseconds * 1000000 + Seconds,
                    case Till > Now of
                        true ->
                            Props;
                        false ->
                            delete_session(SessionID),
                            undefined
                    end;
                _ ->
                    undefined
            end
    end.


%% delete_session/1
%% ====================================================================
%% @doc Deletes a session by SessionID key.
%% @end
-spec delete_session(SessionID :: binary()) -> ok.
%% ====================================================================
delete_session(SessionID) ->
    case SessionID of
        undefined ->
            ok;
        _ ->
            ets:delete(?SESSION_ETS, SessionID),
            ok
    end.


%% clear_expired_sessions/0
%% ====================================================================
%% @doc Deletes all sessions that have expired. Every session is saved
%% with a ValidTill arg, that marks a point in time when it expires (in secs since epoch).
%% The clearing should be performed based on this.
%% @end
-spec clear_expired_sessions() -> ok.
%% ====================================================================
clear_expired_sessions() ->
    {Megaseconds, Seconds, _} = now(),
    Now = Megaseconds * 1000000 + Seconds,
    ExpiredSessions = ets:select(?SESSION_ETS, [{{'$1', '$2', '$3'}, [{'<', '$3', Now}], ['$_']}]),
    lists:foreach(
        fun({SessionID, _, _}) ->
            delete_session(SessionID)
        end, ExpiredSessions),
    length(ExpiredSessions).


%% get_cookie_ttl/0
%% ====================================================================
%% @doc Returns cookies time to live in seconds.
%% @end
-spec get_cookie_ttl() -> integer() | no_return().
%% ====================================================================
get_cookie_ttl() ->
    case application:get_env(veil_cluster_node, control_panel_sessions_cookie_ttl) of
        {ok, Val} when is_integer(Val)->
            Val;
        _ ->
            throw("No cookie TTL specified in env")
    end.