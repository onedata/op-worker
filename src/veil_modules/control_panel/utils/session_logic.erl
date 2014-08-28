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
-include("veil_modules/dao/dao_cookies.hrl").
-include_lib("ctool/include/logging.hrl").

%% session_logic_behaviour API
-export([init/0, cleanup/0]).
-export([save_session/3, lookup_session/1, delete_session/1, clear_expired_sessions/0, get_cookie_ttl/0]).

% ETS name for cookies
-define(SESSION_ETS, cookies).

% Max number of old session cookies retrieved from view at once
-define(cookie_lookup_limit, 100).

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
    ok.


%% cleanup/0
%% ====================================================================
%% @doc Performs any cleanup, such as deleting the previously created ets tables.
%% @end
-spec cleanup() -> ok.
%% ====================================================================
cleanup() ->
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
    ExistingCookieDoc = case dao_lib:apply(dao_cookies, get_cookie, [SessionID], 1) of
                            {ok, Doc} -> Doc;
                            _ -> undefined
                        end,

    CookieDoc = case TillArg of
                    undefined ->
                        case ExistingCookieDoc of
                            undefined ->
                                % New SessionID, but no expiration time specified
                                throw("session expiration not specified");
                            #veil_document{record = #session_cookie{} = CookieInfo} ->
                                % Existing SessionID, use the same record
                                % Props are updated, but expiration time stays the same
                                ExistingCookieDoc#veil_document{
                                    record = CookieInfo#session_cookie{
                                        session_memory = Props
                                    }
                                }
                        end;
                    _ ->
                        case ExistingCookieDoc of
                            undefined ->
                                % New SessionID, expiration time was specified
                                #veil_document{
                                    uuid = SessionID,
                                    record = #session_cookie{
                                        session_memory = Props,
                                        valid_till = TillArg
                                    }
                                };
                            #veil_document{record = CookieInfo} = CDoc ->
                                % Existing SessionID, expiration time was specified and replaces the old one
                                CDoc#veil_document{
                                    record = CookieInfo#session_cookie{
                                        session_memory = Props,
                                        valid_till = TillArg
                                    }
                                }
                        end
                end,

    dao_lib:apply(dao_cookies, save_cookie, [CookieDoc], 1),
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
            case dao_lib:apply(dao_cookies, get_cookie, [SessionID], 1) of
                {ok, #veil_document{record = #session_cookie{valid_till = Till, session_memory = Props}}} ->
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
            dao_lib:apply(dao_cookies, remove_cookie, [SessionID], 1)
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
    clear_expired_sessions(0).

clear_expired_sessions(TotalDeleted) ->
    case dao_lib:apply(dao_cookies, list_expired_cookies, [?cookie_lookup_limit, 0], 1) of
        {ok, UUIDList} ->
            NumberOfCookies = lists:foldl(
                fun(UUID, Counter) ->
                    dao_lib:apply(dao_cookies, remove_cookie, [UUID], 1),
                    Counter + 1
                end, 0, UUIDList),
            case NumberOfCookies of
                ?cookie_lookup_limit ->
                    clear_expired_sessions(TotalDeleted + NumberOfCookies);
                _ ->
                    TotalDeleted + NumberOfCookies
            end;
        Other ->
            ?error("Cannot clear expired sessions - view query returned ~p", [Other]),
            TotalDeleted
    end.


%% get_cookie_ttl/0
%% ====================================================================
%% @doc Returns cookies time to live in seconds.
%% @end
-spec get_cookie_ttl() -> integer() | no_return().
%% ====================================================================
get_cookie_ttl() ->
    case application:get_env(veil_cluster_node, control_panel_sessions_cookie_ttl) of
        {ok, Val} when is_integer(Val) ->
            Val;
        _ ->
            throw("No cookie TTL specified in env")
    end.



