%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Aug 2015 13:07
%%%-------------------------------------------------------------------
-module(g_session).
-author("lopiola").

-include("gui.hrl").

% Key to process dictionary, which holds information of current
% SessionID in the context.
-define(SESSION_ID_KEY, session_id).
% Key to process dictionary, which holds information if
% the current session is valid (user hasn't logged out etc).
-define(LOGGED_IN_KEY, logged_in).

-export([init/0, finish/0]).
-export([put_value/2, get_value/1]).
-export([log_in/0, log_out/0, is_logged_in/0]).
-export([clear_expired_sessions/0]).

init() ->
    SessionID = g_ctx:get_session_id(),
    case lookup_session(SessionID) of
        undefined ->
            put(?LOGGED_IN_KEY, false),
            put(?SESSION_ID_KEY, ?NO_SESSION_COOKIE);
        Props ->
            put(?LOGGED_IN_KEY, true),
            ok = ?GUI_SESSION_PLUGIN:save_session(SessionID, Props),
            put(?SESSION_ID_KEY, SessionID)
    end,
    ok.


finish() ->
    case is_logged_in() of
        false ->
            % Session is not valid, send no_session cookie
            Options = [
                {path, <<"/">>},
                {max_age, 0},
                {secure, true},
                {http_only, true}
            ],
            g_ctx:set_resp_session_id(?NO_SESSION_COOKIE, Options);
        true ->
            % Session is valid, set previous cookie
            % (or generate a new one if the session is new).
            SessionID = case get(?SESSION_ID_KEY) of
                            ?NO_SESSION_COOKIE ->
                                random_id();
                            OldSessionID ->
                                OldSessionID
                        end,
            Options = [
                {path, <<"/">>},
                {max_age, ?GUI_SESSION_PLUGIN:get_cookie_ttl()},
                {secure, true},
                {http_only, true}
            ],
            g_ctx:set_resp_session_id(SessionID, Options)
    end.


put_value(Key, Value) ->
    SessionID = get(?SESSION_ID_KEY),
    case lookup_session(SessionID) of
        undefined ->
            throw(user_not_logged_in);
        Props ->
            NewProps = [{Key, Value} | proplists:delete(Key, Props)],
            save_session(SessionID, NewProps)
    end.


get_value(Key) ->
    SessionID = get(?SESSION_ID_KEY),
    case lookup_session(SessionID) of
        undefined ->
            throw(user_not_logged_in);
        Props ->
            proplists:get_value(Key, Props, undefined)
    end.


log_in() ->
    case get(?SESSION_ID_KEY) of
        ?NO_SESSION_COOKIE ->
            ok;
        _ ->
            throw(user_already_logged_in)
    end,
    SessionID = random_id(),
    put(?SESSION_ID_KEY, SessionID),
    ok = save_session(SessionID),
    put(?LOGGED_IN_KEY, true).


log_out() ->
    case get(?SESSION_ID_KEY) of
        ?NO_SESSION_COOKIE ->
            throw(user_already_logged_out);
        _ ->
            ok
    end,
    ok = delete_session(get(?SESSION_ID_KEY)),
    put(?LOGGED_IN_KEY, false).


is_logged_in() ->
    get(?LOGGED_IN_KEY) =:= true.



%%--------------------------------------------------------------------
%% @doc Deletes all sessions that have expired. Every session is saved
%% with a ValidTill arg, that marks a point in time when it expires
%% (in secs since epoch).
%% It has to be periodically called as it is NOT performed automatically.
%% @end
%%--------------------------------------------------------------------
-spec clear_expired_sessions() -> ok.
clear_expired_sessions() ->
    ?GUI_SESSION_PLUGIN:clear_expired_sessions().


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Generates a random, 44 chars long, base64 encoded session id.
%% @end
%%--------------------------------------------------------------------
-spec random_id() -> binary().
random_id() ->
    base64:encode(
        <<(erlang:md5(term_to_binary(now())))/binary,
        (erlang:md5(term_to_binary(make_ref())))/binary>>).


save_session(SessionID) ->
    save_session(SessionID, []).

save_session(SessionID, Props) ->
    {Megaseconds, Seconds, _} = now(),
    Till = Megaseconds * 1000000 + Seconds +
        ?GUI_SESSION_PLUGIN:get_cookie_ttl(),
    ?GUI_SESSION_PLUGIN:save_session(SessionID, Props, Till).



%%--------------------------------------------------------------------
%% @doc Calls back to session logic module to lookup a session. Will not make
%% senseless calls, such as those when session cookie yields no session.
%% @end
%%--------------------------------------------------------------------
-spec lookup_session(SessionID :: binary()) ->
    [{Key :: binary(), Val :: binary()}] | undefined.
lookup_session(SessionID) ->
    case SessionID of
        undefined ->
            undefined;
        ?NO_SESSION_COOKIE ->
            undefined;
        _ ->
            ?GUI_SESSION_PLUGIN:lookup_session(SessionID)
    end.


%%--------------------------------------------------------------------
%% @doc Calls back to session logic module to delete a session. Will not make
%% senseless calls, such as those when session cookie yields no session.
%% @end
%%--------------------------------------------------------------------
-spec delete_session(SessionID :: binary()) -> ok.
delete_session(SessionID) ->
    case SessionID of
        undefined ->
            ok;
        ?NO_SESSION_COOKIE ->
            ok;
        _ ->
            ?GUI_SESSION_PLUGIN:delete_session(SessionID)
    end.