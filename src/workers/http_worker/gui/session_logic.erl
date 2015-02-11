%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module implements session_logic_behaviour and exports an
%%% API for persisting GUI sessions.
%%% @end
%%%--------------------------------------------------------------------
-module(session_logic).
-author("Lukasz Opiola").

-behaviour(session_logic_behaviour).
-include_lib("ctool/include/logging.hrl").

%% session_logic_behaviour callbacks
-export([init/0, cleanup/0]).
-export([save_session/3, lookup_session/1, delete_session/1, clear_expired_sessions/0, get_cookie_ttl/0]).

% Max number of old session cookies retrieved from view at once
-define(cookie_lookup_limit, 100).

%%%===================================================================
%%% session_logic_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes the session_logic module. Any setup such as ets creation
%% should be performed in this function.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Performs any cleanup, such as deleting the previously created ets tables.
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> ok.
cleanup() ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Saves session data under SessionID key (Props), the entry is valid up to given moment (Till).
%% If Till arg is undefined, the one currently associated with SessionID will be used.
%% If there is no record of session with id SessionID and Till is unspecified, exception will be thrown.
%% Till is expressed in number of seconds since epoch.
%% @end
%%--------------------------------------------------------------------
-spec save_session(SessionID :: binary(), Props :: [tuple()], ValidTill :: integer() | undefined) -> ok | no_return().
save_session(_SessionID, _Props, _TillArg) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Lookups a session by given SessionID key. On success, returns a proplist -
%% session data, or undefined if given session does not exist.
%% NOTE! If SessionID exists, but has expired, it should be automatically
%% removed and undefined should be returned.
%% @end
%%--------------------------------------------------------------------
-spec lookup_session(SessionID :: binary()) -> Props :: [tuple()] | undefined.
lookup_session(_SessionID) ->
    [].

%%--------------------------------------------------------------------
%% @doc
%% Deletes a session by SessionID key.
%% @end
%%--------------------------------------------------------------------
-spec delete_session(SessionID :: binary()) -> ok.
delete_session(_SessionID) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Deletes all sessions that have expired. Every session is saved
%% with a ValidTill arg, that marks a point in time when it expires (in secs since epoch).
%% The clearing should be performed based on this.
%% @end
%%--------------------------------------------------------------------
-spec clear_expired_sessions() -> non_neg_integer().
clear_expired_sessions() ->
    0.

%%--------------------------------------------------------------------
%% @doc Returns cookies time to live in seconds.
%% @end
%%--------------------------------------------------------------------
-spec get_cookie_ttl() -> integer() | no_return().
get_cookie_ttl() ->
    0.
