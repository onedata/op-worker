%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This behaviour specifies an API for session logic - a module,
%%% that is capable of persisting GUI sessions (either in ETS, DB or anything else).
%%% Such module will be called from gui_session_handler.
%%% @end
%%%-------------------------------------------------------------------
-module(gui_session_plugin_behaviour).
-author("Lukasz Opiola").


%%--------------------------------------------------------------------
%% @doc
%% Initializes the session_logic module. Any setup such as ets creation
%% should be performed in this function.
%% @end
%%--------------------------------------------------------------------
-callback init() -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Performs any cleanup, such as deleting the previously created ets tables.
%% @end
%%--------------------------------------------------------------------
-callback cleanup() -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Saves session data under SessionID key (Props), the entry is valid up
%% to given moment (Till). If Till arg is undefined, the one currently
%% associated with SessionID will be used. If there is no record of session
%% with id SessionID and Till is unspecified, exception will be thrown.
%% Till is expressed in number of seconds since epoch.
%% @end
%%--------------------------------------------------------------------
-callback save_session(SessionID :: binary(), Props :: [tuple()],
    ValidTill :: integer() | undefined) -> ok | no_return().

%%--------------------------------------------------------------------
%% @doc
%% Lookups a session by given SessionID key. On success, returns a proplist -
%% session data, or undefined if given session does not exist.
%% NOTE! If SessionID exists, but has expired, it should be automatically
%% removed and undefined should be returned.
%% @end
%%--------------------------------------------------------------------
-callback lookup_session(SessionID :: binary()) ->
    Props :: [tuple()] | undefined.

%%--------------------------------------------------------------------
%% @doc
%% Deletes a session by SessionID key.
%% @end
%%--------------------------------------------------------------------
-callback delete_session(SessionID :: binary()) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Deletes all sessions that have expired. Every session is saved
%% with a ValidTill arg, that marks a point in time when it expires
%% (in secs since epoch). The clearing should be performed based on this.
%% Should return number of deleted session tokens.
%% @end
%%--------------------------------------------------------------------
-callback clear_expired_sessions() -> non_neg_integer().

%%--------------------------------------------------------------------
%% @doc
%% Returns cookies time to live in seconds. This is a callback so
%% every project using ctool can have its own configuration.
%% @end
%%--------------------------------------------------------------------
-callback get_cookie_ttl() -> integer() | no_return().