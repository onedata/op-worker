%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This behaviour specifies an API for session logic - a module,
%% that is capable of persisting GUI sessions (either in ETS, DB or anything else).
%% Such module will be called from gui_session_handler.
%% @end
%% ===================================================================

-module(session_logic_behaviour).

-export([behaviour_info/1]).

%% behaviour_info/1
%% ====================================================================
%% @doc Defines the behaviour (lists the callbacks and their arity)
-spec behaviour_info(Arg) -> Result when
    Arg :: callbacks | Other,
    Result :: [Fun_def]
    | undefined,
    Fun_def :: tuple(),
    Other :: any().
%% ====================================================================
behaviour_info(callbacks) ->
    [
        {init, 0},
        {cleanup, 0},
        {save_session, 3},
        {lookup_session, 1},
        {delete_session, 1},
        {clear_expired_sessions, 0},
        {get_cookie_ttl, 0}
    ];

behaviour_info(_Other) ->
    undefined.


%% ====================================================================
%% Callbacks descriptions
%% ====================================================================

%% init/0
%% ====================================================================
%% Function: init() -> ok.
%% Desription: Initializes the session_logic module. Any setup such as ets creation
%% should be performed in this function.
%% ====================================================================


%% cleanup/0
%% ====================================================================
%% Function: cleanup() -> ok.
%% Desription: Performs any cleanup, such as deleting the previously created ets tables.
%% ====================================================================


%% save_session/3
%% ====================================================================
%% Function: save_session(SessionID :: binary(), Props :: [tuple()], ValidTill :: integer() | undefined) -> ok | no_return().
%% Desription: Saves session data under SessionID key (Props), the entry is valid up to given moment (Till).
%% If Till arg is undefined, the one currently associated with SessionID will be used.
%% If there is no record of session with id SessionID and Till is unspecified, exception will be thrown.
%% Till is expressed in number of seconds since epoch.
%% ====================================================================


%% lookup_session/1
%% ====================================================================
%% Function: lookup_session(SessionID :: binary()) -> Props :: [tuple()] | undefined.
%% Desription: Lookups a session by given SessionID key. On success, returns a proplist -
%% session data, or undefined if given session does not exist.
%% NOTE! If SessionID exists, but has expired, it should be automatically
%% removed and undefined should be returned.
%% ====================================================================


%% delete_session/1
%% ====================================================================
%% Function: delete_session(SessionID :: binary()) -> ok.
%% Desription: Deletes a session by SessionID key.
%% ====================================================================


%% clear_expired_sessions/0
%% ====================================================================
%% Function: clear_expired_sessions() -> integer().
%% Desription: Deletes all sessions that have expired. Every session is saved
%% with a ValidTill arg, that marks a point in time when it expires (in secs since epoch).
%% The clearing should be performed based on this.
%% Should return number of deleted session tokens.
%% ====================================================================


%% get_cookie_ttl/0
%% ====================================================================
%% Function: get_cookie_ttl() -> integer() | no_return().
%% Desription: Returns cookies time to live in seconds. This is a callback so
%% every project using ctool can have its own configuration.
%% ====================================================================