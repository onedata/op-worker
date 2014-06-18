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
        {save_session, 3},
        {lookup_session, 1},
        {delete_session, 1},
        {clear_expired_sessions, 0}
    ];

behaviour_info(_Other) ->
    undefined.

%% ====================================================================
%% Callbacks descriptions
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
%% Function: clear_expired_sessions() -> ok.
%% Desription: Deletes all sessions that have expired.
%% ====================================================================