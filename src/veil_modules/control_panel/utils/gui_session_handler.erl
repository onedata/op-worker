%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Custom session handler, confirming to n2o session handler behaviour.
%% Implements safe cookie handling, by setting HttpOnly and Secure flags,
%% as well as ensuring high session id entropy and no session fixation.
%% @end
%% ===================================================================

-module(gui_session_handler).
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

-compile(export_all).

%% n2o session_handler API, with exception of create/0
-export([init/2, finish/2, get_value/2, set_value/2, create/0, clear/0]).

% Session cookie id
-define(cookie_name, <<"session_id">>).
% Value of cookie when there is no session
-define(no_session_cookie, <<"no_session">>).
% ETS name for cookies
-define(ets_name, cookies).
% 1 day TTL
-define(cookie_max_age, 86400).

% Key for process dictionary, holding information if there is a valid session
-define(session_valid, session_valid).


%% ====================================================================
%% API functions
%% ====================================================================

%% init/2
%% ====================================================================
%% @doc n2o session_handler callback, called before processing every request. Retrieves
%% user's session from a cookie or creates a new session upon login.
%% @end
-spec init(State :: term(), Ctx :: #context{}) -> {ok, NewState :: term(), NewCtx :: #context{}}.
%% ====================================================================
init(State, Ctx) ->
    {Cookie, _} = cowboy_req:cookie(?cookie_name, Ctx#context.req),

    {Megaseconds, Seconds, _} = now(),
    Till = Megaseconds * 1000000 + Seconds + ?cookie_max_age,

    SessionID = case lookup_session(Cookie) of
                    undefined ->
                        put(?session_valid, false),
                        % Creates a new session and allows storing data,
                        % but if create/0 is not called in the scope of this request,
                        % the session is discarded.
                        NewSessionID = random_id(),
                        save_session(NewSessionID, [], Till),
                        NewSessionID;
                    Props ->
                        put(?session_valid, true),
                        % Refreshes the expiration time of current session
                        save_session(Cookie, Props, Till),
                        Cookie
                end,
    {ok, State, Ctx#context{session = SessionID}}.


%% finish/2
%% ====================================================================
%% @doc n2o session_handler callback, called after every request. Checks if
%% there is a valid session in current context. Discards the session if not,
%% or sets a session cookie if the session is to persist.
%% @end
-spec finish(State :: term(), Ctx :: #context{}) -> {ok, NewState :: term(), NewCtx :: #context{}}.
%% ====================================================================
finish(_State, Ctx) ->
    SessionID = Ctx#context.session,
    NewReq = case get(?session_valid) of
                 true ->
                     % Session is valid, set session_id cookie
                     Options = [
                         {path, <<"/">>},
                         {max_age, ?cookie_max_age},
                         {secure, true},
                         {http_only, true}
                     ],
                     cowboy_req:set_resp_cookie(?cookie_name, SessionID, Options, Ctx#context.req);
                 false ->
                     % Session is not valid, discard current session and set "no_session" cookie value
                     % as well as set max_age to 0, which should delete the cookie on client's side.
                     delete_session(SessionID),
                     Options = [
                         {path, <<"/">>},
                         {max_age, 0},
                         {secure, true},
                         {http_only, true}
                     ],
                     cowboy_req:set_resp_cookie(?cookie_name, ?no_session_cookie, Options, Ctx#context.req)
             end,
    {ok, [], Ctx#context{req = NewReq}}.


%% set_value/2
%% ====================================================================
%% @doc n2o session_handler callback, called when data is stored in session
%% memory, e. g. via wf:sesion or wf:user. Associates a Key, Value pair with the
%% session.
%% @end
-spec set_value(Key :: term(), Value :: term()) -> Result :: term().
%% ====================================================================
set_value(Key, Value) ->
    SessionID = ?CTX#context.session,
    Props = lookup_session(SessionID),
    save_session(SessionID, [{Key, Value} | proplists:delete(Key, Props)], undefined),
    Value.


%% get_value/2
%% ====================================================================
%% @doc n2o session_handler callback, called when data retrieved from session
%% memory, e. g. via wf:sesion or wf:user. Returns a Value, associated
%% with given Key in session memory, or default.
%% @end
-spec get_value(Key :: term(), DefaultValue :: term()) -> Result :: term().
%% ====================================================================
get_value(Key, DefaultValue) ->
    try
        Props = lookup_session(?CTX#context.session),
        proplists:get_value(Key, Props, DefaultValue)
    catch
        _:_ ->
            DefaultValue
    end.


%% create/0
%% ====================================================================
%% @doc Effectively creates a session - any data stored in the session
%% memory in current request context will be persisted, and a cookie with
%% session id will be sent back to the client.
%% @end
-spec create() -> ok.
%% ====================================================================
create() ->
    put(?session_valid, true),
    ok.


%% clear/0
%% ====================================================================
%% @doc Clears the session - any session data will be discarded, and
%% session cookie will be invalidated.
%% @end
-spec clear() -> ok.
%% ====================================================================
clear() ->
    put(?session_valid, false),
    delete_session(?CTX#context.session),
    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% save_session/3
%% ====================================================================
%% @doc Saves session data under SessionID key (Props), the entry is valid up to given moment (Till).
%% If Till arg is undefined, the one currently associated with SessionID will be used.
%% If there is no record of session with id SessionID and Till is unspecified, exception will be thrown.
%% Till is expressed in number of seconds since epoch.
%% @end
-spec save_session(SessionID :: binary(), Props :: [tuple()] | undefined, ValidTill :: integer() | undefined) -> ok | no_return().
%% ====================================================================
save_session(SessionID, Props, TillArg) ->
    Till = case TillArg of
               undefined ->
                   case ets:lookup(?ets_name, SessionID) of
                       [{SessionID, _, CurrentTill}] ->
                           CurrentTill;
                       _ ->
                           throw("session expiration not specified")
                   end;
               _ ->
                   TillArg
           end,

    delete_session(SessionID),
    ets:insert(?ets_name, {SessionID, Props, Till}),
    ok.


%% lookup_session/1
%% ====================================================================
%% @doc Lookups a session by given SessionID key. On success, returns a proplist -
%% session data, or undefined if given session does not exist.
%% @end
-spec lookup_session(SessionID :: binary()) -> Props :: [tuple()] | undefined.
%% ====================================================================
lookup_session(SessionID) ->
    ?dump({lookup_session, SessionID}),
    case SessionID of
        ?no_session_cookie ->
            undefined;
        undefined ->
            undefined;
        _ ->
            case ets:lookup(?ets_name, SessionID) of
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
        ?no_session_cookie ->
            ok;
        undefined ->
            ok;
        _ ->
            ets:delete(?ets_name, SessionID),
            ok
    end.


%% random_id/0
%% ====================================================================
%% @doc Generates a random, 44 chars long, base64 encoded session id.
%% @end
-spec random_id() -> binary().
%% ====================================================================
random_id() ->
    base64:encode(<<(erlang:md5(term_to_binary(now())))/binary, (erlang:md5(term_to_binary(make_ref())))/binary>>).