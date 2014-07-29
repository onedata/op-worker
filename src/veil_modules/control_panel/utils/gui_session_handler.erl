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
%% A session logic module (implementing session_logic_behaviour) must
%% be specified in application's env (key: session_logic_module) for this module to work.
%% @end
%% ===================================================================

-module(gui_session_handler).
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

%% n2o session_handler API
-export([init/2, finish/2, get_value/2, set_value/2, clear/0]).
%% Other functions
-export([create/0, get_session_logic_module/0, clear_expired_sessions/0]).

% Session cookie id
-define(cookie_name, <<"session_id">>).
% Value of cookie when there is no session
-define(no_session_cookie, <<"no_session">>).
% ETS name for cookies
-define(ets_name, cookies).

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
    {Path, _} = cowboy_req:path(Ctx#context.req),

    Module = get_session_logic_module(),

    {Megaseconds, Seconds, _} = now(),
    Till = Megaseconds * 1000000 + Seconds + Module:get_cookie_ttl(),

    SessionID = case Module:lookup_session(Cookie) of
                    undefined ->
                        put(?session_valid, false),
                        case Path of
                            <<"/ws/", _/binary>> ->
                                % This is a websocket connection, and no valid session cookie
                                % was sent; don't generate a new session
                                ?no_session_cookie;
                            _ ->
                                % Creates a new session and allows storing data,
                                % but if create/0 is not called in the scope of this request,
                                % the session is discarded.
                                NewSessionID = random_id(),
                                Module:save_session(NewSessionID, [], Till),
                                NewSessionID
                        end;
                    Props ->
                        put(?session_valid, true),
                        % Refreshes the expiration time of current session
                        Module:save_session(Cookie, Props, Till),
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
    Module = get_session_logic_module(),
    SessionID = Ctx#context.session,
    NewReq = case get(?session_valid) of
                 true ->
                     % Session is valid, set session_id cookie
                     Options = [
                         {path, <<"/">>},
                         {max_age, Module:get_cookie_ttl()},
                         {secure, true},
                         {http_only, true}
                     ],
                     cowboy_req:set_resp_cookie(?cookie_name, SessionID, Options, Ctx#context.req);
                 false ->
                     % Session is not valid, discard current session and set "no_session" cookie value
                     % as well as set max_age to 0, which should delete the cookie on client's side.
                     Module:delete_session(SessionID),
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
%% memory, e. g. via wf:session or wf:user. Associates a Key, Value pair with the
%% session.
%% @end
-spec set_value(Key :: term(), Value :: term()) -> Result :: term().
%% ====================================================================
set_value(Key, Value) ->
    Module = get_session_logic_module(),
    SessionID = ?CTX#context.session,
    Props = Module:lookup_session(SessionID),
    Module:save_session(SessionID, [{Key, Value} | proplists:delete(Key, Props)], undefined),
    Value.


%% get_value/2
%% ====================================================================
%% @doc n2o session_handler callback, called when data is retrieved from session
%% memory, e. g. via wf:session or wf:user. Returns a Value, associated
%% with given Key in session memory, or default.
%% @end
-spec get_value(Key :: term(), DefaultValue :: term()) -> Result :: term().
%% ====================================================================
get_value(Key, DefaultValue) ->
    try
        Module = get_session_logic_module(),
        Props = Module:lookup_session(?CTX#context.session),
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
    Module = get_session_logic_module(),
    put(?session_valid, false),
    Module:delete_session(?CTX#context.session),
    ok.


%% get_session_logic_module/0
%% ====================================================================
%% @doc Retrieves session_logic module from env.
%% @end
-spec get_session_logic_module() -> atom() | no_return().
%% ====================================================================
get_session_logic_module() ->
    case application:get_env(veil_cluster_node, session_logic_module) of
        {ok, Module} ->
            Module;
        _ ->
            throw("No session logic module specified in env")
    end.


%% clear_expired_sessions/0
%% ====================================================================
%% @doc Deletes all sessions that have expired. Every session is saved
%% with a ValidTill arg, that marks a point in time when it expires (in secs since epoch).
%% It has to be periodically called as it is NOT performed automatically.
%% @end
-spec clear_expired_sessions() -> ok.
%% ====================================================================
clear_expired_sessions() ->
    Module = get_session_logic_module(),
    Module:clear_expired_sessions().


%% ====================================================================
%% Internal functions
%% ====================================================================

%% random_id/0
%% ====================================================================
%% @doc Generates a random, 44 chars long, base64 encoded session id.
%% @end
-spec random_id() -> binary().
%% ====================================================================
random_id() ->
    base64:encode(<<(erlang:md5(term_to_binary(now())))/binary, (erlang:md5(term_to_binary(make_ref())))/binary>>).