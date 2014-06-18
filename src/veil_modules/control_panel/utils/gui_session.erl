%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Custom session handler, confirming to n2o session handler behaviour.
%% @end
%% ===================================================================

-module(gui_session).
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

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




init(State, Ctx) ->
%%     Req = Ctx#context.req,
    {Cookie, _} = cowboy_req:cookie(?cookie_name, Ctx#context.req),

%%     {{D1, D2, D3}, {T1, T2, T3}} = calendar:now_to_datetime(now()),
%%     Till = {{D1, D2, D3 + 1}, {T1, T2, T3}},

    put(?session_valid, false),
    {Path, _} = cowboy_req:path(Ctx#context.req),
    SessionID = case Path of
                    <<"/validate_login">> ->
                        ?dump(validate_login),
                        NewSessionID = random_uuid(),
                        save_session(NewSessionID, []),
                        NewSessionID;
                    _ ->
                        case lookup_session(Cookie) of
                            undefined ->
                                ?no_session_cookie;
                            _ValidSession ->
                                put(?session_valid, true),
                                Cookie
                        end

                end,

    ?dump({init, Cookie, SessionID, Path}),

    {ok, State, Ctx#context{session = SessionID}}.


finish(_State, Ctx) ->
    SessionID = Ctx#context.session,
    ?dump({finish, SessionID}),
    NewReq = case get(?session_valid) of
                 true ->
                     Options = [
                         {path, <<"/">>},
                         {max_age, ?cookie_max_age},
                         {secure, true},
                         {http_only, true}
                     ],
                     cowboy_req:set_resp_cookie(?cookie_name, SessionID, Options, Ctx#context.req);
                 false ->
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




set_value(Key, Value) ->
%%     ?dump({set_value, Key, Value}),
    SessionID = ?CTX#context.session,
    Props = lookup_session(SessionID),
    save_session(SessionID, [{Key, Value} | proplists:delete(Key, Props)]),
    Value.


get_value(Key, DefaultValue) ->
    try
        Props = lookup_session(?CTX#context.session),
        Res = proplists:get_value(Key, Props, DefaultValue),
%%     ?dump({get_value, Key, Res}),
        Res
    catch
        _:_ ->
            DefaultValue
    end.


save_session(SessionID, Props) ->
    ?dump({save_session, SessionID}),
    delete_session(SessionID),
    ets:insert(?ets_name, {SessionID, Props}).


lookup_session(SessionID) ->
    ?dump({lookup_session, SessionID}),
    case SessionID of
        ?no_session_cookie ->
            undefined;
        undefined ->
            undefined;
        _ ->
            case ets:lookup(?ets_name, SessionID) of
                [{SessionID, Props}] ->
                    Props;
                _ ->
                    undefined
            end
    end.


delete_session(SessionID) ->
    case SessionID of
        ?no_session_cookie ->
            false;
        undefined ->
            false;
        _ ->
            ?dump({delete_session, SessionID}),
            ets:delete(?ets_name, SessionID)
    end.


random_uuid() ->
    random:seed(now()),
    integer_to_binary(random:uniform(1000000000) + 1000000000).


create() ->
    ?dump(create),
    put(?session_valid, true).


clear() ->
    ?dump(clear),
    put(?session_valid, false),
    delete_session(?CTX#context.session).