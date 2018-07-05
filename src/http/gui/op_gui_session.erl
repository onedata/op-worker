%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles GUI session manipulation.
%%% @end
%%%-------------------------------------------------------------------
-module(op_gui_session).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("gui/include/gui_session.hrl").

-export([log_in/3, log_out/1, get/1, get_user_id/1]).
-export([put_value/3, get_value/2, get_value/3, update_value/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Logs given user in, creating new session and setting the session cookie in
%% http response.
%% @end
%%--------------------------------------------------------------------
-spec log_in(session:identity(), session:auth(), cowboy_req:req()) ->
    cowboy_req:req().
log_in(Identity, Auth, Req) ->
    {ok, SessionId} = session_manager:create_gui_session(Identity, Auth),
    put_value(SessionId, gui_session_user_id, Identity#user_identity.user_id),
    set_session_cookie(SessionId, session:session_ttl(), Req).


%%--------------------------------------------------------------------
%% @doc
%% Logs out the user that performed given request, deletes the session and
%% clears the session cookie.
%% @end
%%--------------------------------------------------------------------
-spec log_out(cowboy_req:req()) -> cowboy_req:req().
log_out(Req) ->
    case get_session_cookie(Req) of
        undefined ->
            Req;
        SessionId ->
            session_manager:remove_session(SessionId),
            unset_session_cookie(Req)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves the session by given session id or http request.
%% @end
%%--------------------------------------------------------------------
-spec get(session:id() | cowboy_req:req()) -> {ok, session:doc()} | {error, term()}.
get(SessionId) when is_binary(SessionId) ->
    session:get(SessionId);
get(Req) ->
    case get_session_cookie(Req) of
        undefined ->
            {error, not_found};
        SessionId ->
            ?MODULE:get(SessionId)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns the user id of currents session owner, or error if there is no
%% valid session.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id(cowboy_req:req()) -> {ok, od_user:id()} |  {error, term()}.
get_user_id(Req) ->
    case ?MODULE:get(Req) of
        {error, not_found} ->
            {error, not_logged_in};
        {ok, Session} ->
            session:get_user_id(Session)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Saves a value in session memory.
%% @end
%%--------------------------------------------------------------------
-spec put_value(session:id(), Key :: term(), Value :: term()) -> ok.
put_value(SessionId, Key, Value) ->
    MemoryUpdateFun = fun(Memory) ->
        maps:put(Key, Value, Memory)
    end,
    update_session(SessionId, MemoryUpdateFun).


%%--------------------------------------------------------------------
%% @doc
%% @equiv get_value(Key, undefined).
%% @end
%%--------------------------------------------------------------------
-spec get_value(session:id(), Key :: term()) -> Value :: term().
get_value(SessionId, Key) ->
    get_value(SessionId, Key, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a value from session memory.
%% @end
%%--------------------------------------------------------------------
-spec get_value(session:id(), Key :: term(), Default :: term()) ->
    {ok, Value :: term()} | {error, term()}.
get_value(SessionId, Key, Default) ->
    case session:get(SessionId) of
        {ok, #document{value = #session{memory = Memory}}} ->
            {ok, maps:get(Key, Memory, Default)};
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Atomically updates a value in session memory.
%% @end
%%--------------------------------------------------------------------
-spec update_value(session:id(), Key :: term(), UpdateFun :: fun((term()) -> term()),
    InitialValue :: term()) -> ok | {error, term()}.
update_value(SessionId, Key, UpdateFun, InitialValue) ->
    MemoryUpdateFun = fun(Memory) ->
        OldValue = maps:get(Key, Memory, InitialValue),
        maps:put(Key, UpdateFun(OldValue), Memory)
    end,
    update_session(SessionId, MemoryUpdateFun).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% {@link gui_session_plugin_behaviour} callback update_session/2.
%% @end
%%--------------------------------------------------------------------
-spec update_session(SessId :: binary(),
    MemoryUpdateFun :: fun((maps:map()) -> maps:map())) ->
    ok | {error, term()}.
update_session(SessionId, MemoryUpdateFun) ->
    SessionUpdateFun = fun(#session{memory = OldMemory} = Session) ->
        {ok, Session#session{memory = MemoryUpdateFun(OldMemory)}}
    end,
    case session:update(SessionId, SessionUpdateFun) of
        {ok, _} -> ok;
        {error, Error} -> {error, Error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns the value of session cookie sent by the client, or undefined.
%% @end
%%--------------------------------------------------------------------
-spec get_session_cookie(cowboy_req:req()) -> undefined | binary().
get_session_cookie(Req) ->
    Cookies = cowboy_req:parse_cookies(Req),
    case proplists:get_value(?SESSION_COOKIE_KEY, Cookies, ?NO_SESSION) of
        ?NO_SESSION -> undefined;
        Cookie -> Cookie
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sets the value of session cookie in the response to client request.
%% @end
%%--------------------------------------------------------------------
-spec set_session_cookie(SessionId :: binary(), TTL :: integer(), cowboy_req:req()) ->
    cowboy_req:req().
set_session_cookie(SessionId, TTL, Req) ->
    Options = #{
        path => <<"/">>,
        max_age => TTL,
        secure => true,
        http_only => true
    },
    cowboy_req:set_resp_cookie(?SESSION_COOKIE_KEY, SessionId, Req, Options).


%%--------------------------------------------------------------------
%% @doc
%% Clears the value of session cookie in the response to client request
%% (effectively clearing his session).
%% @end
%%--------------------------------------------------------------------
-spec unset_session_cookie(cowboy_req:req()) -> cowboy_req:req().
unset_session_cookie(Req) ->
    set_session_cookie(?NO_SESSION, 0, Req).