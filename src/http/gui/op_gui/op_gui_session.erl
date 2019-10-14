%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles GUI authentication and session manipulation.
%%% @end
%%%-------------------------------------------------------------------
-module(op_gui_session).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("gui/include/gui_session.hrl").

-export([
    authenticate/1,
    initialize/3,
    is_logged_in/0,
    set_session_id/1, get_session_id/0,
    set_user_id/1, get_user_id/0,
    set_requested_host/1, get_requested_host/0
]).
-export([
    put_value/3,
    get_value/2, get_value/3,
    update_value/4
]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Authenticates a client based on cowboy Req object.
%% @end
%%--------------------------------------------------------------------
-spec authenticate(cowboy_req:req()) ->
    {ok, session:identity(), session:auth()} | false | {error, term()}.
authenticate(Req) ->
    case resolve_auth(Req) of
        undefined ->
            false;
        Token ->
            {PeerIp, _} = cowboy_req:peer(Req),
            Auth = #token_auth{token = Token, peer_ip = PeerIp},
            case user_identity:get_or_fetch(Auth) of
                {error, _} ->
                    ?ERROR_UNAUTHORIZED;
                {ok, #document{value = Identity}} ->
                    {ok, Identity, Auth}
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Initializes a GUI session based on identity and auth, attaches
%% session id and corresponding user id to the current process.
%% @end
%%--------------------------------------------------------------------
-spec initialize(session:identity(), session:auth(), Host :: binary()) ->
    session:id().
initialize(?GUEST_IDENTITY, ?GUEST_AUTH, Host) ->
    set_session_id(?GUEST_SESS_ID),
    set_user_id(?GUEST_USER_ID),
    set_requested_host(Host),
    ?GUEST_SESS_ID;
initialize(Identity, Auth, Host) ->
    {ok, SessionId} = session_manager:reuse_or_create_gui_session(Identity, Auth),
    set_session_id(SessionId),
    set_user_id(Identity#user_identity.user_id),
    set_requested_host(Host),
    SessionId.


%%--------------------------------------------------------------------
%% @doc
%% Predicate saying if current process is attached to a session.
%% @end
%%--------------------------------------------------------------------
-spec is_logged_in() -> boolean().
is_logged_in() ->
    get_session_id() /= undefined.


%%--------------------------------------------------------------------
%% @doc
%% Attaches given session id to current process.
%% @end
%%--------------------------------------------------------------------
-spec set_session_id(session:id()) -> ok.
set_session_id(SessionId) ->
    put(op_gui_session_id, SessionId),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Returns the session id which is attached to the current process.
%% @end
%%--------------------------------------------------------------------
-spec get_session_id() -> session:id().
get_session_id() ->
    get(op_gui_session_id).


%%--------------------------------------------------------------------
%% @doc
%% Attaches given user id to current process.
%% @end
%%--------------------------------------------------------------------
-spec set_user_id(od_user:id()) -> ok.
set_user_id(UserId) ->
    put(op_gui_user_id, UserId),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Returns the user id which is attached to the current process.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id() -> od_user:id().
get_user_id() ->
    get(op_gui_user_id).


%%--------------------------------------------------------------------
%% @doc
%% Attaches given host to current process. Host is the server hostname
%% requested by the client (present in the Host header).
%% @end
%%--------------------------------------------------------------------
-spec set_requested_host(binary()) -> ok.
set_requested_host(Host) ->
    put(op_gui_requested_host, Host),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Returns the host which is attached to the current process.
%% @end
%%--------------------------------------------------------------------
-spec get_requested_host() -> binary().
get_requested_host() ->
    get(op_gui_requested_host).


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
%% Updates the memory stored in the session record according to MemoryUpdateFun.
%% @end
%%--------------------------------------------------------------------
-spec update_session(SessId :: binary(),
    MemoryUpdateFun :: fun((map()) -> map())) ->
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
%% @private
%% @doc
%% Resolves authorization carried by given request - by X-Auth-Token header
%% or in "token" query param.
%% @end
%%--------------------------------------------------------------------
-spec resolve_auth(cowboy_req:req()) -> undefined | binary().
resolve_auth(Req) ->
    case cowboy_req:header(?HDR_X_AUTH_TOKEN, Req, undefined) of
        undefined ->
            QueryParams = cowboy_req:parse_qs(Req),
            proplists:get_value(<<"token">>, QueryParams, undefined);
        Token ->
            Token
    end.