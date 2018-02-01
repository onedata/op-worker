%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for maintaining graph sync connection to Onezone
%%% and handling incoming push messages Whenever the connection dies, this
%%% gen_server is killed and new one is instantiated by gs_worker.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_client_worker).
-author("Lukasz Opiola").

-behaviour(gen_server).

-include("graph_sync/provider_graph_sync.hrl").
-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("http/http_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/api_errors.hrl").


%% @formatter:off
-type client() :: session:id() | session:auth().
-type create_result() :: {ok, Data :: term()} |
                         {ok, {gs_protocol:gri(), doc()}} |
                         gs_protocol:error().
-type get_result() :: {ok, doc()} | gs_protocol:error().
-type update_result() :: ok | gs_protocol:error().
-type delete_result() :: ok | gs_protocol:error().
-type result() :: create_result() |
                  get_result() |
                  update_result() |
                  delete_result().
%% @formatter:on

-export_type([client/0, result/0]).

-record(state, {
    client_ref = undefined :: undefined | gs_client:client_ref()
}).
-type state() :: #state{}.
-type connection_ref() :: pid().
-type doc() :: datastore:document().

%% API
-export([start_link/0]).
-export([request/1, request/2]).
-export([invalidate_cache/1, invalidate_cache/2]).
-export([process_push_message/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts gs_client_worker instance and registers it globally.
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | gs_protocol:error().
start_link() ->
    gen_server2:start_link(?MODULE, [], []).


%%--------------------------------------------------------------------
%% @doc
%% Handles a Graph Sync request by contacting Onezone or serving the response
%% from cache if possible. Uses default client (this provider).
%% @end
%%--------------------------------------------------------------------
-spec request(gs_protocol:rpc_req() | gs_protocol:graph_req()) -> result().
request(Req) ->
    request(?ROOT_SESS_ID, Req).


%%--------------------------------------------------------------------
%% @doc
%% Handles a Graph Sync request by contacting Onezone or serving the response
%% from cache if possible.
%% @end
%%--------------------------------------------------------------------
-spec request(client(), gs_protocol:rpc_req() | gs_protocol:graph_req()) ->
    result().
request(Client, Req) ->
    try
        case get_connection_pid() of
            undefined ->
                ?ERROR_NO_CONNECTION_TO_OZ;
            Pid ->
                do_request(Pid, Client, Req)
        end
    catch
        throw:Error = {error, _} ->
            Error;
        Type:Reason ->
            ?error_stacktrace("Unexpected error while processing GS request - ~p:~p", [
                Type, Reason
            ]),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


%%--------------------------------------------------------------------
%% @doc
%% Invalidates local cache of given entity instance, represented by GRI.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_cache(gs_protocol:gri()) -> ok.
invalidate_cache(#gri{type = Type, id = Id, aspect = instance}) ->
    invalidate_cache(Type, Id).


%%--------------------------------------------------------------------
%% @doc
%% Invalidates local cache of given entity instance, represented by type and id.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_cache(gs_protocol:entity_type(), gs_protocol:entity_id()) -> ok.
invalidate_cache(Type, Id) ->
    Type:delete(Id).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes the worker.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: []) ->
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([]) ->
    process_flag(trap_exit, true),
    case start_gs_connection() of
        {ok, _ClientRef, #gs_resp_handshake{identity = nobody}} ->
            {stop, normal};
        {ok, ClientRef, #gs_resp_handshake{identity = {provider, _}}} ->
            yes = global:register_name(?GS_CLIENT_WORKER_GLOBAL_NAME, self()),
            ?info("Started connection to Onezone: ~p", [ClientRef]),
            oneprovider:on_connection_to_oz(),
            {ok, #state{client_ref = ClientRef}};
        {error, _} = Error ->
            ?warning("Cannot start connection to Onezone: ~p", [Error]),
            {stop, normal}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call(#gs_req{}, _From, #state{client_ref = undefined} = State) ->
    {reply, ?ERROR_NO_CONNECTION_TO_OZ, State};

handle_call(#gs_req{} = GsReq, _From, #state{client_ref = ClientRef} = State) ->
    Result = gs_client:sync_request(ClientRef, GsReq),
    {reply, Result, State};

handle_call({terminate, Reason}, _From, State) ->
    {stop, Reason, ok, State};

handle_call(Request, _From, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast(Request, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info({'EXIT', Pid, Reason}, #state{client_ref = Pid} = State) ->
    ?warning("Connection to Onezone lost, reason: ~p", [Reason]),
    {stop, normal, State};
handle_info(Info, #state{} = State) ->
    ?log_bad_request(Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: state()) -> term().
terminate(Reason, #state{} = State) ->
    ?log_terminate(Reason, State).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()} | {error, term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


-spec process_push_message(gs_protocol:push()) -> any().
process_push_message(#gs_push_nosub{gri = GRI}) ->
    ?debug("Subscription cancelled: ~s", [gs_protocol:gri_to_string(GRI)]),
    invalidate_cache(GRI);

process_push_message(#gs_push_error{error = Error}) ->
    ?error("Unexpected graph sync error: ~p", [Error]);

process_push_message(#gs_push_graph{gri = GRI, change_type = deleted}) ->
    invalidate_cache(GRI),
    ?debug("Entity deleted in OZ: ~s", [gs_protocol:gri_to_string(GRI)]);

process_push_message(#gs_push_graph{gri = GRI, data = Data, change_type = updated}) ->
    Doc = gs_client_translator:translate(GRI, Data),
    cache_record(get_connection_pid(), GRI, Doc).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec start_gs_connection() ->
    {ok, gs_client:client_ref(), gs_protocol:handshake_resp()} | gs_protocol:error().
start_gs_connection() ->
    try
        provider_logic:assert_zone_compatibility(),

        Port = ?GS_CHANNEL_PORT,
        Address = "wss://" ++ oneprovider:get_oz_domain() ++
            ":" ++ integer_to_list(Port) ++ ?GS_CHANNEL_PATH,

        CaCerts = oneprovider:get_ca_certs(),
        Opts = [{cacerts, CaCerts}],
        {ok, ProviderMacaroon} = provider_auth:get_auth_macaroon(),

        gs_client:start_link(
            Address, {macaroon, ProviderMacaroon}, [?GS_PROTOCOL_VERSION],
            fun process_push_message/1, Opts
        )
    catch
        Type:Reason ->
            ?error_stacktrace("Cannot start gs connection due to ~p:~p", [
                Type, Reason
            ]),
            {error, Reason}
    end.


-spec do_request(connection_ref(), client(),
    gs_protocol:rpc_req() | gs_protocol:graph_req()) -> result().
do_request(ConnRef, Client, #gs_req_rpc{} = RpcReq) ->
    case call_onezone(ConnRef, Client, RpcReq) of
        {ok, #gs_resp_rpc{result = Res}} ->
            {ok, Res};
        {error, _} = Error ->
            Error
    end;
do_request(ConnRef, Client, #gs_req_graph{operation = get} = GraphReq) ->
    case maybe_serve_from_cache(ConnRef, Client, GraphReq) of
        {error, _} = Err1 ->
            Err1;
        {true, Doc} ->
            {ok, Doc};
        false ->
            case call_onezone(ConnRef, Client, GraphReq) of
                {error, _} = Err2 ->
                    Err2;
                {ok, #gs_resp_graph{result = Res}} ->
                    GRIStr = maps:get(<<"gri">>, Res),
                    NewGRI = gs_protocol:string_to_gri(GRIStr),
                    Doc = gs_client_translator:translate(NewGRI, maps:remove(<<"gri">>, Res)),
                    cache_record(ConnRef, NewGRI, Doc),
                    {ok, Doc}
            end
    end;
do_request(ConnRef, Client, #gs_req_graph{operation = create} = GraphReq) ->
    case call_onezone(ConnRef, Client, GraphReq) of
        {error, _} = Error ->
            Error;
        {ok, #gs_resp_graph{result = Res}} ->
            case Res of
                undefined ->
                    ok;
                #{<<"data">> := IntData} = Map when map_size(Map) =:= 1 ->
                    {ok, IntData};
                #{<<"gri">> := GRIStr} = Map when map_size(Map) > 1 ->
                    NewGRI = gs_protocol:string_to_gri(GRIStr),
                    Doc = gs_client_translator:translate(NewGRI, maps:remove(<<"gri">>, Res)),
                    cache_record(ConnRef, NewGRI, Doc),
                    {ok, {NewGRI, Doc}}
            end
    end;
% covers 'delete' and 'update' operations
do_request(ConnRef, Client, #gs_req_graph{} = GraphReq) ->
    case call_onezone(ConnRef, Client, GraphReq) of
        {error, _} = Error ->
            Error;
        {ok, #gs_resp_graph{}} ->
            ok
    end.


-spec call_onezone(connection_ref(), client(),
    gs_protocol:rpc_req() | gs_protocol:graph_req() | gs_protocol:unsub_req()) ->
    {ok, gs_protocol:rpc_resp() | gs_protocol:graph_resp() | gs_protocol:unsub_resp()} |
    gs_protocol:error().
call_onezone(ConnRef, Client, Request) ->
    try
        SubType = case Request of
            #gs_req_graph{} -> graph;
            #gs_req_rpc{} -> rpc;
            #gs_req_unsub{} -> unsub
        end,
        GsReq = #gs_req{
            subtype = SubType,
            auth_override = resolve_authorization(Client),
            request = Request
        },
        gen_server2:call(ConnRef, GsReq, ?GS_REQUEST_TIMEOUT)
    catch
        exit:{timeout, _} -> ?ERROR_NO_CONNECTION_TO_OZ;
        exit:{normal, _} -> ?ERROR_NO_CONNECTION_TO_OZ;
        Type:Reason ->
            ?error_stacktrace("Unexpected error during call to gs_client_worker - ~p:~p", [
                Type, Reason
            ]),
            throw(?ERROR_INTERNAL_SERVER_ERROR)
    end.


-spec maybe_serve_from_cache(connection_ref(), client(), gs_protocol:graph_req()) ->
    {true, doc()} | false | gs_protocol:error().
maybe_serve_from_cache(ConnRef, Client, #gs_req_graph{gri = #gri{aspect = instance} = GRI, auth_hint = AuthHint}) ->
    case get_from_cache(GRI) of
        false ->
            false;
        {true, CachedDoc} ->
            #{connection_ref := CachedConnRef, scope := CachedScope} = get_cache_state(CachedDoc),
            #gri{scope = Scope} = GRI,
            case CachedConnRef =/= ConnRef orelse is_scope_lower(CachedScope, Scope) of
                true ->
                    % There was a reconnect since last update or cached scope is
                    % lower than requested -> invalidate cache
                    false;
                false ->
                    case is_authorized(Client, AuthHint, GRI, CachedDoc) of
                        unknown ->
                            false;
                        false ->
                            ?ERROR_FORBIDDEN;
                        true ->
                            Result = case Scope of
                                CachedScope ->
                                    CachedDoc;
                                _ ->
                                    gs_client_translator:apply_scope_mask(CachedDoc, Scope)
                            end,
                            {true, Result}
                    end
            end
    end;
maybe_serve_from_cache(_, _, _) ->
    false.


-spec cache_record(connection_ref(), gs_protocol:gri(), doc()) ->
    ok.
cache_record(ConnRef, GRI = #gri{type = Type, aspect = instance, scope = Scope}, Doc) ->
    ShouldUpdateCache = case get_from_cache(GRI) of
        false ->
            true;
        {true, CachedDoc} ->
            % In case of higher scope, unsubscribe for the lower and update cache.
            % In case of the same scope, just overwrite the cache.
            % In case of lower scope, do not update cache.
            #{scope := OldScope} = get_cache_state(CachedDoc),
            case is_scope_lower(OldScope, Scope) of
                true ->
                    call_onezone(ConnRef, ?ROOT_SESS_ID, #gs_req_unsub{
                        gri = GRI#gri{scope = OldScope}
                    }),
                    true;
                false ->
                    not is_scope_lower(Scope, OldScope)
            end
    end,
    case ShouldUpdateCache of
        false ->
            ok;
        true ->
            CacheState = #{
                scope => Scope,
                connection_ref => ConnRef
            },
            Type:save(put_cache_state(CacheState, Doc)),
            ?debug("Cached ~s", [gs_protocol:gri_to_string(GRI)]),
            ok
    end;
cache_record(_ConnRef, _GRI, _Doc) ->
    % Only 'instance' aspects are cached by provider.
    ok.


-spec get_from_cache(gs_protocol:gri()) -> {true, doc()} | false.
get_from_cache(#gri{type = Type, id = Id}) ->
    case Type:get(Id) of
        {ok, Doc} ->
            {true, Doc};
        _ ->
            false
    end.


-spec get_connection_pid() -> undefined | pid().
get_connection_pid() ->
    global:whereis_name(?GS_CLIENT_WORKER_GLOBAL_NAME).


-spec resolve_authorization(client()) -> gs_protocol:auth_override().
resolve_authorization(?ROOT_SESS_ID) ->
    undefined;

resolve_authorization(?GUEST_SESS_ID) ->
    undefined;

resolve_authorization(SessionId) when is_binary(SessionId) ->
    {ok, Auth} = session:get_auth(SessionId),
    resolve_authorization(Auth);

resolve_authorization(#macaroon_auth{} = Auth) ->
    #macaroon_auth{
        macaroon = MacaroonBin, disch_macaroons = DischargeMacaroonsBin
    } = Auth,
    {ok, Macaroon} = onedata_macaroons:deserialize(MacaroonBin),
    BoundMacaroons = lists:map(
        fun(DischargeMacaroonBin) ->
            {ok, DM} = onedata_macaroons:deserialize(DischargeMacaroonBin),
            BDM = macaroon:prepare_for_request(Macaroon, DM),
            {ok, SerializedBDM} = onedata_macaroons:serialize(BDM),
            SerializedBDM
        end, DischargeMacaroonsBin),
    {macaroon, MacaroonBin, BoundMacaroons};

resolve_authorization(#token_auth{token = Token}) ->
    {token, Token};

resolve_authorization(#basic_auth{credentials = UserPasswdB64}) ->
    {basic, UserPasswdB64}.


-spec put_cache_state(cache_state(), doc()) -> doc().
put_cache_state(CacheState, Doc = #document{value = User = #od_user{}}) ->
    Doc#document{value = User#od_user{cache_state = CacheState}};
put_cache_state(CacheState, Doc = #document{value = Group = #od_group{}}) ->
    Doc#document{value = Group#od_group{cache_state = CacheState}};
put_cache_state(CacheState, Doc = #document{value = Space = #od_space{}}) ->
    Doc#document{value = Space#od_space{cache_state = CacheState}};
put_cache_state(CacheState, Doc = #document{value = Share = #od_share{}}) ->
    Doc#document{value = Share#od_share{cache_state = CacheState}};
put_cache_state(CacheState, Doc = #document{value = Provider = #od_provider{}}) ->
    Doc#document{value = Provider#od_provider{cache_state = CacheState}};
put_cache_state(CacheState, Doc = #document{value = HService = #od_handle_service{}}) ->
    Doc#document{value = HService#od_handle_service{cache_state = CacheState}};
put_cache_state(CacheState, Doc = #document{value = Handle = #od_handle{}}) ->
    Doc#document{value = Handle#od_handle{cache_state = CacheState}}.


-spec get_cache_state(doc()) -> cache_state().
get_cache_state(#document{value = #od_user{cache_state = CacheState}}) ->
    CacheState;
get_cache_state(#document{value = #od_group{cache_state = CacheState}}) ->
    CacheState;
get_cache_state(#document{value = #od_space{cache_state = CacheState}}) ->
    CacheState;
get_cache_state(#document{value = #od_share{cache_state = CacheState}}) ->
    CacheState;
get_cache_state(#document{value = #od_provider{cache_state = CacheState}}) ->
    CacheState;
get_cache_state(#document{value = #od_handle_service{cache_state = CacheState}}) ->
    CacheState;
get_cache_state(#document{value = #od_handle{cache_state = CacheState}}) ->
    CacheState.


-spec is_scope_lower(gs_protocol:scope(), gs_protocol:scope()) -> boolean().
is_scope_lower(public, public) -> false;
is_scope_lower(public, _) -> true;

is_scope_lower(shared, public) -> false;
is_scope_lower(shared, shared) -> false;
is_scope_lower(shared, _) -> true;

is_scope_lower(protected, private) -> true;
is_scope_lower(protected, _) -> false;

is_scope_lower(private, _) -> false.


-spec is_authorized(client(), gs_protocol:auth_hint(), gs_protocol:gri(), doc()) ->
    boolean() | unknown.
is_authorized(?ROOT_SESS_ID, _, #gri{type = od_user, scope = private}, _) ->
    false;
is_authorized(?ROOT_SESS_ID, _, #gri{type = od_user, scope = protected}, _) ->
    true;
is_authorized(?ROOT_SESS_ID, _, #gri{type = od_user, scope = shared}, _) ->
    true;

is_authorized(?ROOT_SESS_ID, _, #gri{type = od_group, scope = private}, _) ->
    false;
is_authorized(?ROOT_SESS_ID, _, #gri{type = od_group, scope = protected}, _) ->
    true;
is_authorized(?ROOT_SESS_ID, _, #gri{type = od_group, scope = shared}, _) ->
    true;

is_authorized(?ROOT_SESS_ID, _, #gri{type = od_space, scope = private}, _) ->
    true;
is_authorized(?ROOT_SESS_ID, _, #gri{type = od_space, scope = protected}, _) ->
    true;

% Provider can access shares of spaces that it supports
is_authorized(?ROOT_SESS_ID, _, #gri{type = od_share, scope = private}, CachedDoc) ->
    provider_logic:supports_space(
        ?ROOT_SESS_ID,
        oneprovider:get_id_or_undefined(),
        CachedDoc#document.value#od_share.space
    );

is_authorized(?ROOT_SESS_ID, _, #gri{type = od_provider, scope = private}, _) ->
    true;
is_authorized(?ROOT_SESS_ID, _, #gri{type = od_provider, scope = protected}, _) ->
    true;

is_authorized(?ROOT_SESS_ID, _, #gri{type = od_handle_service, scope = private}, _) ->
    false;

is_authorized(?ROOT_SESS_ID, _, #gri{type = od_handle, scope = private}, _) ->
    false;

is_authorized(?GUEST_SESS_ID, _, #gri{type = od_space}, _) ->
    % Guest session is a virtual session fully managed by provider, and it needs
    % access to space info to serve public data such as shares.
    true;

is_authorized(_, _, #gri{type = od_share, scope = public}, _) ->
    true;

is_authorized(_, _, #gri{type = od_handle, scope = public}, _) ->
    true;

is_authorized(SessionId, AuthHint, GRI, CachedDoc) when is_binary(SessionId) ->
    {ok, UserId} = session:get_user_id(SessionId),
    is_user_authorized(UserId, SessionId, AuthHint, GRI, CachedDoc);

is_authorized(_, _, _, _) ->
    unknown.


-spec is_user_authorized(od_user:id(), client(), gs_protocol:auth_hint(), gs_protocol:gri(), doc()) ->
    boolean() | unknown.
is_user_authorized(UserId, _, _, #gri{type = od_user, id = UserId, scope = private}, _) ->
    true;
is_user_authorized(_UserId, _, _, #gri{type = od_user, id = _OtherUserId, scope = private}, _) ->
    false;
is_user_authorized(UserId, _, _, #gri{type = od_user, id = UserId, scope = protected}, _) ->
    true;
is_user_authorized(_UserId, _, _, #gri{type = od_user, id = _OtherUserId, scope = protected}, _) ->
    false;
is_user_authorized(UserId, _, _, #gri{type = od_user, id = UserId, scope = shared}, _) ->
    true;
is_user_authorized(ClientUserId, Client, AuthHint, #gri{type = od_user, id = TargetUserId, scope = shared}, _) ->
    case AuthHint of
        ?THROUGH_GROUP(GroupId) ->
            group_logic:can_view_user_through_group(Client, GroupId, ClientUserId, TargetUserId);
        ?THROUGH_SPACE(SpaceId) ->
            space_logic:can_view_user_through_space(Client, SpaceId, ClientUserId, TargetUserId);
        _ ->
            false
    end;

is_user_authorized(UserId, _, _, #gri{type = od_group, scope = private}, CachedDoc) ->
    group_logic:has_eff_privilege(CachedDoc, UserId, ?GROUP_VIEW);
is_user_authorized(UserId, SessionId, _, #gri{type = od_group, scope = protected}, CachedDoc) ->
    user_logic:has_eff_group(SessionId, UserId, CachedDoc#document.key);
is_user_authorized(ClientUserId, Client, AuthHint, GRI = #gri{type = od_group, id = ChildId, scope = shared}, CachedDoc) ->
    case AuthHint of
        ?THROUGH_GROUP(GroupId) ->
            group_logic:can_view_child_through_group(Client, GroupId, ClientUserId, ChildId);
        ?THROUGH_SPACE(SpaceId) ->
            space_logic:can_view_group_through_space(Client, SpaceId, ClientUserId, ChildId);
        _ ->
            is_user_authorized(ClientUserId, Client, AuthHint, GRI#gri{scope = protected}, CachedDoc)
    end;

is_user_authorized(UserId, _, _, #gri{type = od_space, scope = private}, CachedDoc) ->
    space_logic:has_eff_user(CachedDoc, UserId);
is_user_authorized(UserId, SessionId, _, #gri{type = od_space, scope = protected}, CachedDoc) ->
    user_logic:has_eff_space(SessionId, UserId, CachedDoc#document.key);

is_user_authorized(UserId, Client, _, #gri{type = od_share, scope = private}, CachedDoc) ->
    space_logic:has_eff_user(Client, CachedDoc#document.value#od_share.space, UserId);

is_user_authorized(_UserId, _, _, #gri{type = od_provider, scope = private}, _) ->
    false;

is_user_authorized(UserId, _, _, #gri{type = od_provider, scope = protected}, CachedDoc) ->
    case get_cache_state(CachedDoc) of
        #{scope := private} ->
            provider_logic:has_eff_user(CachedDoc, UserId);
        _ ->
            unknown
    end;

is_user_authorized(UserId, _, _, #gri{type = od_handle_service, scope = private}, CachedDoc) ->
    handle_service_logic:has_eff_user(CachedDoc, UserId);

is_user_authorized(UserId, _, _, #gri{type = od_handle, scope = private}, CachedDoc) ->
    handle_logic:has_eff_user(CachedDoc, UserId).

