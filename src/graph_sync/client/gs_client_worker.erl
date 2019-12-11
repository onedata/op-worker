%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
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
-include("http/gui_paths.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/errors.hrl").


%% @formatter:off
-type client() :: session:id() | session:auth().
-type create_result() :: {ok, Data :: term()} |
                         {ok, {gri:gri(), doc()}} |
                         errors:error().
-type get_result() :: {ok, doc()} | errors:error().
-type update_result() :: ok | errors:error().
-type delete_result() :: ok | errors:error().
-type result() :: create_result() |
                  get_result() |
                  update_result() |
                  delete_result().
%% @formatter:on

-export_type([client/0, result/0]).

-record(state, {
    client_ref = undefined :: undefined | gs_client:client_ref(),
    promises = #{} :: #{gs_protocol:message_id() => pid()}
}).
-type state() :: #state{}.
-type connection_ref() :: pid().
-type doc() :: datastore:doc().


%% API
-export([start_link/0]).
-export([force_terminate/0]).
-export([request/1, request/2, request/3]).
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
-spec start_link() -> {ok, pid()} | errors:error().
start_link() ->
    gen_server2:start_link(?MODULE, [], []).


%%--------------------------------------------------------------------
%% @doc
%% Forces termination of gs_client_worker.
%% @end
%%--------------------------------------------------------------------
-spec force_terminate() -> ok.
force_terminate() ->
    case get_connection_pid() of
        Pid when is_pid(Pid) ->
            gen_server2:call(Pid, {terminate, normal}),
            ok;
        _ ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% @equiv request(?ROOT_SESS_ID, Req).
%% @end
%%--------------------------------------------------------------------
-spec request(gs_protocol:rpc_req() | gs_protocol:graph_req()) -> result().
request(Req) ->
    request(?ROOT_SESS_ID, Req).


%%--------------------------------------------------------------------
%% @doc
%% @equiv request(Client, Req, ?GS_REQUEST_TIMEOUT).
%% @end
%%--------------------------------------------------------------------
-spec request(client(), gs_protocol:rpc_req() | gs_protocol:graph_req()) -> result().
request(Client, Req) ->
    request(Client, Req, ?GS_REQUEST_TIMEOUT).


%%--------------------------------------------------------------------
%% @doc
%% Handles a Graph Sync request by contacting Onezone or serving the response
%% from cache if possible.
%% @end
%%--------------------------------------------------------------------
-spec request(client(), gs_protocol:rpc_req() | gs_protocol:graph_req(), timeout()) ->
    result().
request(Client, Req, Timeout) ->
    try
        do_request(Client, Req, Timeout)
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
-spec invalidate_cache(gri:gri()) -> ok.
invalidate_cache(#gri{type = Type, id = Id, aspect = instance}) ->
    invalidate_cache(Type, Id).


%%--------------------------------------------------------------------
%% @doc
%% Invalidates local cache of given entity instance, represented by type and id.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_cache(gs_protocol:entity_type(), gs_protocol:entity_id()) -> ok.
invalidate_cache(Type, Id) ->
    Type:invalidate_cache(Id).


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
        {ok, _ClientRef, #gs_resp_handshake{identity = ?SUB(nobody)}} ->
            {stop, normal};
        {ok, ClientRef, #gs_resp_handshake{identity = ?SUB(?ONEPROVIDER)}} ->
            yes = global:register_name(?GS_CLIENT_WORKER_GLOBAL_NAME, self()),
            ?info("Started connection to Onezone: ~p, running post-init procedures", [
                ClientRef
            ]),
            % Post-init procedures are run in different process to avoid deadlocks.
            spawn(fun() -> try
                oneprovider:on_connect_to_oz()
            catch Type:Message ->
                ?error_stacktrace(
                    "Connection to Onezone lost due to unexpected error in post-init - ~p:~p",
                    [Type, Message]
                ),
                % Kill the connection to Onezone, which will cause a reconnection and retry
                gen_server2:call({global, ?GS_CLIENT_WORKER_GLOBAL_NAME}, {terminate, normal})
            end end),
            {ok, #state{client_ref = ClientRef}};
        {error, unauthorized} ->
            ?info("Unauthorized to start connection to Onezone"),
            oneprovider:on_deregister(),
            {stop, normal};
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
handle_call({async_request, _, _}, _From, #state{client_ref = undefined} = State) ->
    {reply, ?ERROR_NO_CONNECTION_TO_ONEZONE, State};

handle_call({async_request, GsReq, Timeout}, {From, _}, #state{client_ref = ClientRef, promises = Promises} = State) ->
    ReqId = gs_client:async_request(ClientRef, GsReq),
    % Async message triggering a check if the request has timed out
    erlang:send_after(Timeout, self(), {check_timeout, ReqId}),
    {reply, {ok, ReqId}, State#state{
        promises = Promises#{
            ReqId => From
        }
    }};

handle_call({terminate, Reason}, _From, State) ->
    ?warning("Connection to Onezone terminated"),
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
% Received from gs_client as a result of async_request - forwards the response
% to the caller pid.
handle_info({response, ReqId, Response}, #state{promises = Promises} = State) ->
    case maps:take(ReqId, Promises) of
        {Pid, NewPromises} ->
            Pid ! {response, ReqId, Response},
            {noreply, State#state{promises = NewPromises}};
        error ->
            % Possible if 'check_timeout' for the request has fired and
            % ?ERROR_TIMEOUT was sent back to the caller pid, in such case just
            % ignore the result
            {noreply, State}
    end;
% Async check if the request has timed out (there has been no response received)
% in such case, returns ?ERROR_TIMEOUT to the pid waiting for the response.
handle_info({check_timeout, ReqId}, #state{promises = Promises} = State) ->
    case maps:take(ReqId, Promises) of
        {Pid, NewPromises} ->
            Pid ! {response, ReqId, ?ERROR_TIMEOUT},
            {noreply, State#state{promises = NewPromises}};
        error ->
            % There is no promise for the ReqId anymore, which means the request
            % has already been handled - ignore
            {noreply, State}
    end;
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
    ?debug("Subscription cancelled: ~s", [gri:serialize(GRI)]),
    invalidate_cache(GRI);

process_push_message(#gs_push_error{error = Error}) ->
    ?error("Unexpected graph sync error: ~p", [Error]);

process_push_message(#gs_push_graph{gri = GRI, change_type = deleted}) ->
    ProviderId = oneprovider:get_id_or_undefined(),
    case GRI of
        #gri{type = od_provider, id = ProviderId, aspect = instance} ->
            oneprovider:on_deregister();
        #gri{type = od_space, id = SpaceId, aspect = instance} ->
            main_harvesting_stream:space_removed(SpaceId);
        _ ->
            ok
    end,

    invalidate_cache(GRI),
    ?debug("Entity deleted in OZ: ~s", [gri:serialize(GRI)]);

process_push_message(#gs_push_graph{gri = GRI, data = Resource, change_type = updated}) ->
    Revision = maps:get(<<"revision">>, Resource),
    Doc = gs_client_translator:translate(GRI, Resource),
    coalesce_cache(get_connection_pid(), GRI, Doc, Revision).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec start_gs_connection() ->
    {ok, gs_client:client_ref(), gs_protocol:handshake_resp()} | errors:error().
start_gs_connection() ->
    try
        provider_logic:assert_zone_compatibility(),

        Port = ?GS_CHANNEL_PORT,
        Address = str_utils:format("wss://~s:~b~s", [oneprovider:get_oz_domain(), Port, ?GS_CHANNEL_PATH]),
        CaCerts = oneprovider:trusted_ca_certs(),
        Opts = [{cacerts, CaCerts}],
        {ok, AccessToken} = provider_auth:get_access_token(),
        OpWorkerAccessToken = tokens:build_service_access_token(?OP_WORKER, AccessToken),

        gs_client:start_link(
            Address, {token, OpWorkerAccessToken}, [?GS_PROTOCOL_VERSION],
            fun process_push_message/1, Opts
        )
    catch
        Type:Reason ->
            ?error_stacktrace("Cannot start gs connection due to ~p:~p", [
                Type, Reason
            ]),
            {error, Reason}
    end.


-spec do_request(client(), gs_protocol:rpc_req() | gs_protocol:graph_req(), timeout()) ->
    result().
do_request(Client, #gs_req_rpc{} = RpcReq, Timeout) ->
    case call_onezone(Client, RpcReq, Timeout) of
        {ok, #gs_resp_rpc{result = Res}} ->
            {ok, Res};
        {error, _} = Error ->
            Error
    end;
do_request(Client, #gs_req_graph{operation = get} = GraphReq, Timeout) ->
    case maybe_serve_from_cache(Client, GraphReq) of
        {error, _} = Err1 ->
            Err1;
        {true, Doc} ->
            {ok, Doc};
        false ->
            case call_onezone(Client, GraphReq, Timeout) of
                {error, _} = Err2 ->
                    Err2;
                {ok, #gs_resp_graph{data_format = resource, data = Resource}} ->
                    GRIStr = maps:get(<<"gri">>, Resource),
                    Revision = maps:get(<<"revision">>, Resource),
                    NewGRI = gri:deserialize(GRIStr),
                    Doc = gs_client_translator:translate(NewGRI, Resource),
                    case coalesce_cache(get_connection_pid(), NewGRI, Doc, Revision) of
                        {ok, NewestDoc} -> {ok, NewestDoc};
                        % In case a stale record is detected, repeat the request
                        {error, stale_record} -> do_request(Client, GraphReq, Timeout)
                    end
            end
    end;
do_request(Client, #gs_req_graph{operation = create} = GraphReq, Timeout) ->
    case call_onezone(Client, GraphReq, Timeout) of
        {error, _} = Error ->
            Error;
        {ok, GsRespGraph} ->
            case GsRespGraph of
                #gs_resp_graph{data_format = undefined} ->
                    ok;
                #gs_resp_graph{data_format = value, data = Data} ->
                    {ok, Data};
                #gs_resp_graph{data_format = resource, data = #{<<"gri">> := GRIStr} = Resource} ->
                    NewGRI = gri:deserialize(GRIStr),
                    Revision = maps:get(<<"revision">>, Resource),
                    Doc = gs_client_translator:translate(NewGRI, Resource),
                    case coalesce_cache(get_connection_pid(), NewGRI, Doc, Revision) of
                        {ok, NewestDoc} -> {ok, {NewGRI, NewestDoc}};
                        % In case a stale record is detected, repeat the request
                        {error, stale_record} -> do_request(Client, GraphReq, Timeout)
                    end
            end
    end;
% covers 'delete' and 'update' operations
do_request(Client, #gs_req_graph{} = GraphReq, Timeout) ->
    case call_onezone(Client, GraphReq, Timeout) of
        {error, _} = Error ->
            Error;
        {ok, #gs_resp_graph{}} ->
            ok
    end.


-spec call_onezone(client(), gs_protocol:rpc_req() | gs_protocol:graph_req() | gs_protocol:unsub_req(),
    timeout()) -> {ok, gs_protocol:rpc_resp() | gs_protocol:graph_resp() | gs_protocol:unsub_resp()} |
errors:error().
call_onezone(Client, Request, Timeout) ->
    case get_connection_pid() of
        undefined ->
            ?ERROR_NO_CONNECTION_TO_ONEZONE;
        Pid ->
            call_onezone(Pid, Client, Request, Timeout)
    end.


-spec call_onezone(connection_ref(), client(),
    gs_protocol:rpc_req() | gs_protocol:graph_req() | gs_protocol:unsub_req(), timeout()) ->
    {ok, gs_protocol:rpc_resp() | gs_protocol:graph_resp() | gs_protocol:unsub_resp()} |
    errors:error().
call_onezone(ConnRef, Client, Request, Timeout) ->
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
        case gen_server2:call(ConnRef, {async_request, GsReq, Timeout}) of
            {error, _} = Error ->
                Error;
            {ok, ReqId} ->
                receive
                    {response, ReqId, Result} ->
                        Result
                after
                % the gen_server uses Timeout internally, allow some larger margin
                    Timeout + 5000 ->
                        ?ERROR_TIMEOUT
                end
        end
    catch
        exit:{timeout, _} -> ?ERROR_TIMEOUT;
        exit:{normal, _} -> ?ERROR_NO_CONNECTION_TO_ONEZONE;
        throw:{error, _} = Err -> Err;
        Type:Reason ->
            ?error_stacktrace("Unexpected error during call to gs_client_worker - ~p:~p", [
                Type, Reason
            ]),
            throw(?ERROR_INTERNAL_SERVER_ERROR)
    end.


-spec maybe_serve_from_cache(client(), gs_protocol:graph_req()) ->
    {true, doc()} | false | errors:error().
maybe_serve_from_cache(Client, #gs_req_graph{gri = #gri{aspect = instance} = GRI, auth_hint = AuthHint}) ->
    case get_from_cache(GRI) of
        false ->
            false;
        {true, CachedDoc} ->
            #{connection_ref := CachedConnRef, scope := CachedScope} = get_cache_state(CachedDoc),
            #gri{scope = Scope} = GRI,
            ConnRef = get_connection_pid(),
            case (is_pid(ConnRef) andalso ConnRef =/= CachedConnRef) orelse cmp_scope(CachedScope, Scope) == lower of
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
maybe_serve_from_cache(_, _) ->
    false.


-spec coalesce_cache(connection_ref(), gri:gri(), doc(), gs_protocol:revision()) ->
    {ok, doc()} | {error, stale_record}.
coalesce_cache(ConnRef, GRI = #gri{aspect = instance}, Doc = #document{value = Record}, Rev) ->
    #gri{type = Type, id = Id, scope = Scope} = GRI,
    CacheUpdateFun = fun(CachedRecord) ->
        #{scope := CachedScope} = CacheState = get_cache_state(CachedRecord),
        CachedRev = maps:get(revision, CacheState, 0),
        case cmp_scope(Scope, CachedScope) of
            _ when Rev < CachedRev ->
                % In case the fetched revision is lower, return 'stale_record'
                % error, which will cause the request to be repeated
                ?debug("Stale record ~s: received rev. ~B, but rev. ~B is already cached", [gri:serialize(GRI), Rev, CachedRev]),
                {error, stale_record};

            greater when Rev >= CachedRev ->
                % doc with greater scope arrived (revision is not lower than the
                % one in cache), unsubscribe for the lower scope and update the cache
                spawn(fun() ->
                    % spawn an async process as we are within the datastore:update process
                    call_onezone(ConnRef, ?ROOT_SESS_ID, #gs_req_unsub{
                        gri = GRI#gri{scope = CachedScope}
                    }, ?GS_REQUEST_TIMEOUT)
                end),
                ?debug("Cached ~s (rev. ~B)", [gri:serialize(GRI), Rev]),
                {ok, put_cache_state(Record, #{
                    scope => Scope, connection_ref => ConnRef, revision => Rev
                })};

            _ when Rev > CachedRev ->
                % A doc arrived that has a greater revision, overwrite the
                % cache, no matter the scopes
                ?debug("Cached ~s (rev. ~B)", [gri:serialize(GRI), Rev]),
                {ok, put_cache_state(Record, #{
                    scope => Scope, connection_ref => ConnRef, revision => Rev
                })};

            _ when Rev == CachedRev ->
                % Discard updates in case the fetched scope or rev are not
                % greater than those in cache. However, update the connection
                % ref in case it has changed so that the cache can be reused.
                {ok, put_cache_state(CachedRecord, CacheState#{
                    connection_ref => ConnRef
                })}
        end
    end,
    Type:update_cache(Id, CacheUpdateFun, Doc#document{value = put_cache_state(Record, #{
        scope => Scope, connection_ref => ConnRef, revision => Rev
    })});
coalesce_cache(_ConnRef, _GRI, Doc, _Revision) ->
    % Only 'instance' aspects are cached by provider.
    {ok, Doc}.


-spec get_from_cache(gri:gri()) -> {true, doc()} | false.
get_from_cache(#gri{type = Type, id = Id}) ->
    case Type:get_from_cache(Id) of
        {ok, Doc} -> {true, Doc};
        _ -> false
    end.


-spec get_connection_pid() -> undefined | pid().
get_connection_pid() ->
    global:whereis_name(?GS_CLIENT_WORKER_GLOBAL_NAME).


-spec resolve_authorization(client()) -> gs_protocol:auth_override().
resolve_authorization(?ROOT_SESS_ID) ->
    undefined;

resolve_authorization(?GUEST_SESS_ID) ->
    {nobody, undefined};

resolve_authorization(SessionId) when is_binary(SessionId) ->
    {ok, Auth} = session:get_auth(SessionId),
    resolve_authorization(Auth);

resolve_authorization(#token_auth{token = Token, peer_ip = PeerIp}) ->
    {{token, Token}, PeerIp}.


-spec put_cache_state(Record :: tuple(), cache_state()) -> Record :: tuple().
put_cache_state(User = #od_user{}, CacheState) ->
    User#od_user{cache_state = CacheState};
put_cache_state(Group = #od_group{}, CacheState) ->
    Group#od_group{cache_state = CacheState};
put_cache_state(Space = #od_space{}, CacheState) ->
    Space#od_space{cache_state = CacheState};
put_cache_state(Share = #od_share{}, CacheState) ->
    Share#od_share{cache_state = CacheState};
put_cache_state(Provider = #od_provider{}, CacheState) ->
    Provider#od_provider{cache_state = CacheState};
put_cache_state(HService = #od_handle_service{}, CacheState) ->
    HService#od_handle_service{cache_state = CacheState};
put_cache_state(Handle = #od_handle{}, CacheState) ->
    Handle#od_handle{cache_state = CacheState};
put_cache_state(Harvester = #od_harvester{}, CacheState) ->
    Harvester#od_harvester{cache_state = CacheState}.


-spec get_cache_state(Record :: tuple() | doc()) -> cache_state().
get_cache_state(#document{value = Record}) ->
    get_cache_state(Record);
get_cache_state(#od_user{cache_state = CacheState}) ->
    CacheState;
get_cache_state(#od_group{cache_state = CacheState}) ->
    CacheState;
get_cache_state(#od_space{cache_state = CacheState}) ->
    CacheState;
get_cache_state(#od_share{cache_state = CacheState}) ->
    CacheState;
get_cache_state(#od_provider{cache_state = CacheState}) ->
    CacheState;
get_cache_state(#od_handle_service{cache_state = CacheState}) ->
    CacheState;
get_cache_state(#od_handle{cache_state = CacheState}) ->
    CacheState;
get_cache_state(#od_harvester{cache_state = CacheState}) ->
    CacheState.


-spec cmp_scope(gs_protocol:scope(), gs_protocol:scope()) -> lower | same | greater.
cmp_scope(public, public) -> same;
cmp_scope(public, _) -> lower;

cmp_scope(shared, public) -> greater;
cmp_scope(shared, shared) -> same;
cmp_scope(shared, _) -> lower;

cmp_scope(protected, private) -> lower;
cmp_scope(protected, protected) -> same;
cmp_scope(protected, _) -> greater;

cmp_scope(private, private) -> same;
cmp_scope(private, _) -> greater.


-spec is_authorized(client(), gs_protocol:auth_hint(), gri:gri(), doc()) ->
    boolean() | unknown.
is_authorized(?ROOT_SESS_ID, _, #gri{type = od_user, scope = private}, _) ->
    false;
is_authorized(?ROOT_SESS_ID, _, #gri{type = od_user, scope = protected}, _) ->
    true;
is_authorized(?ROOT_SESS_ID, _, #gri{type = od_user, scope = shared}, _) ->
    true;

is_authorized(?ROOT_SESS_ID, _, #gri{type = od_group, scope = shared}, _) ->
    true;

is_authorized(?ROOT_SESS_ID, _, #gri{type = od_space, scope = private}, _) ->
    true;
is_authorized(?ROOT_SESS_ID, _, #gri{type = od_space, scope = protected}, _) ->
    true;

is_authorized(?ROOT_SESS_ID, _, #gri{type = od_harvester, scope = private}, _) ->
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


-spec is_user_authorized(od_user:id(), client(), gs_protocol:auth_hint(), gri:gri(), doc()) ->
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
        ?THROUGH_SPACE(SpaceId) ->
            space_logic:can_view_user_through_space(Client, SpaceId, ClientUserId, TargetUserId);
        _ ->
            false
    end;

is_user_authorized(ClientUserId, Client, AuthHint, #gri{type = od_group, id = GroupId, scope = shared}, _) ->
    case AuthHint of
        ?THROUGH_SPACE(SpaceId) ->
            space_logic:can_view_group_through_space(Client, SpaceId, ClientUserId, GroupId);
        _ ->
            user_logic:has_eff_group(Client, ClientUserId, GroupId)
    end;

is_user_authorized(UserId, _, _, #gri{type = od_space, scope = private}, CachedDoc) ->
    space_logic:has_eff_user(CachedDoc, UserId);
is_user_authorized(UserId, SessionId, _, #gri{type = od_space, scope = protected}, CachedDoc) ->
    user_logic:has_eff_space(SessionId, UserId, CachedDoc#document.key);

is_user_authorized(UserId, Client, _, #gri{type = od_share, scope = private}, CachedDoc) ->
    space_logic:has_eff_user(Client, CachedDoc#document.value#od_share.space, UserId);

is_user_authorized(_UserId, _, _, #gri{type = od_provider, scope = private}, _) ->
    false;

is_user_authorized(UserId, Client, AuthHint, #gri{type = od_provider, id = ProviderId, scope = protected}, CachedDoc) ->
    case {get_cache_state(CachedDoc), AuthHint} of
        {#{scope := private}, _} ->
            provider_logic:has_eff_user(CachedDoc, UserId);
        {#{scope := protected}, ?THROUGH_SPACE(SpaceId)} ->
            space_logic:has_eff_user(Client, SpaceId, UserId) andalso
                space_logic:is_supported(Client, SpaceId, ProviderId);
        _ ->
            unknown
    end;

is_user_authorized(UserId, _, _, #gri{type = od_handle_service, scope = private}, CachedDoc) ->
    handle_service_logic:has_eff_user(CachedDoc, UserId);

is_user_authorized(UserId, _, _, #gri{type = od_handle, scope = private}, CachedDoc) ->
    handle_logic:has_eff_user(CachedDoc, UserId);

is_user_authorized(_, _, _, _, _) ->
    false.

