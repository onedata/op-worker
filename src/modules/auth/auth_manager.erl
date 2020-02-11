%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Provides utility functions to operate on auth() objects. The main one being
%%% subject identity verification in Onezone service and caching resolved
%%% aai:auth objects (or errors in case of invalid tokens) in its ets cache.
%%%
%%% Cache entries are cached for as long as token expiration allows (can be
%%% cached forever if no time caveat is present). At the same time ?MODULE
%%% subscribes in oz for token events and monitors their status so that
%%% eventual changes (eg. revocation) will be reflected in cache.
%%% To avoid exhausting memory, ?MODULE performs periodic checks and clears
%%% cache if size limit is breached. Cache is also cleared, with some delay,
%%% when connection to oz is lost (subscriptions may became invalid) and
%%% immediately when mentioned connection is restored.
%%%
%%% NOTE !!!
%%% Tokens can be revoked and deleted, which means that they may become invalid
%%% before their actual expiration.
%%% To assert that tokens are valid (and not revoked/deleted) verification
%%% checks should be performed periodically.
%%% @end
%%%-------------------------------------------------------------------
-module(auth_manager).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export([root_auth/0, guest_auth/0]).
-export([
    build_token_auth/5,
    is_token_auth/1,

    get_access_token/1,
    get_peer_ip/1,
    get_interface/1,
    get_data_access_caveats_policy/1,
    get_credentials/1, update_credentials/3
]).
-export([
    auth_to_gs_auth_override/1,
    get_caveats/1,
    verify_auth/1,

    invalidate_cache_entry/1
]).
-export([start_link/0, spec/0]).
-export([
    report_oz_connection_start/0,
    report_oz_connection_termination/0,

    report_token_status_update/1,
    report_token_deletion/1,

    report_temporary_tokens_generation_change/1,
    report_temporary_tokens_deletion/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).


-type credentials() :: #credentials{}.
-type access_token() :: tokens:serialized().
-type audience_token() :: undefined | tokens:serialized().

% Record containing access token for user authorization in OZ.
-record(token_auth, {
    access_token :: access_token(),
    audience_token = undefined :: audience_token(),
    peer_ip = undefined :: undefined | ip_utils:ip(),
    interface = undefined :: undefined | cv_interface:interface(),
    data_access_caveats_policy = disallow_data_access_caveats :: data_access_caveats:policy()
}).

-opaque token_auth() :: #token_auth{}.
-type guest_auth() :: ?GUEST_AUTH.
-type root_auth() :: ?ROOT_AUTH.
-type auth() :: token_auth() | guest_auth() | root_auth().

-type timestamp() :: time_utils:seconds().

-type verification_result() ::
    {ok, aai:auth(), TokenValidUntil :: undefined | timestamp()} |
    errors:error().

-type token_ref() ::
    {named, tokens:id()} |
    {temporary, od_user:id(), temporary_token_secret:generation()}.

-record(cache_entry, {
    token_auth :: token_auth(),
    verification_result :: verification_result(),

    token_ref :: undefined | token_ref(),
    token_revoked = false :: boolean(),
    cache_expiration :: timestamp()
}).

-record(state, {
    cache_invalidation_timer = undefined :: undefined | reference()
}).
-type state() :: #state{}.

-export_type([
    credentials/0, access_token/0, audience_token/0,
    token_auth/0, guest_auth/0, root_auth/0, auth/0,
    verification_result/0
]).


-define(CACHE_NAME, ?MODULE).
-define(CHECK_CACHE_SIZE_REQ, check_cache_size).

-define(CACHE_SIZE_LIMIT,
    application:get_env(?APP_NAME, auth_cache_size_limit, 5000)
).
-define(CACHE_SIZE_CHECK_INTERVAL,
    application:get_env(?APP_NAME, auth_cache_size_check_interval, timer:seconds(2))
).
-define(CACHE_ITEM_DEFAULT_TTL,
    application:get_env(?APP_NAME, auth_cache_item_default_ttl, 10)
).

-define(OZ_CONNECTION_STARTED_MSG, oz_connection_stared).
-define(OZ_CONNECTION_TERMINATED_MSG, oz_connection_terminated).

-define(INVALIDATE_CACHE_REQ, invalidate_cache).
-define(CACHE_INVALIDATION_DELAY,
    application:get_env(?APP_NAME, auth_invalidation_delay, timer:seconds(300))
).

-define(MONITOR_TOKEN_REQ(__TokenAuth, __TokenRef),
    {monitor_token, __TokenAuth, __TokenRef}
).

-define(TOKEN_STATUS_CHANGED_MSG(__TokenId, __IsRevoked),
    {token_status_changed, __TokenId, __IsRevoked}
).
-define(TOKEN_DELETED_MSG(__TokenId), {token_deleted, __TokenId}).

-define(TEMP_TOKENS_GENERATION_CHANGED_MSG(__UserId, __Generation),
    {temporary_token_generation_changed, __UserId, __Generation}
).
-define(TEMP_TOKENS_DELETED_MSG(__UserId), {temporary_tokens_deleted, __UserId}).

-define(NOW(), time_utils:system_time_seconds()).


%%%===================================================================
%%% API
%%%===================================================================


-spec root_auth() -> root_auth().
root_auth() ->
    ?ROOT_AUTH.


-spec guest_auth() -> guest_auth().
guest_auth() ->
    ?GUEST_AUTH.


-spec build_token_auth(
    access_token(), audience_token(),
    PeerIp :: undefined | ip_utils:ip(),
    Interface :: undefined | cv_interface:interface(),
    data_access_caveats:policy()
) ->
    token_auth().
build_token_auth(AccessToken, AudienceToken, PeerIp, Interface, DataAccessCaveatsPolicy) ->
    #token_auth{
        access_token = AccessToken,
        audience_token = AudienceToken,
        peer_ip = PeerIp,
        interface = Interface,
        data_access_caveats_policy = DataAccessCaveatsPolicy
    }.


-spec is_token_auth(token_auth() | any()) -> boolean().
is_token_auth(#token_auth{}) -> true;
is_token_auth(_) -> false.


-spec get_access_token(token_auth()) -> access_token().
get_access_token(#token_auth{access_token = AccessToken}) ->
    AccessToken.


-spec get_peer_ip(token_auth()) -> undefined | ip_utils:ip().
get_peer_ip(#token_auth{peer_ip = PeerIp}) ->
    PeerIp.


-spec get_interface(token_auth()) -> undefined | cv_interface:interface().
get_interface(#token_auth{interface = Interface}) ->
    Interface.


-spec get_data_access_caveats_policy(token_auth()) ->
    data_access_caveats:policy().
get_data_access_caveats_policy(#token_auth{data_access_caveats_policy = Policy}) ->
    Policy.


-spec get_credentials(token_auth()) -> credentials().
get_credentials(#token_auth{
    access_token = AccessToken,
    audience_token = AudienceToken
}) ->
    #credentials{
        access_token = AccessToken,
        audience_token = AudienceToken
    }.


-spec update_credentials(token_auth(), access_token(), audience_token()) ->
    token_auth().
update_credentials(TokenAuth, AccessToken, AudienceToken) ->
    TokenAuth#token_auth{
        access_token = AccessToken,
        audience_token = AudienceToken
    }.


-spec auth_to_gs_auth_override(auth()) -> gs_protocol:auth_override().
auth_to_gs_auth_override(?ROOT_AUTH) ->
    undefined;
auth_to_gs_auth_override(?GUEST_AUTH) ->
    #auth_override{client_auth = nobody};
auth_to_gs_auth_override(#token_auth{
    access_token = AccessToken,
    peer_ip = PeerIp,
    interface = Interface,
    audience_token = AudienceToken,
    data_access_caveats_policy = DataAccessCaveatsPolicy
}) ->
    #auth_override{
        client_auth = {token, AccessToken},
        peer_ip = PeerIp,
        interface = Interface,
        audience_token = AudienceToken,
        data_access_caveats_policy = DataAccessCaveatsPolicy
    }.


-spec get_caveats(auth()) -> {ok, [caveats:caveat()]} | errors:error().
get_caveats(?ROOT_AUTH) ->
    {ok, []};
get_caveats(?GUEST_AUTH) ->
    {ok, []};
get_caveats(TokenAuth) ->
    case verify_auth(TokenAuth) of
        {ok, #auth{caveats = Caveats}, _} ->
            {ok, Caveats};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Verifies identity of subject identified by specified auth() and returns
%% time this auth will be valid until. Nevertheless this check should be
%% performed periodically for token_auth() as tokens can be revoked.
%% @end
%%--------------------------------------------------------------------
-spec verify_auth(auth()) -> verification_result().
verify_auth(?ROOT_AUTH) ->
    {ok, #auth{subject = ?SUB(root, ?ROOT_USER_ID)}, undefined};
verify_auth(?GUEST_AUTH) ->
    {ok, #auth{subject = ?SUB(nobody, ?GUEST_USER_ID)}, undefined};
verify_auth(TokenAuth) ->
    verify_token_auth(TokenAuth).


%%--------------------------------------------------------------------
%% @doc
%% Starts the ?MODULE server.
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec report_oz_connection_start() -> ok.
report_oz_connection_start() ->
    Nodes = consistent_hashing:get_all_nodes(),
    gen_server:abcast(Nodes, ?MODULE, ?OZ_CONNECTION_STARTED_MSG),
    ok.


-spec report_oz_connection_termination() -> ok.
report_oz_connection_termination() ->
    Nodes = consistent_hashing:get_all_nodes(),
    gen_server:abcast(Nodes, ?MODULE, ?OZ_CONNECTION_TERMINATED_MSG),
    ok.


-spec report_token_status_update(od_token:doc()) -> ok.
report_token_status_update(#document{
    key = TokenId,
    value = #od_token{revoked = IsRevoked}
}) ->
    gen_server:abcast(
        consistent_hashing:get_all_nodes(), ?MODULE,
        ?TOKEN_STATUS_CHANGED_MSG(TokenId, IsRevoked)
    ),
    ok.


-spec report_token_deletion(od_token:doc()) -> ok.
report_token_deletion(#document{key = TokenId}) ->
    gen_server:abcast(
        consistent_hashing:get_all_nodes(), ?MODULE,
        ?TOKEN_DELETED_MSG(TokenId)
    ),
    ok.


-spec report_temporary_tokens_generation_change(temporary_token_secret:doc()) ->
    ok.
report_temporary_tokens_generation_change(#document{
    key = UserId,
    value = #temporary_token_secret{generation = Generation}
}) ->
    gen_server:abcast(
        consistent_hashing:get_all_nodes(), ?MODULE,
        ?TEMP_TOKENS_GENERATION_CHANGED_MSG(UserId, Generation)
    ),
    ok.


-spec report_temporary_tokens_deletion(temporary_token_secret:doc()) -> ok.
report_temporary_tokens_deletion(#document{key = UserId}) ->
    gen_server:abcast(
        consistent_hashing:get_all_nodes(), ?MODULE,
        ?TEMP_TOKENS_DELETED_MSG(UserId)
    ),
    ok.


-spec invalidate_cache_entry(token_auth()) -> ok.
invalidate_cache_entry(TokenAuth) ->
    ets:delete(?CACHE_NAME, TokenAuth),
    ok.


%%-------------------------------------------------------------------
%% @doc
%% Returns child spec for ?MODULE to attach it to supervision.
%% @end
%%-------------------------------------------------------------------
-spec spec() -> supervisor:child_spec().
spec() -> #{
    id => ?MODULE,
    start => {?MODULE, start_link, []},
    restart => permanent,
    shutdown => timer:seconds(10),
    type => worker,
    modules => [?MODULE]
}.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, state()} | {ok, state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init(_) ->
    process_flag(trap_exit, true),

    ?CACHE_NAME = ets:new(?CACHE_NAME, [
        set, public, named_table, {keypos, #cache_entry.token_auth}
    ]),
    schedule_cache_size_checkup(),

    {ok, #state{}, hibernate}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, {error, wrong_request}, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast(?OZ_CONNECTION_STARTED_MSG, State0) ->
    ets:delete_all_objects(?CACHE_NAME),
    {noreply, cancel_cache_invalidation_timer(State0)};

handle_cast(?OZ_CONNECTION_TERMINATED_MSG, State) ->
    {noreply, schedule_cache_invalidation(State)};

handle_cast(?MONITOR_TOKEN_REQ(TokenAuth, TokenRef), State) ->
    case is_token_revoked(TokenRef) of
        {ok, IsRevoked} ->
            ets:update_element(?CACHE_NAME, TokenAuth, [
                {#cache_entry.token_revoked, IsRevoked}
            ]);
        {error, _} = Error ->
            ets:insert(?CACHE_NAME, #cache_entry{
                token_auth = TokenAuth,
                verification_result = Error,

                token_ref = TokenRef,
                cache_expiration = ?NOW() + ?CACHE_ITEM_DEFAULT_TTL
            })
    end,
    {noreply, State};

handle_cast(?TOKEN_STATUS_CHANGED_MSG(TokenId, IsRevoked), State) ->
    ets:select_replace(?CACHE_NAME, ets:fun2ms(fun(#cache_entry{
        token_ref = {named, Id},
        token_revoked = OldIsRevoked
    } = CacheEntry) when Id == TokenId andalso OldIsRevoked /= IsRevoked ->
        CacheEntry#cache_entry{
            token_revoked = IsRevoked
        }
    end)),
    {noreply, State};

handle_cast(?TOKEN_DELETED_MSG(TokenId), State) ->
    ets:select_replace(?CACHE_NAME, ets:fun2ms(fun(#cache_entry{
        token_ref = {named, Id}
    } = CacheEntry) when Id == TokenId ->
        CacheEntry#cache_entry{
            verification_result = ?ERROR_TOKEN_INVALID,
            token_revoked = false,
            cache_expiration = undefined
        }
    end)),
    {noreply, State};

handle_cast(?TEMP_TOKENS_GENERATION_CHANGED_MSG(UserId, Generation), State) ->
    ets:select_replace(?CACHE_NAME, ets:fun2ms(fun(#cache_entry{
        token_ref = {temporary, Id, OldGeneration}
    } = CacheEntry) when Id == UserId andalso OldGeneration < Generation ->
        CacheEntry#cache_entry{
            verification_result = ?ERROR_TOKEN_REVOKED,
            token_revoked = false,
            cache_expiration = undefined
        }
    end)),
    {noreply, State};

handle_cast(?TEMP_TOKENS_DELETED_MSG(UserId), State) ->
    ets:select_replace(?CACHE_NAME, ets:fun2ms(fun(#cache_entry{
        token_ref = {temporary, Id, _Generation}
    } = CacheEntry) when Id == UserId ->
        CacheEntry#cache_entry{
            verification_result = ?ERROR_TOKEN_INVALID,
            token_revoked = false,
            cache_expiration = undefined
        }
    end)),
    {noreply, State};

handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info(?CHECK_CACHE_SIZE_REQ, State) ->
    case ets:info(?CACHE_NAME, size) > ?CACHE_SIZE_LIMIT of
        true -> ets:delete_all_objects(?CACHE_NAME);
        false -> ok
    end,
    schedule_cache_size_checkup(),
    {noreply, State};

handle_info(?INVALIDATE_CACHE_REQ, State) ->
    ets:delete_all_objects(?CACHE_NAME),
    {noreply, cancel_cache_invalidation_timer(State)};

handle_info(Info, State) ->
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
    state()) -> term().
terminate(Reason, State) ->
    ?log_terminate(Reason, State),
    ets:delete(?CACHE_NAME),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, state(), Extra :: term()) ->
    {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec verify_token_auth(token_auth()) -> verification_result().
verify_token_auth(TokenAuth) ->
    case get_token_auth_verification_result_from_cache(TokenAuth) of
        {ok, CachedVerificationResult} ->
            CachedVerificationResult;
        ?ERROR_NOT_FOUND ->
            try
                verify_token_auth_and_cache_result(TokenAuth)
            catch Type:Reason ->
                ?error_stacktrace("Cannot verify user auth due to ~p:~p", [
                    Type, Reason
                ]),
                ?ERROR_UNAUTHORIZED
            end
    end.


%% @private
-spec verify_token_auth_and_cache_result(token_auth()) ->
    verification_result().
verify_token_auth_and_cache_result(#token_auth{
    access_token = AccessToken,
    peer_ip = PeerIp,
    interface = Interface,
    data_access_caveats_policy = DataAccessCaveatsPolicy
} = TokenAuth) ->
    {TokenRef, Result} = case deserialize_and_validate_token(AccessToken) of
        {ok, #token{subject = Subject} = Token} ->
            AccessTokenVerificationResult = token_logic:verify_access_token(
                AccessToken, PeerIp, Interface, DataAccessCaveatsPolicy
            ),
            case AccessTokenVerificationResult of
                {ok, Subject, TokenTTL} ->
                    AaiAuth = build_aai_auth_from_token(Token),
                    TokenExpiration = infer_token_expiration(TokenTTL),
                    {get_token_ref(Token), {ok, AaiAuth, TokenExpiration}};
                {error, _} = VerificationError ->
                    {undefined, VerificationError}
            end;
        {error, _} = DeserializationError ->
            {undefined, DeserializationError}
    end,

    case save_token_auth_verification_result_in_cache(TokenAuth, TokenRef, Result) of
        true ->
            maybe_request_auth_manager_to_monitor_token(TokenAuth, TokenRef, Result),
            maybe_fetch_user_data(TokenAuth, Result);
        false ->
            ok
    end,
    Result.


%% @private
-spec deserialize_and_validate_token(tokens:serialized()) ->
    {ok, tokens:token()} | errors:error().
deserialize_and_validate_token(SerializedToken) ->
    case tokens:deserialize(SerializedToken) of
        {ok, #token{subject = ?SUB(user, UserId)} = Token} ->
            case provider_logic:has_eff_user(UserId) of
                true ->
                    {ok, Token};
                false ->
                    ?ERROR_USER_NOT_SUPPORTED
            end;
        {ok, _} ->
            ?ERROR_TOKEN_SUBJECT_INVALID;
        {error, _} = DeserializationError ->
            DeserializationError
    end.


%% @private
-spec build_aai_auth_from_token(tokens:token()) -> aai:auth().
build_aai_auth_from_token(#token{subject = Subject} = Token) ->
    #auth{
        subject = Subject,
        caveats = tokens:get_caveats(Token)
    }.


%% @private
-spec infer_token_expiration
    (TokenTTL :: undefined) -> undefined;
    (TokenTTL :: time_utils:seconds()) -> timestamp().
infer_token_expiration(undefined) -> undefined;
infer_token_expiration(TokenTTL) -> ?NOW() + TokenTTL.


%% @private
-spec get_token_ref(tokens:token()) -> token_ref().
get_token_ref(#token{persistence = named, id = TokenId}) ->
    {named, TokenId};
get_token_ref(#token{
    persistence = {temporary, Generation},
    subject = ?SUB(user, UserId)
}) ->
    {temporary, UserId, Generation}.


%% @private
-spec is_token_revoked(token_ref()) -> {ok, boolean()} | errors:error().
is_token_revoked({named, TokenId}) ->
    token_logic:is_token_revoked(TokenId);
is_token_revoked({temporary, UserId, Generation}) ->
    case token_logic:get_temporary_tokens_generation(UserId) of
        {ok, ActualGeneration} ->
            {ok, Generation /= ActualGeneration};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asks auth_manager to monitor token status/revocation in case of successful
%% token verification.
%% @end
%%--------------------------------------------------------------------
-spec maybe_request_auth_manager_to_monitor_token(
    token_auth(),
    token_ref(),
    verification_result()
) ->
    ok.
maybe_request_auth_manager_to_monitor_token(_TokenAuth, _TokenRef, {error, _}) ->
    ok;
maybe_request_auth_manager_to_monitor_token(TokenAuth, TokenRef, {ok, _, _}) ->
    gen_server:cast(?MODULE, ?MONITOR_TOKEN_REQ(TokenAuth, TokenRef)),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fetches user doc to trigger user setup if token verification succeeded.
%% Otherwise does nothing.
%% It must be called after caching verification result as to avoid infinite
%% loop (gs_client_worker would try to verify given TokenAuth).
%% @end
%%--------------------------------------------------------------------
-spec maybe_fetch_user_data(token_auth(), verification_result()) -> ok.
maybe_fetch_user_data(_TokenAuth, {error, _}) ->
    ok;
maybe_fetch_user_data(TokenAuth, {ok, ?USER(UserId), _TokenExpiration}) ->
    user_logic:get(TokenAuth, UserId),
    ok.


%% @private
-spec get_token_auth_verification_result_from_cache(token_auth()) ->
    {ok, verification_result()} | ?ERROR_NOT_FOUND.
get_token_auth_verification_result_from_cache(TokenAuth) ->
    Now = ?NOW(),
    try ets:lookup(?CACHE_NAME, TokenAuth) of
        [#cache_entry{cache_expiration = Expiration} = Item] when
            Expiration == undefined;
            (is_integer(Expiration) andalso Now < Expiration)
        ->
            case Item#cache_entry.token_revoked of
                true ->
                    {ok, ?ERROR_TOKEN_REVOKED};
                false ->
                    {ok, Item#cache_entry.verification_result}
            end;
        _ ->
            ?ERROR_NOT_FOUND
    catch Type:Reason ->
        ?warning("Failed to lookup ~p cache (ets table) due to ~p:~p", [
            ?MODULE, Type, Reason
        ]),
        ?ERROR_NOT_FOUND
    end.


%% @private
-spec save_token_auth_verification_result_in_cache(
    token_auth(),
    undefined | token_ref(),
    verification_result()
) ->
    boolean().
save_token_auth_verification_result_in_cache(TokenAuth, TokenRef, VerificationResult) ->
    CacheEntry = #cache_entry{
        token_auth = TokenAuth,
        verification_result = VerificationResult,

        token_ref = TokenRef,
        token_revoked = false,
        cache_expiration = infer_cache_entry_expiration(TokenAuth, VerificationResult)
    },
    try
        ets:insert(?CACHE_NAME, CacheEntry),
        true
    catch Type:Reason ->
        ?warning("Failed to save entry in ~p cache (ets table) due to ~p:~p", [
            ?MODULE, Type, Reason
        ]),
        false
    end.


%% @private
-spec infer_cache_entry_expiration(token_auth(), verification_result()) ->
    undefined | timestamp().
infer_cache_entry_expiration(_TokenAuth, {error, _}) ->
    ?NOW() + ?CACHE_ITEM_DEFAULT_TTL;
infer_cache_entry_expiration(#token_auth{audience_token = undefined}, {ok, _, Expiration}) ->
    Expiration;
infer_cache_entry_expiration(_TokenAuth, {ok, _, _}) ->
    % Audience token may come from subject not supported by this provider and
    % as such cannot be monitored (subscription in oz for token issued by such
    % subjects). That is why verification_result() for token_auth() with
    % audience_token() should be cached only for short period of time.
    ?NOW() + ?CACHE_ITEM_DEFAULT_TTL.


%% @private
-spec schedule_cache_size_checkup() -> reference().
schedule_cache_size_checkup() ->
    erlang:send_after(?CACHE_SIZE_CHECK_INTERVAL, self(), ?CHECK_CACHE_SIZE_REQ).


%% @private
-spec schedule_cache_invalidation(state()) -> state().
schedule_cache_invalidation(#state{cache_invalidation_timer = undefined} = State) ->
    State#state{cache_invalidation_timer = erlang:send_after(
        ?CACHE_INVALIDATION_DELAY, self(), ?INVALIDATE_CACHE_REQ
    )};
schedule_cache_invalidation(State) ->
    State.


%% @private
-spec cancel_cache_invalidation_timer(state()) -> state().
cancel_cache_invalidation_timer(#state{cache_invalidation_timer = undefined} = State) ->
    State;
cancel_cache_invalidation_timer(#state{cache_invalidation_timer = TimerRef} = State) ->
    erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]),
    State#state{cache_invalidation_timer = undefined}.
