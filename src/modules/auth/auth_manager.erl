%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Verifies subject identity in Onezone service and caches resolved auth
%%% objects (or errors in case of invalid tokens) in its ets cache.
%%% Also, to avoid exhausting memory, performs periodic checks and
%%% clears cache if size limit is breached.
%%%
%%% NOTE !!!
%%% Tokens can be revoked, which means that they will be invalid before their
%%% actual expiration. That is why verification results are cached only for
%%% limited amount of time.
%%% To assert that tokens are valid (and not revoked) verification checks
%%% should be performed periodically.
%%% @end
%%%-------------------------------------------------------------------
-module(auth_manager).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    build_token_auth/5,

    get_access_token/1,
    get_peer_ip/1,
    get_interface/1,
    get_data_access_caveats_policy/1,

    get_credentials/1, update_credentials/3,
    get_auth_override/1,

    get_caveats/1,
    verify/1,

    invalidate/1
]).
-export([start_link/0, spec/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-define(CACHE_NAME, ?MODULE).
-define(CHECK_SIZE_MSG, check_size).

-define(CACHE_SIZE_LIMIT,
    application:get_env(?APP_NAME, auth_cache_size_limit, 5000)
).
-define(CACHE_SIZE_CHECK_INTERVAL,
    application:get_env(?APP_NAME, auth_cache_size_check_interval, timer:seconds(2))
).
-define(CACHE_ITEM_DEFAULT_TTL,
    application:get_env(?APP_NAME, auth_cache_item_default_ttl, 10)
).

-define(NOW(), time_utils:system_time_seconds()).


% Record containing access token for user authorization in OZ.
-record(token_auth, {
    access_token :: tokens:serialized(),
    audience_token = undefined :: undefined | tokens:serialized(),
    peer_ip = undefined :: undefined | ip_utils:ip(),
    interface = undefined :: undefined | cv_interface:interface(),
    data_access_caveats_policy = disallow_data_access_caveats :: data_access_caveats:policy()
}).

-record(cache_item, {
    token_auth :: token_auth(),
    verification_result :: verification_result(),
    cache_expiration :: timestamp()
}).

-type state() :: undefined.
-type timestamp() :: time_utils:seconds().

-type access_token() :: tokens:serialized().
-type audience_token() :: undefined | tokens:serialized().

-type credentials() :: #credentials{}.
-opaque token_auth() :: #token_auth{}.

-type verification_result() ::
    {ok, aai:auth(), TokenValidUntil :: undefined | timestamp()}
    | errors:error().

-export_type([
    access_token/0, audience_token/0,
    credentials/0, token_auth/0, verification_result/0
]).


%%%===================================================================
%%% API
%%%===================================================================


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
get_data_access_caveats_policy(#token_auth{data_access_caveats_policy = DACP}) ->
    DACP.


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


-spec get_auth_override(token_auth()) -> gs_protocol:auth_override().
get_auth_override(#token_auth{
    access_token = Token,
    peer_ip = PeerIp,
    interface = Interface,
    audience_token = AudienceToken,
    data_access_caveats_policy = DataAccessCaveatsPolicy
}) ->
    #auth_override{
        client_auth = {token, Token},
        peer_ip = PeerIp,
        interface = Interface,
        audience_token = AudienceToken,
        data_access_caveats_policy = DataAccessCaveatsPolicy
    }.


-spec get_caveats(token_auth()) -> {ok, [caveats:caveat()]} | errors:error().
get_caveats(TokenAuth) ->
    case verify(TokenAuth) of
        {ok, #auth{caveats = Caveats}, _} ->
            {ok, Caveats};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Verifies identity of subject identified by specified token_auth()
%% and returns time this auth will be valid until. Nevertheless this
%% auth should be confirmed periodically as tokens can be revoked.
%% @end
%%--------------------------------------------------------------------
-spec verify(token_auth()) -> verification_result().
verify(TokenAuth) ->
    Now = ?NOW(),
    case ets:lookup(?CACHE_NAME, TokenAuth) of
        [#cache_item{cache_expiration = Expiration} = Item] when Now < Expiration ->
            Item#cache_item.verification_result;
        _ ->
            try
                {Result, CacheExpiration} = verify_in_onezone(
                    TokenAuth, Now, ?CACHE_ITEM_DEFAULT_TTL
                ),
                ets:insert(?CACHE_NAME, #cache_item{
                    token_auth = TokenAuth,
                    verification_result = Result,
                    cache_expiration = CacheExpiration
                }),
                %% Fetches user doc to trigger user setup if token verification
                %% succeeded. Otherwise do nothing.
                %% It must be called after caching verification result as to
                %% avoid infinite loop (gs_client_worker would try to verify
                %% given TokenAuth).
                case Result of
                    {ok, ?USER(UserId), _} ->
                        {ok, _} = user_logic:get(TokenAuth, UserId);
                    _ ->
                        ok
                end,
                Result
            catch Type:Reason ->
                ?error_stacktrace("Cannot verify user auth due to ~p:~p", [
                    Type, Reason
                ]),
                ?ERROR_UNAUTHORIZED
            end
    end.


-spec invalidate(token_auth()) -> ok.
invalidate(TokenAuth) ->
    ets:delete(?CACHE_NAME, TokenAuth),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Starts the ?MODULE server.
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


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
    ets:new(?CACHE_NAME, [
        set, public, named_table, {keypos, #cache_item.token_auth}
    ]),
    schedule_cache_size_checkup(),
    {ok, undefined, hibernate}.


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
handle_info(?CHECK_SIZE_MSG, State) ->
    case ets:info(?CACHE_NAME, size) > ?CACHE_SIZE_LIMIT of
        true -> ets:delete_all_objects(?CACHE_NAME);
        false -> ok
    end,
    schedule_cache_size_checkup(),
    {noreply, State, hibernate};
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
terminate(_Reason, _State) ->
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
-spec verify_in_onezone(token_auth(), Now :: timestamp(),
    DefaultVerificationResultTTL :: time_utils:seconds()
) ->
    {verification_result(), VerificationResultExpiration :: timestamp()}.
verify_in_onezone(#token_auth{
    access_token = AccessToken,
    peer_ip = PeerIp,
    interface = Interface,
    data_access_caveats_policy = DataAccessCaveatsPolicy
}, Now, DefaultVerificationResultTTL) ->

    Result = token_logic:verify_access_token(
        AccessToken, PeerIp,
        Interface, DataAccessCaveatsPolicy
    ),
    case Result of
        {ok, _Auth, undefined} ->
            {Result, Now + DefaultVerificationResultTTL};
        {ok, Auth, TokenTTL} ->
            Expiration = Now + min(TokenTTL, DefaultVerificationResultTTL),
            {{ok, Auth, Now + TokenTTL}, Expiration};
        {error, _} ->
            {Result, Now + DefaultVerificationResultTTL}
    end.


%% @private
-spec schedule_cache_size_checkup() -> reference().
schedule_cache_size_checkup() ->
    erlang:send_after(?CACHE_SIZE_CHECK_INTERVAL, self(), ?CHECK_SIZE_MSG).
