%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Verifies subject identity in Onezone service. Resolved auth objects
%%% (or errors in case of invalid tokens) are cached in its ets cache.
%%% Also, to avoid exhausting memory, performs periodic checks and
%%% clears cache if size limit is breached.
%%% @end
%%%-------------------------------------------------------------------
-module(auth_manager).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    pack_token_bin/1, pack_token_bin/2, unpack_token_bin/1,
    build_token_auth/4, build_token_auth/5,

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


-record(cache_item, {
    token_auth :: token_auth(),
    verification_result :: {ok, aai:auth(), ValidUntil :: undefined | timestamp()} | errors:error(),
    cache_expiration :: timestamp()
}).

-type state() :: undefined.
-type timestamp() :: time_utils:seconds().

-type subject_token() :: tokens:serialized().
-type audience_token() :: undefined | tokens:serialized().

-type token_bin() :: #token_bin{}.
-type token_auth() :: #token_auth{}.

-export_type([
    subject_token/0, audience_token/0,
    token_bin/0, token_auth/0
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec pack_token_bin(token_auth()) -> token_bin().
pack_token_bin(#token_auth{
    token = SubjectToken,
    audience_token = AudienceToken
}) ->
    pack_token_bin(SubjectToken, AudienceToken).


-spec pack_token_bin(subject_token(), audience_token()) -> token_bin().
pack_token_bin(SubjectToken, AudienceToken) ->
    #token_bin{
        subject_token = SubjectToken,
        audience_token = AudienceToken
    }.


-spec unpack_token_bin(token_bin()) -> {subject_token(), audience_token()}.
unpack_token_bin(#token_bin{
    subject_token = SubjectToken,
    audience_token = AudienceToken
}) ->
    {SubjectToken, AudienceToken}.


-spec build_token_auth(token_bin(),
    undefined | ip_utils:ip(), undefined | cv_interface:interface(),
    data_access_caveats:policy()
) ->
    token_auth().
build_token_auth(TokenBin, PeerIp, Interface, DataAccessCaveatsPolicy) ->
    build_token_auth(
        TokenBin#token_bin.subject_token, TokenBin#token_bin.audience_token,
        PeerIp, Interface, DataAccessCaveatsPolicy
    ).


-spec build_token_auth(subject_token(), audience_token(),
    undefined | ip_utils:ip(), undefined | cv_interface:interface(),
    data_access_caveats:policy()
) ->
    token_auth().
build_token_auth(SubjectToken, AudienceToken, PeerIp, Interface, DataAccessCaveatsPolicy) ->
    #token_auth{
        token = SubjectToken,
        audience_token = AudienceToken,
        peer_ip = PeerIp,
        interface = Interface,
        data_access_caveats_policy = DataAccessCaveatsPolicy
    }.


%%--------------------------------------------------------------------
%% @doc
%% Verifies identity of subject identified by specified token_auth()
%% and returns time this auth will be valid until. Nevertheless this
%% auth should be confirmed periodically as tokens can be revoked.
%% @end
%%--------------------------------------------------------------------
-spec verify(token_auth()) ->
    {ok, aai:auth(), ValidUntil :: undefined | timestamp()} | errors:error().
verify(TokenAuth) ->
    Now = ?NOW(),
    case ets:lookup(?CACHE_NAME, TokenAuth) of
        [#cache_item{cache_expiration = Expiration} = Item] when Now < Expiration ->
            Item#cache_item.verification_result;
        _ ->
            try
                {VerificationResult, CacheItemTTL} = case verify_in_onezone(TokenAuth) of
                    {ok, _Auth, undefined} = Result ->
                        {Result, ?CACHE_ITEM_DEFAULT_TTL};
                    {ok, Auth, TokenTTL} ->
                        {
                            {ok, Auth, Now + TokenTTL},
                            min(TokenTTL, ?CACHE_ITEM_DEFAULT_TTL)
                        };
                    {error, _} = Error ->
                        {Error, ?CACHE_ITEM_DEFAULT_TTL}
                end,
                ets:insert(?CACHE_NAME, #cache_item{
                    token_auth = TokenAuth,
                    verification_result = VerificationResult,
                    cache_expiration = Now + CacheItemTTL
                }),
                VerificationResult
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
-spec verify_in_onezone(token_auth()) ->
    {ok, aai:auth(), TokenTTL :: undefined | timestamp()} | errors:error().
verify_in_onezone(TokenAuth) ->
    case token_logic:verify_access_token(TokenAuth) of
        {ok, ?USER(UserId), _TokenTTL} = Result ->
            case provider_logic:has_eff_user(UserId) of
                false ->
                    ?ERROR_FORBIDDEN;
                true ->
                    % Fetch the user doc to trigger user setup
                    % (od_user:run_after/3)
                    {ok, _} = user_logic:get(TokenAuth, UserId),
                    Result
            end;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec schedule_cache_size_checkup() -> reference().
schedule_cache_size_checkup() ->
    erlang:send_after(?CACHE_SIZE_CHECK_INTERVAL, self(), ?CHECK_SIZE_MSG).
