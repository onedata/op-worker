%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Creates, maintains and provides functions for manipulation of auth cache,
%%% that is mapping between token credentials and user auth.
%%% Cache entries are kept for as long as token expiration allows (can be
%%% cached forever if no time caveat is present). At the same time ?SERVER
%%% subscribes in Onezone for token status updates and monitors them so that
%%% it can reflect those changes (eg. revocation) in cached entries.
%%% To avoid exhausting memory, ?SERVER performs periodic checks and purges
%%% cache if size limit is breached. Cache is also purged, with some delay,
%%% when connection to oz is lost (subscriptions may became invalid) and
%%% immediately when mentioned connection is restored.
%%% @end
%%%-------------------------------------------------------------------
-module(auth_cache).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export([spec/0, start_link/0]).
-export([
    get_token_ref/1,
    get_token_credentials_verification_result/1,
    save_token_credentials_verification_result/3,

    delete_cache_entry/1
]).
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

-type timestamp() :: time_utils:seconds().

-type token_ref() ::
    {named, tokens:id()} |
    {temporary, od_user:id(), temporary_token_secret:generation()}.

-record(cache_entry, {
    token_credentials :: auth_manager:token_credentials(),
    verification_result :: auth_manager:verification_result(),

    token_ref :: undefined | token_ref(),
    token_revoked = false :: boolean(),
    cache_expiration :: undefined | timestamp()
}).

-record(state, {
    cache_purge_timer = undefined :: undefined | reference()
}).
-type state() :: #state{}.

-export_type([token_ref/0]).


-define(SERVER, ?MODULE).
-define(CACHE_NAME, ?MODULE).

-define(CHECK_CACHE_SIZE_REQ, check_cache_size).
-define(CACHE_SIZE_CHECK_INTERVAL, application:get_env(
    ?APP_NAME, auth_cache_size_check_interval, timer:seconds(2)
)).
-define(CACHE_SIZE_LIMIT, application:get_env(
    ?APP_NAME, auth_cache_size_limit, 5000
)).

-define(PURGE_CACHE_REQ, purge_cache).
-define(CACHE_PURGE_DELAY, application:get_env(
    ?APP_NAME, auth_cache_purge_delay, timer:seconds(300)
)).

-define(CACHE_ITEM_DEFAULT_TTL, application:get_env(
    ?APP_NAME, auth_cache_item_default_ttl, 10
)).

-define(OZ_CONNECTION_STARTED_MSG, oz_connection_stared).
-define(OZ_CONNECTION_TERMINATED_MSG, oz_connection_terminated).

-define(MONITOR_TOKEN_REQ(__TokenCredentials, __TokenRef),
    {monitor_token, __TokenCredentials, __TokenRef}
).

-define(TOKEN_STATUS_CHANGED_MSG(__TokenId), {token_status_changed, __TokenId}).
-define(TOKEN_DELETED_MSG(__TokenId), {token_deleted, __TokenId}).

-define(TEMP_TOKENS_GENERATION_CHANGED_MSG(__UserId, __Generation),
    {temporary_token_generation_changed, __UserId, __Generation}
).
-define(TEMP_TOKENS_DELETED_MSG(__UserId),
    {temporary_tokens_deleted, __UserId}
).

-define(NOW(), time_utils:system_time_seconds()).


%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Returns child spec for ?SERVER to attach it to supervision.
%% @end
%%-------------------------------------------------------------------
-spec spec() -> supervisor:child_spec().
spec() -> #{
    id => ?SERVER,
    start => {?MODULE, start_link, []},
    restart => permanent,
    shutdown => timer:seconds(10),
    type => worker,
    modules => [?MODULE]
}.


%%--------------------------------------------------------------------
%% @doc
%% Starts the ?SERVER server.
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


-spec get_token_ref(tokens:token()) -> token_ref().
get_token_ref(#token{persistence = named, id = TokenId}) ->
    {named, TokenId};
get_token_ref(#token{
    persistence = {temporary, Generation},
    subject = ?SUB(user, UserId)
}) ->
    {temporary, UserId, Generation}.


-spec get_token_credentials_verification_result(auth_manager:token_credentials()) ->
    {ok, auth_manager:verification_result()} | ?ERROR_NOT_FOUND.
get_token_credentials_verification_result(TokenCredentials) ->
    Now = ?NOW(),
    try ets:lookup(?CACHE_NAME, TokenCredentials) of
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
            ?CACHE_NAME, Type, Reason
        ]),
        ?ERROR_NOT_FOUND
    end.


-spec save_token_credentials_verification_result(
    auth_manager:token_credentials(),
    undefined | token_ref(),
    auth_manager:verification_result()
) ->
    boolean().
save_token_credentials_verification_result(TokenCredentials, TokenRef, VerificationResult) ->
    CacheEntry = #cache_entry{
        token_credentials = TokenCredentials,
        verification_result = VerificationResult,

        token_ref = TokenRef,
        token_revoked = false,
        cache_expiration = infer_cache_entry_expiration(TokenCredentials, VerificationResult)
    },
    try
        ets:insert(?CACHE_NAME, CacheEntry),
        maybe_request_token_monitoring(TokenCredentials, TokenRef, VerificationResult),
        maybe_fetch_user_data(TokenCredentials, VerificationResult),
        true
    catch Type:Reason ->
        ?warning("Failed to save entry in ~p cache (ets table) due to ~p:~p", [
            ?CACHE_NAME, Type, Reason
        ]),
        false
    end.


-spec delete_cache_entry(auth_manager:token_credentials()) -> ok.
delete_cache_entry(TokenCredentials) ->
    ets:delete(?CACHE_NAME, TokenCredentials),
    ok.


-spec report_oz_connection_start() -> ok.
report_oz_connection_start() ->
    broadcast(?OZ_CONNECTION_STARTED_MSG).


-spec report_oz_connection_termination() -> ok.
report_oz_connection_termination() ->
    broadcast(?OZ_CONNECTION_TERMINATED_MSG).


-spec report_token_status_update(od_token:id()) -> ok.
report_token_status_update(TokenId) ->
    broadcast(?TOKEN_STATUS_CHANGED_MSG(TokenId)).


-spec report_token_deletion(od_token:id()) -> ok.
report_token_deletion(TokenId) ->
    broadcast(?TOKEN_DELETED_MSG(TokenId)).


-spec report_temporary_tokens_generation_change(temporary_token_secret:doc()) ->
    ok.
report_temporary_tokens_generation_change(#document{
    key = UserId,
    value = #temporary_token_secret{generation = Generation}
}) ->
    broadcast(?TEMP_TOKENS_GENERATION_CHANGED_MSG(UserId, Generation)).


-spec report_temporary_tokens_deletion(od_user:id()) -> ok.
report_temporary_tokens_deletion(UserId) ->
    broadcast(?TEMP_TOKENS_DELETED_MSG(UserId)).


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
        set, public, named_table, {keypos, #cache_entry.token_credentials}
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
handle_cast(?OZ_CONNECTION_STARTED_MSG, State) ->
    {noreply, purge_cache(State)};

handle_cast(?OZ_CONNECTION_TERMINATED_MSG, State) ->
    {noreply, schedule_cache_purge(State)};

handle_cast(?MONITOR_TOKEN_REQ(TokenCredentials, TokenRef), State) ->
    ?debug("Received request to monitor token (~s)", [TokenRef]),

    case subscribe_for_token_changes(TokenRef) of
        {ok, IsTokenRevoked} ->
            ets:update_element(?CACHE_NAME, TokenCredentials, [
                {#cache_entry.token_revoked, IsTokenRevoked}
            ]);
        {error, _} = Error ->
            ets:insert(?CACHE_NAME, #cache_entry{
                token_credentials = TokenCredentials,
                verification_result = Error,

                token_ref = TokenRef,
                cache_expiration = ?NOW() + ?CACHE_ITEM_DEFAULT_TTL
            })
    end,
    {noreply, State};

handle_cast(?TOKEN_STATUS_CHANGED_MSG(TokenId), State) ->
    ?debug("Received token status changed event for token ~s", [TokenId]),

    case token_logic:is_token_revoked(TokenId) of
        {ok, IsRevoked} ->
            ets:select_replace(?CACHE_NAME, ets:fun2ms(fun(#cache_entry{
                verification_result = {ok, _, _},
                token_ref = {named, Id},
                token_revoked = OldIsRevoked
            } = CacheEntry) when Id == TokenId andalso OldIsRevoked /= IsRevoked ->
                CacheEntry#cache_entry{
                    token_revoked = IsRevoked
                }
            end));
        {error, _} = Error ->
            Expiration = ?NOW() + ?CACHE_ITEM_DEFAULT_TTL,
            ets:select_replace(?CACHE_NAME, ets:fun2ms(fun(#cache_entry{
                token_ref = {named, Id}
            } = CacheEntry) when Id == TokenId ->
                CacheEntry#cache_entry{
                    verification_result = Error,
                    token_revoked = false,
                    cache_expiration = Expiration
                }
            end))
    end,
    {noreply, State};

handle_cast(?TOKEN_DELETED_MSG(TokenId), State) ->
    ?debug("Received token deleted event for token ~s", [TokenId]),

    Expiration = ?NOW() + ?CACHE_ITEM_DEFAULT_TTL,
    ets:select_replace(?CACHE_NAME, ets:fun2ms(fun(#cache_entry{
        token_ref = {named, Id}
    } = CacheEntry) when Id == TokenId ->
        CacheEntry#cache_entry{
            verification_result = ?ERROR_TOKEN_INVALID,
            token_revoked = false,
            cache_expiration = Expiration
        }
    end)),
    {noreply, State};

handle_cast(?TEMP_TOKENS_GENERATION_CHANGED_MSG(UserId, Generation), State) ->
    ?debug("Received temporary tokens generation changed (gen: ~p) event for user ~s", [
        UserId, Generation
    ]),
    Expiration = ?NOW() + ?CACHE_ITEM_DEFAULT_TTL,
    ets:select_replace(?CACHE_NAME, ets:fun2ms(fun(#cache_entry{
        verification_result = {ok, _, _},
        token_ref = {temporary, Id, OldGeneration}
    } = CacheEntry) when Id == UserId andalso OldGeneration < Generation ->
        CacheEntry#cache_entry{
            verification_result = ?ERROR_TOKEN_REVOKED,
            token_revoked = false,
            cache_expiration = Expiration
        }
    end)),
    {noreply, State};

handle_cast(?TEMP_TOKENS_DELETED_MSG(UserId), State) ->
    ?debug("Received temporary tokens deleted event for user ~s", [UserId]),

    Expiration = ?NOW() + ?CACHE_ITEM_DEFAULT_TTL,
    ets:select_replace(?CACHE_NAME, ets:fun2ms(fun(#cache_entry{
        token_ref = {temporary, Id, _Generation}
    } = CacheEntry) when Id == UserId ->
        CacheEntry#cache_entry{
            verification_result = ?ERROR_TOKEN_INVALID,
            token_revoked = false,
            cache_expiration = Expiration
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

handle_info(?PURGE_CACHE_REQ, State) ->
    {noreply, purge_cache(State)};

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
-spec broadcast(Msg :: term()) -> ok.
broadcast(Msg) ->
    gen_server:abcast(consistent_hashing:get_all_nodes(), ?SERVER, Msg),
    ok.


%% @private
-spec purge_cache(state()) -> state().
purge_cache(State) ->
    ets:delete_all_objects(?CACHE_NAME),
    ?debug("Purged ~p cache", [?CACHE_NAME]),

    cancel_cache_purge_timer(State).


%% @private
-spec subscribe_for_token_changes(token_ref()) ->
    {ok, IsTokenRevoked :: boolean()} | errors:error().
subscribe_for_token_changes({named, TokenId}) ->
    token_logic:is_token_revoked(TokenId);
subscribe_for_token_changes({temporary, UserId, Generation}) ->
    case token_logic:get_temporary_tokens_generation(UserId) of
        {ok, ActualGeneration} ->
            {ok, Generation /= ActualGeneration};
        {error, _} = Error ->
            Error
    end.


%% @private
-spec infer_cache_entry_expiration(auth_manager:token_credentials(),
    auth_manager:verification_result()) -> undefined | timestamp().
infer_cache_entry_expiration(_TokenCredentials, {error, _}) ->
    ?NOW() + ?CACHE_ITEM_DEFAULT_TTL;
infer_cache_entry_expiration(TokenCredentials, {ok, _, Expiration}) ->
    case auth_manager:get_consumer_token(TokenCredentials) of
        undefined ->
            Expiration;
        _ ->
            % Consumer token may come from subject not supported by this
            % provider and as such cannot be monitored (subscription in
            % oz for token issued by such subjects). That is why verification
            % result for token_credentials with consumer token should be
            % cached only for short period of time.
            ?NOW() + ?CACHE_ITEM_DEFAULT_TTL
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Asks ?SERVER to monitor token status/revocation in case of successful
%% token verification. In case of changes in token status/revocation or
%% token deletion ?SERVER reflects those changes in cached entry.
%% @end
%%--------------------------------------------------------------------
-spec maybe_request_token_monitoring(auth_manager:token_credentials(),
    undefined | token_ref(), auth_manager:verification_result()) -> ok.
maybe_request_token_monitoring(_TokenCredentials, _TokenRef, {error, _}) ->
    ok;
maybe_request_token_monitoring(TokenCredentials, TokenRef, {ok, _, _}) ->
    gen_server:cast(?SERVER, ?MONITOR_TOKEN_REQ(TokenCredentials, TokenRef)),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fetches user doc to trigger user setup if token verification succeeded.
%% Otherwise does nothing.
%%
%% NOTE !!!
%% It must be called after caching verification result as to avoid infinite
%% loop (gs_client_worker would try to verify given TokenCredentials).
%% @end
%%--------------------------------------------------------------------
-spec maybe_fetch_user_data(auth_manager:token_credentials(),
    auth_manager:verification_result()) -> ok.
maybe_fetch_user_data(_TokenCredentials, {error, _}) ->
    ok;
maybe_fetch_user_data(TokenCredentials, {ok, ?USER(UserId), _TokenExpiration}) ->
    user_logic:get(TokenCredentials, UserId),
    ok.


%% @private
-spec schedule_cache_size_checkup() -> reference().
schedule_cache_size_checkup() ->
    erlang:send_after(?CACHE_SIZE_CHECK_INTERVAL, self(), ?CHECK_CACHE_SIZE_REQ).


%% @private
-spec schedule_cache_purge(state()) -> state().
schedule_cache_purge(#state{cache_purge_timer = undefined} = State) ->
    State#state{cache_purge_timer = erlang:send_after(
        ?CACHE_PURGE_DELAY, self(), ?PURGE_CACHE_REQ
    )};
schedule_cache_purge(State) ->
    State.


%% @private
-spec cancel_cache_purge_timer(state()) -> state().
cancel_cache_purge_timer(#state{cache_purge_timer = undefined} = State) ->
    State;
cancel_cache_purge_timer(#state{cache_purge_timer = TimerRef} = State) ->
    erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]),
    State#state{cache_purge_timer = undefined}.
