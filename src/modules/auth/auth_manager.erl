%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Manages ets cache for user auth. That includes periodic checks of
%%% cache size and clearing it if it exceeds max size.
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
-export([verify/1, invalidate/1]).
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

-define(CACHE_ITEM_DEFAULT_TTL, 10).                %% in seconds
-define(NOW, time_utils:system_time_seconds()).


-record(cache_item, {
    key :: #token_auth{},
    value :: {ok, aai:auth(), ValidUntil :: undefined | timestamp()} | errors:error(),
    expiration :: timestamp()
}).

-type state() :: undefined.
-type timestamp() :: non_neg_integer().  %% in s


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Verifies identity of subject identified by specified #token_auth{}
%% and returns time this auth will be valid until. Nevertheless this
%% auth should be confirmed periodically as tokens can be revoked.
%% @end
%%--------------------------------------------------------------------
-spec verify(#token_auth{}) ->
    {ok, aai:auth(), ValidUntil :: undefined | timestamp()} | errors:error().
verify(#token_auth{} = TokenAuth) ->
    Now = ?NOW,
    case ets:lookup(?CACHE_NAME, TokenAuth) of
        [#cache_item{value = Value, expiration = Expiration}] when Now < Expiration ->
            Value;
        _ ->
            try
                {CacheValue, CacheItemTTL} = case fetch(TokenAuth) of
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
                    key = TokenAuth,
                    value = CacheValue,
                    expiration = Now + CacheItemTTL
                }),
                CacheValue
            catch _:Reason ->
                ?error_stacktrace("Cannot establish user identity due to: ~p", [
                    Reason
                ]),
                ?ERROR_UNAUTHORIZED
            end
    end.


-spec invalidate(#token_auth{}) -> ok.
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
        set, public, named_table, {keypos, #cache_item.key}
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
-spec fetch(#token_auth{}) ->
    {ok, aai:auth(), TTL :: undefined | timestamp()} | errors:error().
fetch(TokenAuth) ->
    case token_logic:verify_access_token(TokenAuth) of
        {ok, #auth{subject = ?SUB(user, UserId)}, _TTL} = Result ->
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
