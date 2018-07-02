%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Aggregates transfer statistics for on the fly transfers for various spaces.
%%% This gen_server is started for every node and registered locally.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_onf_stats_aggregator).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    start_link/0,
    update_statistics/2,
    spec/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-type stats() :: #{od_provider:id() => integer()}.

-record(state, {
    cached_stats = #{} :: #{od_space:id() => stats()},
    caching_timers = #{} :: #{od_space:id() => reference()}
}).

-type state() :: #state{}.


%% How long transfer stats are aggregated before updating transfer document
-define(STATS_AGGREGATION_TIME, application:get_env(
    ?APP_NAME, onf_transfer_stats_aggregation_time, 4500)
).
-define(FLUSH_STATS, flush_stats).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts the aggregator for onf transfer stats.
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).


%%-------------------------------------------------------------------
%% @doc
%% Stops transfer_controller process and marks transfer as completed.
%% @end
%%-------------------------------------------------------------------
-spec update_statistics(od_space:id(), stats()) -> ok.
update_statistics(SpaceId, BytesPerProvider) ->
    gen_server2:cast(?MODULE, {update_stats, SpaceId, BytesPerProvider}).


%%-------------------------------------------------------------------
%% @doc
%% Returns child spec for transfer_onf_stats_aggregator to attach it
%% to supervision.
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
    {ok, State :: state()} | {ok, State :: state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init(_Args) ->
    {ok, #state{}}.


%%--------------------------------------------------------------------
%% @private
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
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, wrong_request, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast({update_stats, SpaceId, BytesPerProvider}, State) ->
    {noreply, cache_stats(SpaceId, BytesPerProvider, State)};
handle_cast(Request, State) ->
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
handle_info({?FLUSH_STATS, SpaceId}, State) ->
    {noreply, flush_stats(SpaceId, State)};
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
    State :: state()) -> term().
terminate(_Reason, State) ->
    flush_all_stats(State),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Caches given BytesPerProvider (transfer statistics for on the fly transfers
%% for specified space) to avoid updating space_transfer_stats document
%% hundred of times per second and sets timeout after which
%% cached stats will be flushed.
%% @end
%%--------------------------------------------------------------------
-spec cache_stats(od_space:id(), #{od_provider:id() => integer()}, state()) ->
    state().
cache_stats(SpaceId, BytesPerProvider, #state{cached_stats = Stats} = State) ->
    SpaceStats = maps:get(SpaceId, Stats, #{}),
    NewSpaceStats = maps:fold(fun(ProviderId, Bytes, Acc) ->
        Acc#{ProviderId => Bytes + maps:get(ProviderId, Acc, 0)}
    end, SpaceStats, BytesPerProvider),

    NewStats = Stats#{SpaceId => NewSpaceStats},
    set_caching_timer(SpaceId, State#state{cached_stats = NewStats}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Flushes aggregated so far on the fly transfer statistics for specified space.
%% @end
%%--------------------------------------------------------------------
-spec flush_stats(od_space:id(), state()) -> state().
flush_stats(SpaceId, #state{cached_stats = StatsPerSpace} = State) ->
    case maps:take(SpaceId, StatsPerSpace) of
        error ->
            State;
        {Stats, RestStatsPerSpace} ->
            CurrentTime = provider_logic:zone_time_seconds(),
            case space_transfer_stats:update(
                ?ON_THE_FLY_TRANSFERS_TYPE, SpaceId, Stats, CurrentTime
            ) of
                ok ->
                    ok;
                {error, Error} ->
                    ?error(
                        "Failed to update on the fly transfer statistics for "
                        "space ~p due to ~p", [
                            SpaceId, Error
                        ]
                    )
            end,

            NewState = cancel_caching_timer(SpaceId, State#state{
                cached_stats = RestStatsPerSpace
            }),
            erlang:garbage_collect(),
            NewState
    end.


-spec flush_all_stats(state()) -> state().
flush_all_stats(#state{cached_stats = StatsPerSpace} = State) ->
    lists:foldl(fun(SpaceId, Acc) ->
        flush_stats(SpaceId, Acc)
    end, State, maps:keys(StatsPerSpace)).


-spec set_caching_timer(od_space:id(), state()) -> state().
set_caching_timer(SpaceId, #state{caching_timers = Timers} = State) ->
    TimerRef = case maps:get(SpaceId, Timers, undefined) of
        undefined ->
            Msg = {?FLUSH_STATS, SpaceId},
            erlang:send_after(?STATS_AGGREGATION_TIME, self(), Msg);
        OldTimerRef ->
            OldTimerRef
    end,
    State#state{caching_timers = Timers#{SpaceId => TimerRef}}.


-spec cancel_caching_timer(od_space:id(), state()) -> state().
cancel_caching_timer(SpaceId, #state{caching_timers = Timers} = State) ->
    NewTimers = case maps:take(SpaceId, Timers) of
        {TimerRef, RestTimers} ->
            erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]),
            RestTimers;
        error ->
            Timers
    end,
    State#state{caching_timers = NewTimers}.
