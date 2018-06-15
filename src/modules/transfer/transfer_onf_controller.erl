%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Manages on the fly transfers, which include mainly aggregating and updating
%%% transfer statistics.
%%% Such process is spawned for each space.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_onf_controller).
-author("Tomasz Lichon").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([update_statistics/2]).
%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-record(state, {
    space_id :: od_space:id(),
    cached_stats = #{} :: #{od_provider:id() => integer()},
    caching_timer :: undefined | reference()
}).

-type state() :: #state{}.


%% How long transfer stats are aggregated before updating transfer document
-define(STATS_AGGREGATION_TIME, application:get_env(
    ?APP_NAME, rtransfer_stats_aggregation_time, 2000)
).
-define(FLUSH_STATS, flush_stats).


%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Stops transfer_controller process and marks transfer as completed.
%% @end
%%-------------------------------------------------------------------
-spec update_statistics(pid(), term()) -> ok.
update_statistics(Pid, BytesPerProvider) ->
    gen_server2:cast(Pid, {update_stats, BytesPerProvider}).


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
init(SpaceId) ->
    {ok, #state{space_id = SpaceId}}.


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
handle_cast({update_stats, BytesPerProvider}, State) ->
    {noreply, cache_stats(BytesPerProvider, State)};
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
handle_info(?FLUSH_STATS, State) ->
    {noreply, flush_stats(State), hibernate};
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
    flush_stats(State),
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
%% Cache specified BytesPerProvider to avoid updating space_transfer_stats
%% document hundred of times per second and set timeout after which
%% cached stats will be flushed.
%% @end
%%--------------------------------------------------------------------
-spec cache_stats(#{od_provider:id() => integer()}, state()) -> state().
cache_stats(BytesPerProvider, #state{cached_stats = Stats} = State) ->
    NewStats = maps:fold(fun(ProviderId, Bytes, Acc) ->
        Acc#{ProviderId => Bytes + maps:get(ProviderId, Acc, 0)}
    end, Stats, BytesPerProvider),

    set_caching_timer(State#state{cached_stats = NewStats}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Flush aggregated so far transfer statistics by updating space_transfer_stats
%% document.
%% @end
%%--------------------------------------------------------------------
-spec flush_stats(state()) -> state().
flush_stats(#state{cached_stats = Stats} = State) when map_size(Stats) == 0 ->
    State;
flush_stats(#state{space_id = SpaceId, cached_stats = Stats} = State) ->
    CurrentTime = provider_logic:zone_time_seconds(),
    case space_transfer_stats:update(
        ?ON_THE_FLY_TRANSFERS_TYPE, SpaceId, Stats, CurrentTime
    ) of
        {ok, _} ->
            ok;
        {error, Error} ->
            ?error("Failed to update on the fly transfer statistics for "
            "space ~p due to ~p", [SpaceId, Error])
    end,

    erlang:garbage_collect(),

    cancel_caching_timer(State#state{cached_stats = #{}}).


-spec set_caching_timer(state()) -> state().
set_caching_timer(#state{caching_timer = undefined} = State) ->
    TimerRef = erlang:send_after(?STATS_AGGREGATION_TIME, self(), ?FLUSH_STATS),
    State#state{caching_timer = TimerRef};
set_caching_timer(State) ->
    State.


-spec cancel_caching_timer(state()) -> state().
cancel_caching_timer(#state{caching_timer = undefined} = State) ->
    State;
cancel_caching_timer(#state{caching_timer = TimerRef} = State) ->
    erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]),
    State#state{caching_timer = undefined}.
