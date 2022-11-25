%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for:
%%% - discarding expired atm workflow executions
%%% - purging discarded atm workflow executions
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_garbage_collector).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("modules/automation/atm_execution.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/http/codes.hrl").

%% API
-export([id/0, spec/0, start_link/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-type state() :: undefined.


-define(GC_RUN_INTERVAL_SECONDS, op_worker:get_env(
    atm_workflow_execution_garbage_collector_run_interval_sec, 3600  %% 1 hour
)).
-define(ATM_SUSPENDED_WORKFLOW_EXECUTION_EXPIRATION_INTERVAL_SECONDS, op_worker:get_env(
    atm_suspended_workflow_executions_expiration_interval_sec, 2592000  %% 30 days
)).
-define(ATM_ENDED_WORKFLOW_EXECUTION_EXPIRATION_INTERVAL_SECONDS, op_worker:get_env(
    atm_ended_workflow_executions_expiration_interval_sec, 1296000  %% 15 days
)).
-define(NOW_SECONDS(), global_clock:timestamp_seconds()).

-define(LIST_BATCH_SIZE, 1000).

-define(SERVER, {global, ?MODULE}).


%%%===================================================================
%%% API
%%%===================================================================


-spec id() -> atom().
id() -> ?MODULE.


-spec spec() -> supervisor:child_spec().
spec() ->
    #{
        id => id(),
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [?MODULE]
    }.


-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link(?SERVER, ?MODULE, [], []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


-spec init(Args :: term()) -> {ok, undefined, non_neg_integer()}.
init(_) ->
    process_flag(trap_exit, true),
    {ok, undefined, timer:seconds(?GC_RUN_INTERVAL_SECONDS)}.


-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), NewState :: state()}.
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, {error, wrong_request}, State}.


-spec handle_cast(Request :: term(), state()) ->
    {noreply, NewState :: state()}.
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


-spec handle_info(Info :: term(), state()) ->
    {noreply, NewState :: state(), non_neg_integer()}.
handle_info(timeout, State) ->
    ?info("Starting garbage atm_workflow_execution collecting procedure..."),

    discard_expired_atm_workflow_executions(),
    purge_discarded_atm_workflow_executions(),

    ?info("Garbage atm_workflow_execution collecting procedure finished succesfully."),

    {noreply, State, timer:seconds(?GC_RUN_INTERVAL_SECONDS)};

handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State}.


-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    state()) -> term().
terminate(_Reason, _State) ->
    ok.


-spec code_change(OldVsn :: term() | {down, term()}, state(), Extra :: term()) ->
    {ok, NewState :: state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec discard_expired_atm_workflow_executions() -> ok.
discard_expired_atm_workflow_executions() ->
    case provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            ?info("Starting expired atm_workflow_execution discarding procedure..."),

            lists:foreach(fun discard_expired_atm_workflow_executions/1, SpaceIds),

            ?info("Expired atm_workflow_execution discarding procedure finished succesfully.");

        {error, _} = Error ->
            ?warning(
                "Skipping expired atm_workflow_execution discarding procedure due to: ~p",
                [Error]
            )
    end.


%% @private
-spec discard_expired_atm_workflow_executions(od_space:id()) -> ok.
discard_expired_atm_workflow_executions(SpaceId) ->
    ?info("[Space: ~s] Starting expired atm_workflow_execution discarding procedure...", [
        SpaceId
    ]),

    discard_expired_atm_workflow_executions(SpaceId, ?SUSPENDED_PHASE),
    discard_expired_atm_workflow_executions(SpaceId, ?ENDED_PHASE),

    ?info("[Space: ~s] Expired atm_workflow_execution discarding procedure finished succesfully.", [
        SpaceId
    ]).


%% @private
-spec discard_expired_atm_workflow_executions(od_space:id(), ?SUSPENDED_PHASE | ?ENDED_PHASE) ->
    ok.
discard_expired_atm_workflow_executions(SpaceId, ?SUSPENDED_PHASE) ->
    ExpirationTime = ?NOW_SECONDS() - ?ATM_SUSPENDED_WORKFLOW_EXECUTION_EXPIRATION_INTERVAL_SECONDS,

    discard_expired_atm_workflow_executions(SpaceId, ?SUSPENDED_PHASE, #{
        start_index => atm_workflow_executions_forest:index(<<>>, ExpirationTime),
        limit => ?LIST_BATCH_SIZE
    });

discard_expired_atm_workflow_executions(SpaceId, ?ENDED_PHASE) ->
    ExpirationTime = ?NOW_SECONDS() - ?ATM_ENDED_WORKFLOW_EXECUTION_EXPIRATION_INTERVAL_SECONDS,

    discard_expired_atm_workflow_executions(SpaceId, ?ENDED_PHASE, #{
        start_index => atm_workflow_executions_forest:index(<<>>, ExpirationTime),
        limit => ?LIST_BATCH_SIZE
    }).


%% @private
-spec discard_expired_atm_workflow_executions(
    od_space:id(),
    ?SUSPENDED_PHASE | ?ENDED_PHASE,
    atm_workflow_executions_forest:listing_opts()
) ->
    ok.
discard_expired_atm_workflow_executions(SpaceId, Phase, ListingOpts = #{start_index := StartIndex}) ->
    {ok, AtmWorkflowExecutionBasicEntries, IsLast} = atm_workflow_execution_api:list(
        SpaceId, Phase, basic, ListingOpts
    ),

    {LastEntryIndex, DiscardedAtmWorkflowExecutionIds} = lists:foldl(
        fun({Index, AtmWorkflowExecutionId}, {_, Acc}) ->
            {Index, case discard_atm_workflow_execution(AtmWorkflowExecutionId) of
                true -> [AtmWorkflowExecutionId | Acc];
                false -> Acc
            end}
        end,
        {StartIndex, []},
        AtmWorkflowExecutionBasicEntries
    ),
    ?debug("Discarded atm ~p workflow executions: ~p", [Phase, DiscardedAtmWorkflowExecutionIds]),

    case IsLast of
        true ->
            ok;
        false ->
            discard_expired_atm_workflow_executions(SpaceId, Phase, ListingOpts#{
                start_index => LastEntryIndex, offset => 1
            })
    end.


%% @private
-spec discard_atm_workflow_execution(atm_workflow_execution:id()) -> boolean().
discard_atm_workflow_execution(AtmWorkflowExecutionId) ->
    case atm_workflow_execution_api:discard(AtmWorkflowExecutionId) of
        ok ->
            true;
        {error, _} = Error ->
            ?warning("Failed to discard atm workflow execution (id: ~p) due to: ~p", [
                AtmWorkflowExecutionId, Error
            ]),
            false
    end.


%% @private
-spec purge_discarded_atm_workflow_executions() -> ok.
purge_discarded_atm_workflow_executions() ->
    ?info("Starting discarded atm_workflow_execution purging procedure..."),

    purge_discarded_atm_workflow_executions(<<>>),

    ?info("Discarded atm_workflow_execution purging procedure finished succesfully.").


%% @private
-spec purge_discarded_atm_workflow_executions(atm_workflow_execution:id()) -> ok.
purge_discarded_atm_workflow_executions(StartAtmWorkflowExecutionId) ->
    DiscardedAtmWorkflowExecutionIds = atm_discarded_workflow_executions:list(
        StartAtmWorkflowExecutionId, ?LIST_BATCH_SIZE
    ),

    {LastAtmWorkflowExecutionId, PurgedAtmWorkflowExecutionIds} = lists:foldl(
        fun(AtmWorkflowExecutionId, {_, Acc}) ->
            {AtmWorkflowExecutionId, case purge_atm_workflow_execution(AtmWorkflowExecutionId) of
                true -> [AtmWorkflowExecutionId | Acc];
                false -> Acc
            end}
        end,
        {StartAtmWorkflowExecutionId, []},
        DiscardedAtmWorkflowExecutionIds
    ),
    ?debug("Purged atm workflow executions: ~p", [PurgedAtmWorkflowExecutionIds]),

    case length(DiscardedAtmWorkflowExecutionIds) < ?LIST_BATCH_SIZE of
        true ->
            ok;
        false ->
            purge_discarded_atm_workflow_executions(LastAtmWorkflowExecutionId)
    end.


%% @private
-spec purge_atm_workflow_execution(atm_workflow_execution:id()) -> boolean().
purge_atm_workflow_execution(AtmWorkflowExecutionId) ->
    try
        atm_workflow_execution_factory:delete_insecure(AtmWorkflowExecutionId),
        true
    catch Type:Reason:Stacktrace ->
        ?error_stacktrace(
            "Failed to purge atm workflow execution (id: ~s) due to ~p:~p",
            [AtmWorkflowExecutionId, Type, Reason],
            Stacktrace
        ),
        false
    end.
