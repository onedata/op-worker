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
-export([run/0]).

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
-define(ATM_SUSPENDED_WORKFLOW_EXECUTION_EXPIRATION_SECONDS, op_worker:get_env(
    atm_suspended_workflow_executions_expiration_sec, 2592000  %% 30 days
)).
-define(ATM_ENDED_WORKFLOW_EXECUTION_EXPIRATION_SECONDS, op_worker:get_env(
    atm_ended_workflow_executions_expiration_sec, 1296000  %% 15 days
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


-spec run() -> ok.
run() ->
    gen_server:call(?SERVER, gc_atm_workflow_executions).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


-spec init(Args :: term()) -> {ok, undefined, non_neg_integer()}.
init(_) ->
    process_flag(trap_exit, true),
    {ok, undefined, timer:seconds(?GC_RUN_INTERVAL_SECONDS)}.


-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), non_neg_integer()}.
handle_call(gc_atm_workflow_executions, _From, State) ->
    garbage_collect_atm_workflow_executions(),
    {reply, ok, State, timer:seconds(?GC_RUN_INTERVAL_SECONDS)};

handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, {error, wrong_request}, State}.


-spec handle_cast(Request :: term(), state()) ->
    {noreply, NewState :: state()}.
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


-spec handle_info(Info :: term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), non_neg_integer()}.
handle_info(timeout, State) ->
    garbage_collect_atm_workflow_executions(),
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
-spec garbage_collect_atm_workflow_executions() -> ok.
garbage_collect_atm_workflow_executions() ->
    ?info("Running workflow execution garbage collector..."),

    discard_expired_atm_workflow_executions(),
    purge_discarded_atm_workflow_executions().


%% @private
-spec discard_expired_atm_workflow_executions() -> ok.
discard_expired_atm_workflow_executions() ->
    case provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            lists:foreach(fun discard_expired_atm_workflow_executions/1, SpaceIds);
        {error, _} = Error ->
            ?warning(
                "Skipping expired automation workflow executions discarding procedure due to: ~p",
                [Error]
            )
    end.


%% @private
-spec discard_expired_atm_workflow_executions(od_space:id()) -> ok.
discard_expired_atm_workflow_executions(SpaceId) ->
    discard_expired_atm_workflow_executions(SpaceId, ?SUSPENDED_PHASE),
    discard_expired_atm_workflow_executions(SpaceId, ?ENDED_PHASE).


%% @private
-spec discard_expired_atm_workflow_executions(od_space:id(), ?SUSPENDED_PHASE | ?ENDED_PHASE) ->
    ok.
discard_expired_atm_workflow_executions(SpaceId, ?SUSPENDED_PHASE) ->
    discard_expired_atm_workflow_executions(SpaceId, ?SUSPENDED_PHASE, #{
        start_index => atm_workflow_executions_forest:index(
            <<>>, ?NOW_SECONDS() - ?ATM_SUSPENDED_WORKFLOW_EXECUTION_EXPIRATION_SECONDS
        ),
        limit => ?LIST_BATCH_SIZE
    });

discard_expired_atm_workflow_executions(SpaceId, ?ENDED_PHASE) ->
    discard_expired_atm_workflow_executions(SpaceId, ?ENDED_PHASE, #{
        start_index => atm_workflow_executions_forest:index(
            <<>>, ?NOW_SECONDS() - ?ATM_ENDED_WORKFLOW_EXECUTION_EXPIRATION_SECONDS
        ),
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
    ?debug("[Space: ~s] Atm gc: discarded ~B expired workflow executions", [
        SpaceId, length(DiscardedAtmWorkflowExecutionIds)
    ]),

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
            % Log only warning as next gc run will again try to discard this execution
            ?warning("Failed to discard automation workflow execution (id: ~p) due to: ~p", [
                AtmWorkflowExecutionId, Error
            ]),
            false
    end.


%% @private
-spec purge_discarded_atm_workflow_executions() -> ok.
purge_discarded_atm_workflow_executions() ->
    purge_discarded_atm_workflow_executions(<<>>).


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
    ?debug("Atm gc: purged ~B discarded workflow executions", [length(PurgedAtmWorkflowExecutionIds)]),

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
            "Failed to purge automation workflow execution (id: ~s) due to ~p:~p",
            [AtmWorkflowExecutionId, Type, Reason],
            Stacktrace
        ),
        false
    end.
