%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record storing dir_stats_service state as part of space_support_state
%%% model.
%%%
%%% Collecting status can be changed using enable/1 and disable/1 API functions.
%%% In such a case, status is not changed directly to 'enabled'/'disabled' but is
%%% changed via intermediate states 'initializing'/'stopping'. These intermediate
%%% states are used because changing of status requires travers via all
%%% directories in space (collections_initialization) or broadcasting
%%% messages to all collectors ('stopping'). Changes from 'initializing' to 'enabled'
%%% status and 'stopping' to 'disabled' status are triggered automatically when
%%% all work is performed. Thus, collecting status changes can be depicted as
%%% follows where transitions ◄────────► are triggered automatically
%%% and other transitions are triggered using API functions:
%%%
%%%            disabled ◄─────────────────── stopping
%%%               │                              ▲
%%%               │                              │
%%%               │                              │
%%%               ▼                              │
%%%          initializing ──────────────────► enabled
%%%
%%% Additionally, state includes timestamps of collecting status changes
%%% that allow verification when historic statistics were trustworthy.
%%%
%%% NOTE: Timestamps are generated at collecting status transition.
%%%       Collecting status is changed to 'enabled' as soon as all directories
%%%       calculate statistics using their direct children. Statistics
%%%       propagation via files tree is asynchronous. Thus, timestamps
%%%       should be treated as indicative.
%%%
%%% NOTE: Restart hook is added when disabling of stats collecting in space is
%%%       requested for the first time. It is not deleted after space changes
%%%       status to 'disabled'. The hook will be deleted at provider restart as
%%%       checking hook once at cluster restart is lighter than handling
%%%       add/delete hook races.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_service_state).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API - getters
-export([
    get_state/1,
    is_active/1,
    get_status/1,
    get_extended_status/1,
    get_last_status_change_timestamp_if_in_enabled_status/1,
    get_status_change_timestamps/1
]).
%% API - cleanup
-export([clean/1]).
%% API - collecting status changes
-export([
    enable/1, disable/1,
    report_collections_initialization_finished/1, report_collectors_stopped/1
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).


% update requests can be generated only for active statuses
-type active_status() :: enabled | initializing.
% extended status includes information about incarnation - it is used
% outside this module while internally status and incarnation are stored separately
-type extended_status() :: extended_active_status() | disabled | stopping.
-type extended_active_status() :: enabled | {initializing, Incarnation :: non_neg_integer()}.

% Information about next status transition that is expected to be executed after ongoing transition is finished.
% `enable` or `disable` value is used when transition to `enabled` or `disabled` status is expected after ongoing
% transition is finished. `undefined` value is used when no transition is expected and `canceled` value is used when
% transition to `enabled` or `disabled` status was expected but another API call canceled transition before it started.
-type pending_status_transition() :: enable | disable | canceled | undefined.

-type status_change_timestamp() :: {support_parameters:dir_stats_service_status(), time:seconds()}.

-type record() :: #dir_stats_service_state{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type ctx() :: datastore:ctx().

-export_type([
    active_status/0,
    extended_status/0, extended_active_status/0,
    pending_status_transition/0, status_change_timestamp/0,
    record/0
]).

-define(CTX, #{
    model => ?MODULE,
    memory_copies => all
}).

-define(MAX_HISTORY_SIZE, 50).
-define(RESTART_HOOK_ID(SpaceId), <<"DIR_STATS_COLLECTOR_HOOK_", SpaceId/binary>>).

%%%===================================================================
%%% API - getters
%%%===================================================================


-spec get_state(od_space:id()) -> {ok, record()} | errors:error().
get_state(SpaceId) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = DirStatsServiceState}} ->
            {ok, DirStatsServiceState};

        {error, not_found} ->
            ?ERROR_NOT_FOUND
    end.


-spec is_active(od_space:id() | record()) -> boolean().
is_active(SpaceIdOrState) ->
    case get_extended_status(SpaceIdOrState) of
        enabled -> true;
        {initializing, _} -> true;
        _ -> false
    end.


-spec get_status(od_space:id() | record()) -> support_parameters:dir_stats_service_status().
get_status(SpaceIdOrState) ->
    case get_extended_status(SpaceIdOrState) of
        {initializing, _} -> initializing;
        Status -> Status
    end.


-spec get_extended_status(od_space:id() | record()) ->
    extended_status().
get_extended_status(#dir_stats_service_state{
    status = initializing,
    incarnation = Incarnation
}) ->
    {initializing, Incarnation};

get_extended_status(#dir_stats_service_state{
    status = Status
}) ->
    Status;

get_extended_status(SpaceId) ->
    case get_state(SpaceId) of
        {ok, State} ->
            get_extended_status(State);
        ?ERROR_NOT_FOUND ->
            disabled
    end.


-spec get_last_status_change_timestamp_if_in_enabled_status(od_space:id() | record()) ->
    {ok, time:seconds()} | dir_stats_collector:collecting_status_error().
get_last_status_change_timestamp_if_in_enabled_status(#dir_stats_service_state{
    status = enabled,
    status_change_timestamps = []
}) ->
    {ok, 0};

get_last_status_change_timestamp_if_in_enabled_status(#dir_stats_service_state{
    status = enabled,
    status_change_timestamps = [{enabled, Time} | _]
}) ->
    {ok, Time};

get_last_status_change_timestamp_if_in_enabled_status(#dir_stats_service_state{
    status = initializing
}) ->
    ?ERROR_DIR_STATS_NOT_READY;

get_last_status_change_timestamp_if_in_enabled_status(#dir_stats_service_state{}) ->
    ?ERROR_DIR_STATS_DISABLED_FOR_SPACE;

get_last_status_change_timestamp_if_in_enabled_status(SpaceId) ->
    case get_state(SpaceId) of
        {ok, State} ->
            get_last_status_change_timestamp_if_in_enabled_status(State);
        ?ERROR_NOT_FOUND ->
            ?ERROR_DIR_STATS_DISABLED_FOR_SPACE
    end.


-spec get_status_change_timestamps(od_space:id()) -> [status_change_timestamp()].
get_status_change_timestamps(SpaceId) ->
    case get_state(SpaceId) of
        {ok, #dir_stats_service_state{status_change_timestamps = Timestamps}} ->
            Timestamps;
        ?ERROR_NOT_FOUND ->
            []
    end.


%%%===================================================================
%%% API - init/cleanup
%%%===================================================================


-spec clean(od_space:id()) -> ok.
clean(SpaceId) ->
    ok = datastore_model:delete(?CTX, SpaceId).


%%%===================================================================
%%% API - collecting status changes
%%%===================================================================


-spec enable(od_space:id()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
enable(SpaceId) ->
    NewRecord = #dir_stats_service_state{
        status = initializing,
        incarnation = 1
    },
    Diff = fun
        (#dir_stats_service_state{
            status = disabled,
            incarnation = PrevIncarnation
        } = State) ->
            {ok, State#dir_stats_service_state{
                status = initializing,
                incarnation = PrevIncarnation + 1
            }};
        (#dir_stats_service_state{
            status = stopping,
            pending_status_transition = PendingTransition
        } = State) when PendingTransition =:= undefined ; PendingTransition =:= canceled ->
            {ok, State#dir_stats_service_state{pending_status_transition = enable}};
        (#dir_stats_service_state{pending_status_transition = disable} = State) ->
            {ok, State#dir_stats_service_state{pending_status_transition = canceled}};
        (#dir_stats_service_state{}) ->
            {error, no_action_needed}
    end,

    case update(SpaceId, Diff, NewRecord) of
        {ok, #document{value = #dir_stats_service_state{
            status = initializing,
            incarnation = Incarnation,
            pending_status_transition = PendingTransition
        }}} when PendingTransition =/= canceled ->
            run_initialization_traverse(SpaceId, Incarnation);
        {ok, _} ->
            ok;
        {error, no_action_needed} ->
            ok
    end.


-spec disable(od_space:id()) -> ok | errors:error().
disable(SpaceId) ->
    Diff = fun
        (#dir_stats_service_state{
            status = enabled
        } = State) ->
            {ok, State#dir_stats_service_state{status = stopping}};
        (#dir_stats_service_state{
            status = initializing,
            pending_status_transition = PendingTransition
        } = State) when PendingTransition =:= undefined ; PendingTransition =:= canceled ->
            {ok, State#dir_stats_service_state{pending_status_transition = disable}};
        (#dir_stats_service_state{pending_status_transition = enable} = State) ->
            {ok, State#dir_stats_service_state{pending_status_transition = canceled}};
        (#dir_stats_service_state{}) ->
            {error, no_action_needed}
    end,

    case restart_hooks:add_hook(
        ?RESTART_HOOK_ID(SpaceId), ?MODULE, report_collectors_stopped, [SpaceId], forbid_override
    ) of
        ok -> ok;
        {error, already_exists} -> ok
    end,

    case update(SpaceId, Diff) of
        {ok, #document{value = #dir_stats_service_state{
            status = stopping,
            pending_status_transition = PendingTransition
        }}} when PendingTransition =/= canceled ->
            dir_stats_collector:stop_collecting(SpaceId);
        {ok, #document{value = #dir_stats_service_state{
            status = initializing,
            incarnation = Incarnation
        }}} ->
            dir_stats_collections_initialization_traverse:cancel(SpaceId, Incarnation);
        {ok, _} ->
            ok;
        {error, no_action_needed} ->
            ok;
        ?ERROR_NOT_FOUND ->
            ?warning("Disabling space ~p without dir stats service state document", [SpaceId]);
        ?ERROR_FORBIDDEN ->
            ?ERROR_FORBIDDEN
    end.


-spec report_collections_initialization_finished(od_space:id()) -> ok.
report_collections_initialization_finished(SpaceId) ->
    Diff = fun
        (#dir_stats_service_state{
            status = initializing,
            pending_status_transition = disable
        } = State) ->
            {ok, State#dir_stats_service_state{
                status = stopping,
                pending_status_transition = undefined
            }};
        (#dir_stats_service_state{status = initializing} = State) ->
            {ok, State#dir_stats_service_state{
                status = enabled,
                pending_status_transition = undefined
            }};
        (#dir_stats_service_state{status = Status}) ->
            {error, {wrong_status, Status}}
    end,

    case update(SpaceId, Diff) of
        {ok, #document{value = #dir_stats_service_state{status = enabled}}} ->
            ok;
        {ok, #document{value = #dir_stats_service_state{status = stopping}}} ->
            dir_stats_collector:stop_collecting(SpaceId);
        {error, {wrong_status, WrongStatus}} ->
            ?warning("Reporting space ~p enabling finished when space has status ~p", [SpaceId, WrongStatus]);
        ?ERROR_NOT_FOUND ->
            ?warning("Reporting space ~p enabling finished when space has no dir stats service state document", [SpaceId])
    end.


-spec report_collectors_stopped(od_space:id()) -> ok.
report_collectors_stopped(SpaceId) ->
    Diff = fun
        (#dir_stats_service_state{
            status = stopping,
            pending_status_transition = enable,
            incarnation = Incarnation
        } = State) ->
            {ok, State#dir_stats_service_state{
                status = initializing,
                incarnation = Incarnation + 1,
                pending_status_transition = undefined
            }};
        (#dir_stats_service_state{status = stopping} = State) ->
            {ok, State#dir_stats_service_state{
                status = disabled,
                pending_status_transition = undefined
            }};
        (#dir_stats_service_state{status = Status}) ->
            {error, {wrong_status, Status}}
    end,

    case update(SpaceId, Diff) of
        {ok, #document{value = #dir_stats_service_state{status = disabled}}} ->
            ok;
        {ok, #document{value = #dir_stats_service_state{
            status = initializing,
            incarnation = Incarnation
        }}} ->
            run_initialization_traverse(SpaceId, Incarnation),
            ok;
        % Log errors on debug as they can appear at node restart
        {error, {wrong_status, WrongStatus}} ->
            ?debug("Reporting space ~p disabling finished when space has status ~p", [SpaceId, WrongStatus]);
        ?ERROR_NOT_FOUND ->
            ?debug("Reporting space ~p disabling finished when space has no dir stats service state document", [SpaceId])
    end.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {status, atom},
        {incarnation, integer},
        {pending_status_transition, atom},
        {collecting_status_change_timestamps, [{atom, integer}]}
    ]}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec update(od_space:id(), diff()) -> {ok, doc()} | {error, term()}.
update(SpaceId, Diff) ->
    datastore_model:update(?CTX, SpaceId, diff_fun_with_timestamp_update(Diff)).


-spec update(od_space:id(), diff(), record()) -> {ok, doc()} | {error, term()}.
update(SpaceId, Diff, Default) ->
    datastore_model:update(?CTX, SpaceId, diff_fun_with_timestamp_update(Diff), Default).


-spec diff_fun_with_timestamp_update(diff()) -> diff().
diff_fun_with_timestamp_update(Diff) ->
    fun(#dir_stats_service_state{status = Status} = State) ->
        case Diff(State) of
            {ok, #dir_stats_service_state{
                status = NewStatus,
                status_change_timestamps = Timestamps
            } = NewState} when NewStatus =/= Status ->
                {ok, NewState#dir_stats_service_state{
                    status_change_timestamps = update_timestamps(NewStatus, Timestamps)
                }};
            Other ->
                Other
        end
    end.


-spec update_timestamps(support_parameters:dir_stats_service_status(), [status_change_timestamp()]) ->
    [status_change_timestamp()].
update_timestamps(NewStatus, Timestamps) ->
    NewTimestamps = [{NewStatus, global_clock:timestamp_seconds()} | Timestamps],
    case length(NewTimestamps) > ?MAX_HISTORY_SIZE of
        true -> lists:sublist(NewTimestamps, ?MAX_HISTORY_SIZE);
        false -> NewTimestamps
    end.


-spec run_initialization_traverse(file_id:space_id(), non_neg_integer()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
run_initialization_traverse(SpaceId, Incarnation) ->
    case dir_stats_collections_initialization_traverse:run(SpaceId, Incarnation) of
        ok ->
            ok;
        ?ERROR_INTERNAL_SERVER_ERROR ->
            Diff = fun
                (State = #dir_stats_service_state{status = initializing}) ->
                    {ok, State#dir_stats_service_state{
                        status = disabled,
                        pending_status_transition = undefined
                    }};

                (#dir_stats_service_state{status = Status}) ->
                    {error, {wrong_status, Status}}
            end,

            case update(SpaceId, Diff) of
                {ok, #document{value = #dir_stats_service_state{status = disabled}}} ->
                    ok;
                {error, {wrong_status, WrongStatus}} ->
                    ?warning("Reporting space ~p initialization traverse failure when space has status ~p",
                        [SpaceId, WrongStatus]);
                ?ERROR_NOT_FOUND ->
                    ?warning("Reporting space ~p initialization traverse failure when "
                        "space has no dir stats service state document", [SpaceId])
            end,

            ?ERROR_INTERNAL_SERVER_ERROR
    end.
