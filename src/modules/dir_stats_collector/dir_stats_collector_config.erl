%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing dir_stats_collector configuration for each space.
%%% For all space supports granted by providers in versions
%%% 21.02.0-alpha25 or newer, collecting status is determined by the value
%%% of dir_stats_collecting_status_for_new_spaces environment variable
%%% at the moment of space support granting by provider. For other spaces
%%% collecting status is disabled.
%%%
%%% Collecting status can be changed using enable/1 and
%%% disable/1 API functions. In such a case, status is not changed
%%% directly to enabled/disabled but is changed via intermediate states
%%% collections_initialization/collectors_stopping. These intermediate
%%% states are used because changing of status requires travers via all
%%% directories in space (collections_initialization) or broadcasting
%%% messages to all collectors (collectors_stopping). Changes from
%%% collections_initialization to enabled status and collectors_stopping
%%% to disabled status are triggered automatically when all work
%%% is performed. Thus, collecting status changes can be depicted as
%%% follows where transitions ◄────────► are triggered automatically
%%% and other transitions are triggered using API functions:
%%%
%%%            disabled ◄────────────── collectors_stopping
%%%               │                              ▲
%%%               │                              │
%%%               │                              │
%%%               ▼                              │
%%%       collections_initialization ────────► enabled
%%%
%%% Additionally, config includes timestamps of collecting status changes
%%% that allow verification when historic statistics were trustworthy.
%%%
%%% NOTE: Timestamps are generated at collecting status transition.
%%%       Collecting status is changed to enabled as soon as all directories
%%%       calculate statistics using their direct children. Statistics
%%%       propagation via files tree is asynchronous. Thus, timestamps
%%%       should be treated as indicative.
%%%
%%% NOTE: Restart hook is added when disabling of stats collecting in space is requested for the first time.
%%%       It is not deleted after space changes status to disabled. The hook will be deleted at provider restart as
%%%       checking hook once at cluster restart is lighter than handling add/delete hook races.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collector_config).
-author("Michal Wrzeszcz").

-behaviour(persistent_record).

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API - getters
-export([is_collecting_active/1, get_extended_collecting_status/1,
    get_last_status_change_timestamp_if_in_enabled_status/1, get_collecting_status_change_timestamps/1]).
%% API - collecting status changes
-export([enable/1, disable/1,
    report_collections_initialization_finished/1, report_collectors_stopped/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type collecting_status() :: active_collecting_status() | disabled | collectors_stopping.
% update requests can be generated only for active statuses
-type active_collecting_status() :: enabled | collections_initialization.
% extended status includes information about incarnation - it is used
% outside this module while internally status and incarnation are stored separately
-type extended_collecting_status() :: extended_active_collecting_status() | disabled | collectors_stopping.
-type extended_active_collecting_status() :: enabled |
    {collections_initialization, Incarnation :: non_neg_integer()}.

% Information about next status transition that is expected to be executed after ongoing transition is finished.
% `enable` or `disable` value is used when transition to `enabled` or `disabled` status is expected after ongoing
% transition is finished. `undefined` value is used when no transition is expected and `canceled` value is used when
% transition to `enabled` or `disabled` status was expected but another API call canceled transition before it started.
-type pending_status_transition() :: enable | disable | canceled | undefined.

-type status_change_timestamp() :: {collecting_status(), time:seconds()}.

-type record() :: #dir_stats_collector_config{}.
-type diff_fun() :: fun((record()) -> {ok, record()} | {error, term()}).

-export_type([collecting_status/0, active_collecting_status/0,
    extended_collecting_status/0, extended_active_collecting_status/0,
    pending_status_transition/0, status_change_timestamp/0,
    record/0
]).


-define(MAX_HISTORY_SIZE, 50).
-define(RESTART_HOOK_ID(SpaceId), <<"DIR_STATS_COLLECTOR_HOOK_", SpaceId/binary>>).

%%%===================================================================
%%% API - getters
%%%===================================================================

-spec is_collecting_active(od_space:id()) -> boolean().
is_collecting_active(SpaceId) ->
    case get_extended_collecting_status(SpaceId) of
        enabled -> true;
        {collections_initialization, _} -> true;
        _ -> false
    end.


-spec get_extended_collecting_status(od_space:id()) -> extended_collecting_status().
get_extended_collecting_status(SpaceId) ->
    case get_config(SpaceId) of
        {ok, #dir_stats_collector_config{
            collecting_status = collections_initialization,
            incarnation = Incarnation
        }} ->
            {collections_initialization, Incarnation};
        {ok, #dir_stats_collector_config{collecting_status = Status}} ->
            Status;
        ?ERROR_NOT_FOUND ->
            disabled
    end.


-spec get_last_status_change_timestamp_if_in_enabled_status(od_space:id()) ->
    {ok, time:seconds()} | dir_stats_collector:collecting_status_error().
get_last_status_change_timestamp_if_in_enabled_status(SpaceId) ->
    case get_config(SpaceId) of
        {ok, #dir_stats_collector_config{
            collecting_status = enabled,
            collecting_status_change_timestamps = []
        }} ->
            {ok, 0};
        {ok, #dir_stats_collector_config{
            collecting_status = enabled,
            collecting_status_change_timestamps = [{enabled, Time} | _]
        }} ->
            {ok, Time};
        {ok, #dir_stats_collector_config{collecting_status = collections_initialization}} ->
            ?ERROR_DIR_STATS_NOT_READY;
        {ok, _} ->
            ?ERROR_DIR_STATS_DISABLED_FOR_SPACE;
        ?ERROR_NOT_FOUND ->
            ?ERROR_DIR_STATS_DISABLED_FOR_SPACE
    end.


-spec get_collecting_status_change_timestamps(od_space:id()) -> [status_change_timestamp()].
get_collecting_status_change_timestamps(SpaceId) ->
    case get_config(SpaceId) of
        {ok, #dir_stats_collector_config{collecting_status_change_timestamps = Timestamps}} ->
            Timestamps;
        ?ERROR_NOT_FOUND ->
            []
    end.


%%%===================================================================
%%% API - collecting status changes
%%%===================================================================

-spec enable(od_space:id()) -> ok | ?ERROR_INTERNAL_SERVER_ERROR.
enable(SpaceId) ->
    Diff = fun
        (#dir_stats_collector_config{
            collecting_status = disabled, 
            incarnation = PrevIncarnation
        } = Config) ->
            {ok, Config#dir_stats_collector_config{
                collecting_status = collections_initialization,
                incarnation = PrevIncarnation + 1
            }};
        (#dir_stats_collector_config{
            collecting_status = collectors_stopping, 
            pending_status_transition = PendingTransition
        } = Config) when PendingTransition =:= undefined ; PendingTransition =:= canceled ->
            {ok, Config#dir_stats_collector_config{pending_status_transition = enable}};
        (#dir_stats_collector_config{pending_status_transition = disable} = Config) ->
            {ok, Config#dir_stats_collector_config{pending_status_transition = canceled}};
        (#dir_stats_collector_config{}) ->
            {error, no_action_needed}
    end,

    case update_config(SpaceId, Diff) of
        {ok, #dir_stats_collector_config{
            collecting_status = collections_initialization,
            incarnation = Incarnation,
            pending_status_transition = PendingTransition
        }} when PendingTransition =/= canceled ->
            run_initialization_traverse(SpaceId, Incarnation);
        {ok, _} ->
            ok;
        {error, no_action_needed} ->
            ok
    end.


-spec disable(od_space:id()) -> ok.
disable(SpaceId) ->
    ConfigDiff = fun
        (#dir_stats_collector_config{
            collecting_status = enabled
        } = Config) ->
            {ok, Config#dir_stats_collector_config{collecting_status = collectors_stopping}};
        (#dir_stats_collector_config{
            collecting_status = collections_initialization,
            pending_status_transition = PendingTransition
        } = Config) when PendingTransition =:= undefined ; PendingTransition =:= canceled ->
            {ok, Config#dir_stats_collector_config{pending_status_transition = disable}};
        (#dir_stats_collector_config{pending_status_transition = enable} = Config) ->
            {ok, Config#dir_stats_collector_config{pending_status_transition = canceled}};
        (#dir_stats_collector_config{}) ->
            {error, no_action_needed}
    end,
    Condition = fun(#space_support_state{accounting_status = Status}) -> Status == disabled end,

    case restart_hooks:add_hook(
        ?RESTART_HOOK_ID(SpaceId), ?MODULE, report_collectors_stopped, [SpaceId], forbid_override
    ) of
        ok -> ok;
        {error, already_exists} -> ok
    end,

    case update_config_if_allowed(SpaceId, ConfigDiff, Condition) of
        {ok, #dir_stats_collector_config{
            collecting_status = collectors_stopping,
            pending_status_transition = PendingTransition
        }} when PendingTransition =/= canceled ->
            dir_stats_collector:stop_collecting(SpaceId);
        {ok, #dir_stats_collector_config{
            collecting_status = collections_initialization,
            incarnation = Incarnation
        }} ->
            dir_stats_collections_initialization_traverse:cancel(SpaceId, Incarnation);
        {ok, _} ->
            ok;
        {error, no_action_needed} ->
            ok;
        ?ERROR_NOT_FOUND ->
            ?warning("Disabling space ~p without collector config document", [SpaceId])
    end.


-spec report_collections_initialization_finished(od_space:id()) -> ok.
report_collections_initialization_finished(SpaceId) ->
    Diff = fun
        (#dir_stats_collector_config{
            collecting_status = collections_initialization,
            pending_status_transition = disable
        } = Config) ->
            {ok, Config#dir_stats_collector_config{
                collecting_status = collectors_stopping,
                pending_status_transition = undefined
            }};
        (#dir_stats_collector_config{collecting_status = collections_initialization} = Config) ->
            {ok, Config#dir_stats_collector_config{
                collecting_status = enabled,
                pending_status_transition = undefined
            }};
        (#dir_stats_collector_config{collecting_status = Status}) ->
            {error, {wrong_status, Status}}
    end,

    case update_config(SpaceId, Diff) of
        {ok, #dir_stats_collector_config{collecting_status = enabled}} ->
            ok;
        {ok, #dir_stats_collector_config{collecting_status = collectors_stopping}} ->
            dir_stats_collector:stop_collecting(SpaceId);
        {error, {wrong_status, WrongStatus}} ->
            ?warning("Reporting space ~p enabling finished when space has status ~p", [SpaceId, WrongStatus]);
        ?ERROR_NOT_FOUND ->
            ?warning("Reporting space ~p enabling finished when space has no collector config document", [SpaceId])
    end.


-spec report_collectors_stopped(od_space:id()) -> ok.
report_collectors_stopped(SpaceId) ->
    Diff = fun
        (#dir_stats_collector_config{
            collecting_status = collectors_stopping,
            pending_status_transition = enable,
            incarnation = Incarnation
        } = Config) ->
            {ok, Config#dir_stats_collector_config{
                collecting_status = collections_initialization,
                incarnation = Incarnation + 1,
                pending_status_transition = undefined
            }};
        (#dir_stats_collector_config{collecting_status = collectors_stopping} = Config) ->
            {ok, Config#dir_stats_collector_config{
                collecting_status = disabled,
                pending_status_transition = undefined
            }};
        (#dir_stats_collector_config{collecting_status = Status}) ->
            {error, {wrong_status, Status}}
    end,

    case update_config(SpaceId, Diff) of
        {ok, #dir_stats_collector_config{collecting_status = disabled}} ->
            ok;
        {ok, #dir_stats_collector_config{
            collecting_status = collections_initialization,
            incarnation = Incarnation
        }} ->
            run_initialization_traverse(SpaceId, Incarnation),
            ok;
        % Log errors on debug as they can appear at node restart
        {error, {wrong_status, WrongStatus}} ->
            ?debug("Reporting space ~p disabling finished when space has status ~p", [SpaceId, WrongStatus]);
        ?ERROR_NOT_FOUND ->
            ?debug("Reporting space ~p disabling finished when space has no collector config document", [SpaceId])
    end.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_map().
db_encode(#dir_stats_collector_config{
    collecting_status = CollectingStatus,
    incarnation = Incarnation,
    pending_status_transition = PendingStatusTransition,
    collecting_status_change_timestamps = CollectingStatusChangeTimestamps
}, _NestedRecordEncoder) ->
    #{
        <<"collectingStatus">> => atom_to_binary(CollectingStatus, utf8),
        <<"incarnation">> => Incarnation,
        <<"pendingStatusTransition">> => atom_to_binary(PendingStatusTransition, utf8),
        <<"collectingStatusChangeTimestamps">> => lists:map(fun({CollectingStatus, TimestampSec}) ->
            [atom_to_binary(CollectingStatus, utf8), TimestampSec]
        end, CollectingStatusChangeTimestamps)
    }.


-spec db_decode(json_utils:json_map(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"collectingStatus">> := CollectingStatusBin,
    <<"incarnation">> := Incarnation,
    <<"pendingStatusTransition">> := PendingStatusTransitionBin,
    <<"collectingStatusChangeTimestamps">> := EncodedCollectingStatusChangeTimestamps
}, _NestedRecordDecoder) ->
    #dir_stats_collector_config{
        collecting_status = binary_to_atom(CollectingStatusBin, utf8),
        incarnation = Incarnation,
        pending_status_transition = binary_to_atom(PendingStatusTransitionBin, utf8),
        collecting_status_change_timestamps = lists:map(fun([CollectingStatusBin, TimestampSec]) ->
            {binary_to_atom(CollectingStatusBin, utf8), TimestampSec}
        end, EncodedCollectingStatusChangeTimestamps)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_config(od_space:id()) -> {ok, record()} | errors:error().
get_config(SpaceId) ->
    case space_support_state:get(SpaceId) of
        {ok, #document{value = #space_support_state{
            dir_stats_collector_config = DirStatsCollectorConfig
        }}} ->
            {ok, DirStatsCollectorConfig};

        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND
    end.


%% @private
-spec update_config(od_space:id(), diff_fun()) -> {ok, record()} | {error, term()}.
update_config(SpaceId, ConfigDiff) ->
    update_config_if_allowed(SpaceId, ConfigDiff, fun(_) -> true end).


%% @private
-spec update_config_if_allowed(
    od_space:id(),
    diff_fun(),
    fun((space_support_state:record()) -> boolean())
) ->
    {ok, record()} | {error, term()}.
update_config_if_allowed(SpaceId, ConfigDiff, Condition) ->
    ConfigDiffWithTimestampUpdate = diff_fun_with_timestamp_update(ConfigDiff),

    SpaceSupportStateDiff = fun(SpaceSupportState = #space_support_state{
        dir_stats_collector_config = Config
    }) ->
        case Condition(SpaceSupportState) of
            true ->
                case ConfigDiffWithTimestampUpdate(Config) of
                    {ok, NewConfig} ->
                        {ok, SpaceSupportState#space_support_state{
                            dir_stats_collector_config = NewConfig
                        }};
                    {error, _} = Error ->
                        Error
                end;
            false ->
                ?ERROR_FORBIDDEN
        end
    end,

    case space_support_state:update(SpaceId, SpaceSupportStateDiff) of
        {ok, #document{value = #space_support_state{dir_stats_collector_config = Config}}} ->
            {ok, Config};
        {error, _} = Error ->
            Error
    end.


-spec diff_fun_with_timestamp_update(diff_fun()) -> diff_fun().
diff_fun_with_timestamp_update(Diff) ->
    fun(#dir_stats_collector_config{collecting_status = Status} = Config) ->
        case Diff(Config) of
            {ok, #dir_stats_collector_config{
                collecting_status = NewStatus,
                collecting_status_change_timestamps = Timestamps
            } = NewConfig} when NewStatus =/= Status ->
                {ok, NewConfig#dir_stats_collector_config{
                    collecting_status_change_timestamps = update_timestamps(NewStatus, Timestamps)
                }};
            Other ->
                Other
        end
    end.


-spec update_timestamps(collecting_status(), [status_change_timestamp()]) -> [status_change_timestamp()].
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
                (SpaceSupportState = #space_support_state{
                    dir_stats_collector_config = Config = #dir_stats_collector_config{
                        collecting_status = collections_initialization
                    }
                }) ->
                    {ok, SpaceSupportState#space_support_state{
                        accounting_status = disabled,
                        dir_stats_collector_config = Config#dir_stats_collector_config{
                            collecting_status = disabled,
                            pending_status_transition = undefined
                        }
                    }};

                (#space_support_state{dir_stats_collector_config = #dir_stats_collector_config{
                    collecting_status = Status
                }}) ->
                    {error, {wrong_status, Status}}
            end,

            case space_support_state:update(SpaceId, Diff) of
                {ok, #document{value = #space_support_state{
                    dir_stats_collector_config = #dir_stats_collector_config{
                        collecting_status = disabled
                    }
                }}} ->
                    ok;
                {error, {wrong_status, WrongStatus}} ->
                    ?warning("Reporting space ~p initialization traverse failure when space has status ~p",
                        [SpaceId, WrongStatus]);
                ?ERROR_NOT_FOUND ->
                    ?warning("Reporting space ~p initialization traverse failure when "
                        "space has no collector config document", [SpaceId])
            end,

            ?ERROR_INTERNAL_SERVER_ERROR
    end.