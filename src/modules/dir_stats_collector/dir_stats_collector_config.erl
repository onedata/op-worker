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
%%% of enable_dir_stats_collector_for_new_spaces environment variable
%%% at the moment of space support granting by provider. For other spaces
%%% collecting status is disabled.
%%%
%%% Collecting status can be changed using enable_for_space/1 and
%%% disable_for_space/1 API functions. In such a case, status is not changed
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
%%% Additionally, config includes timestamps of collecting state changes
%%% that allow verification when historic statistics were trustworthy.
%%%
%%% NOTE: Timestamps are generated at collecting status transition.
%%%       Collecting status is changed to enabled as soon as all directories
%%%       calculate statistics using their direct children. Statistics
%%%       propagation via files tree is asynchronous. Thus, timestamps
%%%       should be treated as indicative.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collector_config).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API - getters
-export([is_collecting_active/1, get_extended_collecting_status/1,
    get_enabling_time/1, get_collecting_status_change_timestamps/1]).
%% API - init/cleanup
-export([init_for_empty_space/1, clean/1]).
%% API - collecting status changes
-export([enable_for_space/1, disable_for_space/1,
    report_collections_initialization_finished/1, report_collectors_stopped/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2]).


-type collecting_status() :: active_collecting_status() | disabled | collectors_stopping.
% update requests can be generated only for active statuses
-type active_collecting_status() :: enabled | collections_initialization.
% extended status includes information about initialization traverse - it is used
% outside this module while internally status and travers number are stored separately
-type extended_collecting_status() :: extended_active_collecting_status() | disabled | collectors_stopping.
-type extended_active_collecting_status() :: enabled |
    {collections_initialization, InitializationTraverseNum :: non_neg_integer()}.

-type collecting_status_change_order() :: enable | disable.
-type status_change_timestamp() :: {collecting_status(), time:seconds()}.

-type record() :: #dir_stats_collector_config{}.
-type diff_fun() :: datastore_doc:diff(record()).
-type ctx() :: datastore:ctx().

-export_type([collecting_status/0, active_collecting_status/0,
    extended_collecting_status/0, extended_active_collecting_status/0,
    collecting_status_change_order/0, status_change_timestamp/0]).


-define(CTX, #{
    model => ?MODULE,
    memory_copies => all
}).

-define(ENABLE_FOR_NEW_SPACES, op_worker:get_env(enable_dir_stats_collector_for_new_spaces, false)).
-define(MAX_HISTORY_SIZE, 50).

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
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = #dir_stats_collector_config{
            collecting_status = collections_initialization,
            collections_initialization_traverse_num = TraverseNum
        }}} ->
            {collections_initialization, TraverseNum};
        {ok, #document{value = #dir_stats_collector_config{collecting_status = Status}}} ->
            Status;
        {error, not_found} ->
            disabled
    end.


-spec get_enabling_time(od_space:id()) -> {ok, time:seconds()} | ?ERROR_FORBIDDEN.
get_enabling_time(SpaceId) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = #dir_stats_collector_config{
            collecting_status = enabled, 
            collecting_status_change_timestamps = []}
        }} -> 
            {ok, 0};
        {ok, #document{value = #dir_stats_collector_config{
            collecting_status = enabled,
            collecting_status_change_timestamps = [{enabled, Time}]}
        }} ->
            {ok, Time};
        {ok, _} ->
            ?ERROR_FORBIDDEN;
        {error, not_found} ->
            ?ERROR_FORBIDDEN
    end.


-spec get_collecting_status_change_timestamps(od_space:id()) -> [status_change_timestamp()].
get_collecting_status_change_timestamps(SpaceId) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = #dir_stats_collector_config{
            collecting_status_change_timestamps = Timestamps
        }}} ->
            Timestamps;
        {error, not_found} ->
            []
    end.


%%%===================================================================
%%% API - init/cleanup
%%%===================================================================

-spec init_for_empty_space(od_space:id()) -> ok.
init_for_empty_space(SpaceId) ->
    {ok, _} = datastore_model:create(?CTX, #document{
        key = SpaceId,
        value = #dir_stats_collector_config{collecting_status = is_enabled_to_status(?ENABLE_FOR_NEW_SPACES)}
    }),
    ok.


-spec clean(od_space:id()) -> ok.
clean(SpaceId) ->
    ok = datastore_model:delete(?CTX, SpaceId).


%%%===================================================================
%%% API - collecting status changes
%%%===================================================================

-spec enable_for_space(od_space:id()) -> ok.
enable_for_space(SpaceId) ->
    NewRecord = #dir_stats_collector_config{
        collecting_status = collections_initialization,
        collections_initialization_traverse_num = 1
    },

    Diff = fun
        (#dir_stats_collector_config{
            collecting_status = disabled, 
            collections_initialization_traverse_num = Num
        } = Config) ->
            {ok, Config#dir_stats_collector_config{
                collecting_status = collections_initialization,
                collections_initialization_traverse_num = Num + 1
            }};
        (#dir_stats_collector_config{
            collecting_status = collectors_stopping, 
            next_collecting_status_change_order = undefined
        } = Config) ->
            {ok, Config#dir_stats_collector_config{next_collecting_status_change_order = enable}};
        (#dir_stats_collector_config{}) ->
            {error, no_action_needed}
    end,

    case update(SpaceId, Diff, NewRecord) of
        {ok, #document{value = #dir_stats_collector_config{
            collecting_status = collections_initialization,
            collections_initialization_traverse_num = TraverseNum
        }}} ->
            dir_stats_collections_initialization_traverse:run(SpaceId, TraverseNum);
        {ok, _} ->
            ok;
        {error, no_action_needed} ->
            ok
    end.


-spec disable_for_space(od_space:id()) -> ok.
disable_for_space(SpaceId) ->
    Diff = fun
        (#dir_stats_collector_config{
            collecting_status = enabled
        } = Config) ->
            {ok, Config#dir_stats_collector_config{collecting_status = collectors_stopping}};
        (#dir_stats_collector_config{
            collecting_status = collections_initialization,
            next_collecting_status_change_order = undefined
        } = Config) ->
            {ok, Config#dir_stats_collector_config{next_collecting_status_change_order = disable}};
        (#dir_stats_collector_config{}) ->
            {error, no_action_needed}
    end,

    case update(SpaceId, Diff) of
        {ok, #document{value = #dir_stats_collector_config{
            collecting_status = collectors_stopping
        }}} ->
            dir_stats_collector:stop_collecting(SpaceId);
        {ok, #document{value = #dir_stats_collector_config{
            collecting_status = collections_initialization,
            collections_initialization_traverse_num = TraverseNum
        }}} ->
            dir_stats_collections_initialization_traverse:cancel(SpaceId, TraverseNum);
        {error, no_action_needed} ->
            ok;
        {error, not_found} ->
            ?warning("Disabling space ~p without collector config document", [SpaceId])
    end.


-spec report_collections_initialization_finished(od_space:id()) -> ok.
report_collections_initialization_finished(SpaceId) ->
    Diff = fun
        (#dir_stats_collector_config{
            collecting_status = collections_initialization,
            next_collecting_status_change_order = disable
        } = Config) ->
            {ok, Config#dir_stats_collector_config{
                collecting_status = collectors_stopping,
                next_collecting_status_change_order = undefined
            }};
        (#dir_stats_collector_config{collecting_status = collections_initialization} = Config) ->
            {ok, Config#dir_stats_collector_config{collecting_status = enabled}};
        (#dir_stats_collector_config{collecting_status = Status}) ->
            {error, {wrong_status, Status}}
    end,

    case update(SpaceId, Diff) of
        {ok, #document{value = #dir_stats_collector_config{collecting_status = enabled}}} ->
            ok;
        {ok, #document{value = #dir_stats_collector_config{collecting_status = collectors_stopping}}} ->
            dir_stats_collector:stop_collecting(SpaceId);
        {error, {wrong_status, WrongStatus}} ->
            ?warning("Reporting space ~p enabling finished when space has status ~p", [SpaceId, WrongStatus]);
        {error, not_found} ->
            ?warning("Reporting space ~p enabling finished when space has no collector config document ~p", [SpaceId])
    end.


-spec report_collectors_stopped(od_space:id()) -> ok.
report_collectors_stopped(SpaceId) ->
    Diff = fun
        (#dir_stats_collector_config{
            collecting_status = collectors_stopping,
            next_collecting_status_change_order = enable,
            collections_initialization_traverse_num = Num
        } = Config) ->
            {ok, Config#dir_stats_collector_config{
                collecting_status = collections_initialization,
                collections_initialization_traverse_num = Num + 1,
                next_collecting_status_change_order = undefined
            }};
        (#dir_stats_collector_config{collecting_status = collectors_stopping} = Config) ->
            {ok, Config#dir_stats_collector_config{collecting_status = disabled}};
        (#dir_stats_collector_config{collecting_status = Status}) ->
            {error, {wrong_status, Status}}
    end,

    case update(SpaceId, Diff) of
        {ok, #document{value = #dir_stats_collector_config{collecting_status = disabled}}} ->
            ok;
        {ok, #document{value = #dir_stats_collector_config{
            collecting_status = collections_initialization,
            collections_initialization_traverse_num = TraverseNum
        }}} ->
            dir_stats_collections_initialization_traverse:run(SpaceId, TraverseNum);
        {error, {wrong_status, WrongStatus}} ->
            ?warning("Reporting space ~p disabling finished when space has status ~p", [SpaceId, WrongStatus]);
        {error, not_found} ->
            ?warning("Reporting space ~p disabling finished when space has no collector config document ~p", [SpaceId])
    end.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.


-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {enabled, boolean}
    ]};
get_record_struct(2) ->
    {record, [
        {collecting_status, atom},
        {collections_initialization_traverse_num, integer},
        {next_collecting_status_change_order, atom},
        {collecting_status_change_timestamps, [{atom, integer}]}
    ]}.


-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, IsEnabled}) ->
    {2, {?MODULE, is_enabled_to_status(IsEnabled), 0, undefined, []}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec update(od_space:id(), diff_fun()) -> {ok, datastore_doc:doc(record())} | {error, term()}.
update(SpaceId, Diff) ->
    datastore_model:update(?CTX, SpaceId, diff_fun_with_timestamp_update(Diff)).


-spec update(od_space:id(), diff_fun(), record()) -> {ok, datastore_doc:doc(record())} | {error, term()}.
update(SpaceId, Diff, Default) ->
    datastore_model:update(?CTX, SpaceId, diff_fun_with_timestamp_update(Diff), Default).


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


-spec is_enabled_to_status(boolean()) -> collecting_status().
is_enabled_to_status(true = _IsEnabled) ->
    enabled;
is_enabled_to_status(false = _IsEnabled) ->
    disabled.