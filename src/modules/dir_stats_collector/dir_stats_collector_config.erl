%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing dir_stats_collector configuration for each space.
%%% The collector is by default enabled for all space supports granted
%%% by providers in versions 21.02.0-alpha25 or newer, otherwise disabled.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collector_config).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([init_for_empty_space/1, clean_for_space/1,
    enable_for_space/1, disable_for_space/1, report_enabling_finished/1, report_disabling_finished/1,
    is_enabled_for_space/1, get_status_for_space/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2]).


-type active_status() :: enabled | initializing. % TODO - zmienic initializing/stopping na enabling/disabling
-type status() :: active_status() | disabled | stopping.
-type status_change_order() :: enable | disable.
-type ctx() :: datastore:ctx().

-export_type([active_status/0, status/0, status_change_order/0]).


-define(CTX, #{
    model => ?MODULE,
    memory_copies => all
}).

-define(ENABLE_FOR_NEW_SPACES, op_worker:get_env(enable_dir_stats_collector_for_new_spaces, false)).

%%%===================================================================
%%% API
%%%===================================================================

-spec init_for_empty_space(od_space:id()) -> ok.
init_for_empty_space(SpaceId) ->
    {ok, _} = datastore_model:create(?CTX, #document{
        key = SpaceId,
        value = #dir_stats_collector_config{status = is_enabled_to_status(?ENABLE_FOR_NEW_SPACES)}
    }),
    ok.


-spec clean_for_space(od_space:id()) -> ok.
clean_for_space(SpaceId) ->
    ok = datastore_model:delete(?CTX, SpaceId).


-spec enable_for_space(od_space:id()) -> ok.
enable_for_space(SpaceId) ->
    NewRecord = #dir_stats_collector_config{status = initializing, traverse_num = 1},

    Diff = fun
        (#dir_stats_collector_config{status = disabled, traverse_num = Num} = Config) ->
            {ok, Config#dir_stats_collector_config{status = initializing, traverse_num = Num + 1}};
        (#dir_stats_collector_config{status = stopping, next_status_change_order = undefined} = Config) ->
            {ok, Config#dir_stats_collector_config{next_status_change_order = enable}};
        (#dir_stats_collector_config{}) ->
            {error, no_action_needed}
    end,

    case datastore_model:update(?CTX, SpaceId, Diff, NewRecord) of
        {ok, #document{value = #dir_stats_collector_config{status = initializing, traverse_num = TraverseNum}}} ->
            dir_stats_initialization_traverse:run(SpaceId, TraverseNum);
        {ok, _} ->
            ok;
        {error, no_action_needed} ->
            ok
    end.


-spec disable_for_space(od_space:id()) -> ok.
disable_for_space(SpaceId) ->
    Diff = fun
        (#dir_stats_collector_config{status = enabled} = Config) ->
            {ok, Config#dir_stats_collector_config{status = stopping}};
        (#dir_stats_collector_config{status = initializing, next_status_change_order = undefined} = Config) ->
            {ok, Config#dir_stats_collector_config{next_status_change_order = disable}};
        (#dir_stats_collector_config{}) ->
            {error, no_action_needed}
    end,

    case datastore_model:update(?CTX, SpaceId, Diff) of
        {ok, #document{value = #dir_stats_collector_config{status = stopping}}} ->
            dir_stats_collector:disable_stats_collecting(SpaceId);
        {ok, #document{value = #dir_stats_collector_config{status = initializing, traverse_num = TraverseNum}}} ->
            dir_stats_initialization_traverse:cancel(SpaceId, TraverseNum);
        {error, no_action_needed} ->
            ok;
        {error, not_found} ->
            ?warning("Disabling space ~p without collector config document", [SpaceId])
    end.


-spec report_enabling_finished(od_space:id()) -> ok.
report_enabling_finished(SpaceId) ->
    Diff = fun
        (#dir_stats_collector_config{status = initializing, next_status_change_order = disable} = Config) ->
            {ok, Config#dir_stats_collector_config{status = stopping, next_status_change_order = undefined}};
        (#dir_stats_collector_config{status = initializing} = Config) ->
            {ok, Config#dir_stats_collector_config{status = enabled}};
        (#dir_stats_collector_config{status = Status}) ->
            {error, {wrong_status, Status}}
    end,

    case datastore_model:update(?CTX, SpaceId, Diff) of
        {ok, #document{value = #dir_stats_collector_config{status = enabled}}} ->
            ok;
        {ok, #document{value = #dir_stats_collector_config{status = stopping}}} ->
            dir_stats_collector:disable_stats_collecting(SpaceId);
        {error, {wrong_status, WrongStatus}} ->
            ?warning("Reporting space ~p enabling finished when space has status ~p", [SpaceId, WrongStatus]);
        {error, not_found} ->
            ?warning("Reporting space ~p enabling finished when space has no collector config document ~p", [SpaceId])
    end.


-spec report_disabling_finished(od_space:id()) -> ok.
report_disabling_finished(SpaceId) ->
    Diff = fun
        (#dir_stats_collector_config{status = stopping, next_status_change_order = enable, traverse_num = Num} = Config) ->
            {ok, Config#dir_stats_collector_config{
                status = initializing,
                traverse_num = Num + 1,
                next_status_change_order = undefined
            }};
        (#dir_stats_collector_config{status = stopping} = Config) ->
            {ok, Config#dir_stats_collector_config{status = disabled}};
        (#dir_stats_collector_config{status = Status}) ->
            {error, {wrong_status, Status}}
    end,

    case datastore_model:update(?CTX, SpaceId, Diff) of
        {ok, #document{value = #dir_stats_collector_config{status = disabled}}} ->
            ok;
        {ok, #document{value = #dir_stats_collector_config{status = initializing, traverse_num = TraverseNum}}} ->
            dir_stats_initialization_traverse:run(SpaceId, TraverseNum);
        {error, {wrong_status, WrongStatus}} ->
            ?warning("Reporting space ~p disabling finished when space has status ~p", [SpaceId, WrongStatus]);
        {error, not_found} ->
            ?warning("Reporting space ~p disabling finished when space has no collector config document ~p", [SpaceId])
    end.


-spec is_enabled_for_space(od_space:id()) -> boolean().
is_enabled_for_space(SpaceId) -> % TODO - zmienic nazwe zeby nie sugerowala ze chodzi o stan enabled tylko o to, ze zbieramy statystyki
    case get_status_for_space(SpaceId) of
        enabled -> true;
        initializing -> true;
        _ -> false
    end.


-spec get_status_for_space(od_space:id()) -> status().
get_status_for_space(SpaceId) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = #dir_stats_collector_config{status = Status}}} -> Status;
        {error, not_found} -> disabled
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
        {status, atom},
        {traverse_num, integer},
        {next_status_change_order, atom}
    ]}.


-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, IsEnabled}) ->
    {2, {?MODULE, is_enabled_to_status(IsEnabled), 0, undefined}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec is_enabled_to_status(boolean()) -> status().
is_enabled_to_status(true = _IsEnabled) ->
    enabled;
is_enabled_to_status(false = _IsEnabled) ->
    disabled.