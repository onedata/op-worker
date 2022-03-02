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
    model => ?MODULE
}).

-define(ENABLE_FOR_NEW_SPACES, op_worker:get_env(enable_dir_stats_collector_for_new_spaces, false)).

%%%===================================================================
%%% API
%%%===================================================================

-spec init_for_empty_space(od_space:id()) -> ok.
init_for_empty_space(SpaceId) ->
    Status = case ?ENABLE_FOR_NEW_SPACES of
        true -> enabled;
        false -> disabled
    end,
    NewRecord = #dir_stats_collector_config{status = Status},
    {ok, _} = datastore_model:create(?CTX, #document{
        key = SpaceId,
        value = NewRecord
    }),
    node_cache:put({dir_stats_collector_status, SpaceId}, NewRecord#dir_stats_collector_config.status).


-spec clean_for_space(od_space:id()) -> ok.
clean_for_space(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId),
    utils:rpc_multicall(consistent_hashing:get_all_nodes(), node_cache, clear, [{dir_stats_collector_status, SpaceId}]),
    ok.


-spec enable_for_space(od_space:id()) -> ok.
enable_for_space(SpaceId) ->
    NewRecordStatus = case ?ENABLE_FOR_NEW_SPACES of
        true -> enabled;
        false -> disabled
    end,
    NewRecord = #dir_stats_collector_config{status = NewRecordStatus},

    Diff = fun
        (#dir_stats_collector_config{status = disabled, traverse_num = Num} = Config) ->
            {ok, Config#dir_stats_collector_config{status = initializing, traverse_num = Num + 1}};
        (#dir_stats_collector_config{status = stopping} = Config) ->
            {ok, Config#dir_stats_collector_config{next_status_change_order = enable}};
        (#dir_stats_collector_config{}) ->
            {error, no_action_needed}
    end,

    case datastore_model:update(?CTX, SpaceId, Diff, NewRecord) of
        {ok, #document{value = #dir_stats_collector_config{status = initializing, traverse_num = TraverseNum}}} ->
            % TODO - wyczyscic cache na innyych node
            node_cache:put({dir_stats_collector_status, SpaceId}, initializing),
            dir_stats_initialization_traverse:start(SpaceId, TraverseNum),
            ok;
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
        (#dir_stats_collector_config{status = initializing, next_status_change_order = disable}) ->
            {error, no_action_needed};
        (#dir_stats_collector_config{status = initializing} = Config) ->
            {ok, Config#dir_stats_collector_config{next_status_change_order = disable}};
        (#dir_stats_collector_config{}) ->
            {error, no_action_needed}
    end,

    case datastore_model:update(?CTX, SpaceId, Diff) of
        {ok, #document{value = #dir_stats_collector_config{status = stopping}}} ->
            node_cache:put({dir_stats_collector_status, SpaceId}, stopping),
            dir_stats_collector:disable_stats_collecting(SpaceId);
        {ok, #document{value = #dir_stats_collector_config{status = initializing, traverse_num = TraverseNum}}} ->
            dir_stats_initialization_traverse:cancel(SpaceId, TraverseNum),
            ok;
        {ok, _} ->
            ok;
        {error, no_action_needed} ->
            ok;
        {error, not_found} ->
            ok
    end.


% TODO - wywolywane z callbackow cancel i finich traversu
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
            node_cache:put({dir_stats_collector_status, SpaceId}, enabled),
            ok;
        {ok, #document{value = #dir_stats_collector_config{status = stopping}}} ->
            node_cache:put({dir_stats_collector_status, SpaceId}, stopping),
            % TODO - wyslac do wszystkich procesow info o stopie (zrobic to w funkcji pomocniczej wywolywanej tu i w disable_for_space)
            ok;
        {error, {wrong_status, WrongStatus}} ->
            ?warning("Reporting space ~p enabling finished when space has status ~p", [SpaceId, WrongStatus]);
        {error, not_found} ->
            ok
    end.


-spec report_disabling_finished(od_space:id()) -> ok.
report_disabling_finished(SpaceId) ->
    Diff = fun
        (#dir_stats_collector_config{status = stopping, next_status_change_order = enable} = Config) ->
            {ok, Config#dir_stats_collector_config{status = initializing, next_status_change_order = undefined}};
        (#dir_stats_collector_config{status = stopping} = Config) ->
            {ok, Config#dir_stats_collector_config{status = disabled}};
        (#dir_stats_collector_config{status = Status}) ->
            {error, {wrong_status, Status}}
    end,

    case datastore_model:update(?CTX, SpaceId, Diff) of
        {ok, #document{value = #dir_stats_collector_config{status = disabled}}} ->
            node_cache:put({dir_stats_collector_status, SpaceId}, disabled),
            ok;
        {ok, #document{value = #dir_stats_collector_config{status = initializing}}} ->
            node_cache:put({dir_stats_collector_status, SpaceId}, initializing),
            dir_stats_initialization_traverse:run(SpaceId),
            ok;
        {error, {wrong_status, WrongStatus}} ->
            ?warning("Reporting space ~p disabling finished when space has status ~p", [SpaceId, WrongStatus]);
        {error, not_found} ->
            ok
    end.


-spec is_enabled_for_space(od_space:id()) -> boolean().
is_enabled_for_space(SpaceId) ->
    case get_status_for_space(SpaceId) of
        disabled -> false;
        _ -> true
    end.


-spec get_status_for_space(od_space:id()) -> status().
get_status_for_space(SpaceId) ->
    % TODO VFS-8837 - consider usage of datastore memory_copies instead of node_cache
    % (check performance when doc cannot be found)
    % TODO - ogarnac race ze zmienianiem uprawnien
%%    {ok, Ans} = node_cache:acquire({dir_stats_collector_status, SpaceId}, fun() ->
%%        case datastore_model:get(?CTX, SpaceId) of
%%            {ok, #document{value = #dir_stats_collector_config{status = Status}}} ->
%%                {ok, Status, infinity};
%%            {error, not_found} ->
%%                {ok, disabled, infinity}
%%        end
%%    end),
%%    Ans.
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = #dir_stats_collector_config{status = Status}}} ->
            {ok, Status};
        {error, not_found} ->
            {ok, disabled}
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
        {status, atom}
    ]}.


-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, true = _Enabled}) ->
    {2, {?MODULE, enabled}};
upgrade_record(1, {?MODULE, false = _Enabled}) ->
    {2, {?MODULE, disabled}}.