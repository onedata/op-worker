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


%% API
-export([init_for_space/1, clean_for_space/1, is_enabled_for_space/1, get_status_for_space/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2]).


-type active_status() :: enabled | initializing.
-type status() :: active_status() | disabled.
-type ctx() :: datastore:ctx().

-export_type([active_status/0, status/0]).


-define(CTX, #{
    model => ?MODULE
}).

-define(ENABLE_FOR_NEW_SPACES, op_worker:get_env(enable_dir_stats_collector_for_new_spaces, false)).

%%%===================================================================
%%% API
%%%===================================================================

-spec init_for_space(od_space:id()) -> ok.
init_for_space(SpaceId) ->
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
    {ok, Ans} = node_cache:acquire({dir_stats_collector_status, SpaceId}, fun() ->
        case datastore_model:get(?CTX, SpaceId) of
            {ok, #document{value = #dir_stats_collector_config{status = Status}}} ->
                {ok, Status, infinity};
            {error, not_found} ->
                {ok, disabled, infinity}
        end
    end),
    Ans.


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