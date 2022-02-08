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
%%% TODO VFS-8935 - allow enabling of existing spaces.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collector_config).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").


%% API
-export([init_for_space/1, clean_for_space/1, is_enabled_for_space/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).


-type ctx() :: datastore:ctx().


-define(CTX, #{
    model => ?MODULE
}).


%%%===================================================================
%%% API
%%%===================================================================

-spec init_for_space(od_space:id()) -> ok.
init_for_space(SpaceId) ->
    NewRecord = #dir_stats_collector_config{},
    ok = ?extract_ok(datastore_model:create(?CTX, #document{
        key = SpaceId,
        value = NewRecord
    })),
    node_cache:put({dir_stats_collector_enabled, SpaceId}, NewRecord#dir_stats_collector_config.enabled).


-spec clean_for_space(od_space:id()) -> ok.
clean_for_space(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId),
    utils:rpc_multicall(consistent_hashing:get_all_nodes(), node_cache, clear, [{dir_stats_collector_enabled, SpaceId}]),
    ok.


-spec is_enabled_for_space(od_space:id()) -> boolean().
is_enabled_for_space(SpaceId) ->
    % TODO VFS-8837 - consider usage of datastore memory_copies instead of node_cache
    % (check performance when doc cannot be found)
    {ok, IsEnabled} = node_cache:acquire({dir_stats_collector_enabled, SpaceId}, fun() ->
        case datastore_model:get(?CTX, SpaceId) of
            {ok, #document{value = #dir_stats_collector_config{enabled = Enabled}}} ->
                {ok, Enabled, infinity};
            {error, not_found} ->
                {ok, false, infinity}
        end
    end),
    IsEnabled.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.


-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {enabled, boolean}
    ]}.