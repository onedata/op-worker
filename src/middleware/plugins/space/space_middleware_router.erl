%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module resolves middleware_handler modules for requests to op_space
%%% entity type.
%%% @end
%%%-------------------------------------------------------------------
-module(space_middleware_router).
-author("Bartosz Walkowicz").

-behaviour(middleware_router).

-include_lib("ctool/include/errors.hrl").

-export([resolve_handler/3]).


-define(SPACE_ATM_MIDDLEWARE_HANDLER, space_atm_middleware_handler).
-define(SPACE_DATASETS_MIDDLEWARE_HANDLER, space_datasets_middleware_handler).
-define(SPACE_OZ_MIDDLEWARE_HANDLER, space_oz_middleware_handler).
-define(SPACE_STATS_MIDDLEWARE_HANDLER, space_stats_middleware_handler).
-define(SPACE_QOS_MIDDLEWARE_HANDLER, space_qos_middleware_handler).
-define(SPACE_TRANSFERS_MIDDLEWARE_HANDLER, space_transfers_middleware_handler).
-define(SPACE_VIEWS_MIDDLEWARE_HANDLER, space_views_middleware_handler).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec resolve_handler(middleware:operation(), gri:aspect(), middleware:scope()) ->
    module().
resolve_handler(create, {view, _}, private) -> ?SPACE_VIEWS_MIDDLEWARE_HANDLER;
resolve_handler(create, {view_reduce_function, _}, private) -> ?SPACE_VIEWS_MIDDLEWARE_HANDLER;
resolve_handler(create, evaluate_qos_expression, private) -> ?SPACE_QOS_MIDDLEWARE_HANDLER;

resolve_handler(get, list, private) -> ?SPACE_OZ_MIDDLEWARE_HANDLER;
resolve_handler(get, instance, private) -> ?SPACE_OZ_MIDDLEWARE_HANDLER;
resolve_handler(get, views, private) -> ?SPACE_VIEWS_MIDDLEWARE_HANDLER;
resolve_handler(get, {view, _}, private) -> ?SPACE_VIEWS_MIDDLEWARE_HANDLER;
resolve_handler(get, {query_view, _}, private) -> ?SPACE_VIEWS_MIDDLEWARE_HANDLER;
resolve_handler(get, eff_users, private) -> ?SPACE_OZ_MIDDLEWARE_HANDLER;
resolve_handler(get, eff_groups, private) -> ?SPACE_OZ_MIDDLEWARE_HANDLER;
resolve_handler(get, shares, private) -> ?SPACE_OZ_MIDDLEWARE_HANDLER;
resolve_handler(get, providers, private) -> ?SPACE_OZ_MIDDLEWARE_HANDLER;
resolve_handler(get, transfers, private) -> ?SPACE_TRANSFERS_MIDDLEWARE_HANDLER;
resolve_handler(get, transfers_active_channels, private) -> ?SPACE_TRANSFERS_MIDDLEWARE_HANDLER;
resolve_handler(get, {transfers_throughput_charts, _}, private) -> ?SPACE_TRANSFERS_MIDDLEWARE_HANDLER;
resolve_handler(get, dir_stats_service_state, private) -> ?SPACE_STATS_MIDDLEWARE_HANDLER;


resolve_handler(get, available_qos_parameters, private) -> ?SPACE_QOS_MIDDLEWARE_HANDLER;
resolve_handler(get, datasets, private) -> ?SPACE_DATASETS_MIDDLEWARE_HANDLER;
resolve_handler(get, datasets_details, private) -> ?SPACE_DATASETS_MIDDLEWARE_HANDLER;
resolve_handler(get, atm_workflow_execution_summaries, private) -> ?SPACE_ATM_MIDDLEWARE_HANDLER;

resolve_handler(update, {view, _}, private) -> ?SPACE_VIEWS_MIDDLEWARE_HANDLER;
resolve_handler(update, dir_stats_service_state, private) -> ?SPACE_STATS_MIDDLEWARE_HANDLER;

resolve_handler(delete, {view, _}, private) -> ?SPACE_VIEWS_MIDDLEWARE_HANDLER;
resolve_handler(delete, {view_reduce_function, _}, private) -> ?SPACE_VIEWS_MIDDLEWARE_HANDLER;

resolve_handler(_, _, _) -> throw(?ERROR_NOT_SUPPORTED).
