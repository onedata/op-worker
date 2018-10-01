%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides information about rest protocol plugin and it's used
%%% by onedata during plugin registration process.
%%% @end
%%%--------------------------------------------------------------------
-module(rest_protocol_plugin).
-behaviour(protocol_plugin_behaviour).
-author("Tomasz Lichon").

%% API
-export([routes/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns routes to rest protocol.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{Route :: string(), protocol_plugin_behaviour:handler()}].
routes() ->
    [
        {"/api/v3/oneprovider/attributes/[...]", #{handler => attributes}},
        {"/api/v3/oneprovider/changes/metadata/:sid", #{handler => changes}},
        {"/api/v3/oneprovider/debug/transfers_mock", #{handler => transfers_mock}},
        {"/api/v3/oneprovider/files/[...]", #{handler => files}},
        {"/api/v3/oneprovider/index", #{handler => index_collection}},
        {"/api/v3/oneprovider/spaces/:sid/indexes/:index_name", #{handler => index_by_name}},
        {"/api/v3/oneprovider/metadata/[...]", #{handler => metadata}},
        {"/api/v3/oneprovider/metadata-id/:id", #{handler => metadata,
            handler_initial_opts => #{resource_type => id}}},
        {"/api/v3/oneprovider/metrics/space/:sid", #{handler => onedata_metrics,
            handler_initial_opts => #{subject_type => space, secondary_subject_type => undefined}}},
        {"/api/v3/oneprovider/metrics/space/:sid/user/:uid", #{handler => onedata_metrics,
            handler_initial_opts => #{subject_type => space, secondary_subject_type => user}}},
        {"/api/v3/oneprovider/query-index/:id", #{handler => query_index}},
        {"/api/v3/oneprovider/replicas/[...]", #{handler => replicas}},
        {"/api/v3/oneprovider/replicas-id/:id", #{handler => replicas,
            handler_initial_opts => #{resource_type => id}}},
        {"/api/v3/oneprovider/replicas-index/[...]", #{handler => replicas_index}},
        {"/api/v3/oneprovider/spaces", #{handler => spaces}},
        {"/api/v3/oneprovider/spaces/:sid", #{handler => space_by_id}},
        {"/api/v3/oneprovider/spaces/:sid/transfers", #{handler => transfers}},
        {"/api/v3/oneprovider/transfers/:id", #{handler => transfer_by_id}},
        {"/api/v3/oneprovider/transfers/:id/rerun", #{handler => transfer_by_id}}
].


%%%===================================================================
%%% Internal functions
%%%===================================================================