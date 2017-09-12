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
        {"/api/v3/oneprovider/files/[...]", #{handler => files}},
        {"/api/v3/oneprovider/index", #{handler => index_collection}},
        {"/api/v3/oneprovider/index/:id", #{handler => index}},
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
        {"/api/v3/oneprovider/spaces", #{handler => spaces}},
        {"/api/v3/oneprovider/spaces/:sid", #{handler => spaces_by_id}},
        {"/api/v3/oneprovider/space-cleanup/:sid", #{handler => space_cleanup}},
        {"/api/v3/oneprovider/transfers", #{handler => transfers,
            handler_initial_opts => #{list_all => true}}},
        {"/api/v3/oneprovider/transfers/:id", #{handler => transfers}}
].


%%%===================================================================
%%% Internal functions
%%%===================================================================