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
        {"/api/v3/oneprovider/metrics/provider/:id", #{handler => metrics,
            handler_initial_opts => #{subject_type => provider}}},
        {"/api/v3/oneprovider/metrics/space/:id", #{handler => metrics,
            handler_initial_opts => #{subject_type => space}}},
        {"/api/v3/oneprovider/metrics/user/:id", #{handler => metrics,
            handler_initial_opts => #{subject_type => user}}},
        {"/api/v3/oneprovider/replicas/[...]", #{handler => replicas}},
        {"/api/v3/oneprovider/replicas-id/:id", #{handler => replicas,
            handler_initial_opts => #{resource_type => id}}},
        {"/api/v3/oneprovider/spaces", #{handler => spaces}},
        {"/api/v3/oneprovider/spaces/:sid", #{handler => spaces_by_id}}
].


%%%===================================================================
%%% Internal functions
%%%===================================================================