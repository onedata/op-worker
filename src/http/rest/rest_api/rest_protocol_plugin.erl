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
        {"/rest/:version/file_distribution/[...]", #{handler => file_distribution_handler}},
        {"/rest/:version/[...]", #{handler => rest_handler}}
    ].


%%%===================================================================
%%% Internal functions
%%%===================================================================