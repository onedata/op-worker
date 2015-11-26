%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This mudule provides information about cdmi protocol plugin and it's used
%%% by onedata during plugin registration process.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_protocol_plugin).
-behaviour(protocol_plugin_behaviour).
-author("Tomasz Lichon").

%% API
-export([routes/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns routes to cdmi protocol.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{Route :: string(), protocol_plugin_behaviour:handler()}].
routes() ->
    [
        {"/cdmi/cdmi_capabilities/[...]", #{handler => cdmi_capabilities_handler}},
        {"/cdmi/cdmi_objectid/:id/[...]", #{handler => cdmi_objectid_handler}},
        {"/cdmi/[...]", fun cdmi_handler_selector:choose_object_or_container_handler/1}
    ].


%%%===================================================================
%%% Internal functions
%%%===================================================================