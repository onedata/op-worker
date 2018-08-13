%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Routes used by cowboy to route requests to adequate handlers.
%%% @end
%%%--------------------------------------------------------------------
-module(rest_router).
-author("Tomasz Lichon").

-include("global_definitions.hrl").

%% API
-export([top_level_routing/0]).

-define(HANDLERS, [
    attributes,
    changes,
    files,
    index,
    index_collection,
    metadata,
    onedata_metrics,
    query_index,
    replicas,
    spaces,
    space_by_id,
    transfers,
    cdmi_capabilities_handler,
    cdmi_container_capabilities_handler,
    cdmi_container_handler,
    cdmi_dataobject_capabilities_handler,
    cdmi_object_handler,
    cdmi_objectid_handler
]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Finds all protocol plugins and returns routes to them with preceding
%% pre_handler.
%% @end
%%--------------------------------------------------------------------
-spec top_level_routing() -> list().
top_level_routing() ->
    % Handler modules must be loaded to ensure proper working of
    % erlang:function_exported used to check if given handler implements
    % optional callback; otherwise it will always return false
    lists:foreach(fun(Module) -> code:ensure_loaded(Module) end, ?HANDLERS),
    {ok, Plugins} = application:get_env(?APP_NAME, protocol_plugins),
    PluginRoutes = [Plugin:routes() || Plugin <- Plugins],
    FlattenPluginRoutes = lists:flatten(PluginRoutes),
    [{Path, pre_handler, PluginDescription} || {Path, PluginDescription} <- FlattenPluginRoutes].

%%%===================================================================
%%% Internal functions
%%%===================================================================


