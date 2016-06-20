%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains functions that modify handler_description maps,
%%% provided by protocol plugins.
%%% @end
%%%--------------------------------------------------------------------
-module(plugin_properties).
-author("Tomasz Lichon").

%% API
-export([fill_with_default/1]).

-define(DEFAULT_HANDLER_INITIAL_OPTS, #{}).
-define(DEFAULT_EXCEPTION_HANDLER, fun request_exception_handler:handle/4).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Fills missing properties of given handler description with default values.
%% @end
%%--------------------------------------------------------------------
-spec fill_with_default(protocol_plugin_behaviour:handler_description()) ->
    protocol_plugin_behaviour:handler_description().
fill_with_default(HandlerDesc) ->
    HandlerDescWithOpts = fill_if_empty(handler_initial_opts, HandlerDesc, ?DEFAULT_HANDLER_INITIAL_OPTS),
    fill_if_empty(exception_handler, HandlerDescWithOpts, ?DEFAULT_EXCEPTION_HANDLER).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sets key to default value in map if it's not present there.
%% @end
%%--------------------------------------------------------------------
-spec fill_if_empty(term(), #{}, term()) -> #{}.
fill_if_empty(Key, Map, Default) ->
    case maps:is_key(Key, Map) of
        false ->
            maps:put(Key, Default, Map);
        true ->
            Map
    end.