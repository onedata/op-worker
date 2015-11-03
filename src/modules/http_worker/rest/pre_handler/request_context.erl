%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Operations on process dictionary, that manipulate on context of rest request.
%%% @end
%%%--------------------------------------------------------------------
-module(request_context).
-author("Tomasz Lichon").

%% API
-export([set_handler/1, get_handler/0,
    set_exception_handler/1, get_exception_handler/0,
    set_content_types_accepted/1, get_content_types_accepted/0,
    set_content_types_provided/1, get_content_types_provided/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Convenience function that stores the reference to handler module in
%% process dict.
%% @end
%%--------------------------------------------------------------------
-spec set_handler(Module :: atom()) -> term().
set_handler(Module) ->
    erlang:put(handler, Module).

%%--------------------------------------------------------------------
%% @doc
%% Convenience function that retrieves the reference to handler module from
%% process dict.
%% @end
%%--------------------------------------------------------------------
-spec get_handler() -> atom().
get_handler() ->
    erlang:get(handler).

%%--------------------------------------------------------------------
%% @doc
%% Convenience function that stores the error_translator fun in process dict.
%% @end
%%--------------------------------------------------------------------
-spec set_exception_handler(Delegation :: protocol_plugin_behaviour:exception_handler()) -> term().
set_exception_handler(Delegation) ->
    erlang:put(exception_handler, Delegation).

%%--------------------------------------------------------------------
%% @doc
%% Convenience function that retrieves the error_translator fun from process
%% dict.
%% @end
%%--------------------------------------------------------------------
-spec get_exception_handler() -> protocol_plugin_behaviour:exception_handler().
get_exception_handler() ->
    erlang:get(exception_handler).

%%--------------------------------------------------------------------
%% @doc
%% Convenience function that stores the list of callbacks accepting different
%% content type in process dict.
%% @end
%%--------------------------------------------------------------------
-spec set_content_types_accepted([plugin_callback_selector:content_type_callback()]) -> term().
set_content_types_accepted(AcceptedContent) ->
    erlang:put(content_types_accepted, AcceptedContent).

%%--------------------------------------------------------------------
%% @doc
%% Convenience function that retrieves the list of callbacks accepting different
%% content type in process dict.
%% dict.
%% @end
%%--------------------------------------------------------------------
-spec get_content_types_accepted() -> [plugin_callback_selector:content_type_callback()].
get_content_types_accepted() ->
    erlang:get(content_types_accepted).

%%--------------------------------------------------------------------
%% @doc
%% Convenience function that stores the list of callbacks accepting different
%% content type in process dict.
%% @end
%%--------------------------------------------------------------------
-spec set_content_types_provided([plugin_callback_selector:content_type_callback()]) -> term().
set_content_types_provided(ProvidedContent) ->
    erlang:put(content_types_provided, ProvidedContent).

%%--------------------------------------------------------------------
%% @doc
%% Convenience function that retrieves the list of callbacks accepting different
%% content type in process dict.
%% dict.
%% @end
%%--------------------------------------------------------------------
-spec get_content_types_provided() -> [plugin_callback_selector:content_type_callback()].
get_content_types_provided() ->
    erlang:get(content_types_provided).

%%%===================================================================
%%% Internal functions
%%%===================================================================