%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module resolves middleware_handler modules for requests to op_file
%%% entity type.
%%% @end
%%%-------------------------------------------------------------------
-module(file_middleware_router).
-author("Bartosz Walkowicz").

-behaviour(middleware_router).

%% middleware_router callbacks
-export([resolve_handler/3]).

%%%===================================================================
%%% middleware_router callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_router} callback resolve_handler/3.
%% @end
%%--------------------------------------------------------------------
-spec resolve_handler(middleware:operation(), gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_handler(create, Aspect, Scope) ->
    file_middleware_create_handler:assert_operation_supported(Aspect, Scope),
    file_middleware_create_handler;
resolve_handler(get, Aspect, Scope) ->
    file_middleware_get_handler:assert_operation_supported(Aspect, Scope),
    file_middleware_get_handler;
resolve_handler(update, Aspect, Scope) ->
    file_middleware_update_handler:assert_operation_supported(Aspect, Scope),
    file_middleware_update_handler;
resolve_handler(delete, Aspect, Scope) ->
    file_middleware_delete_handler:assert_operation_supported(Aspect, Scope),
    file_middleware_delete_handler.

