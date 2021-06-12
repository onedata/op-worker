%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This behaviour should be implemented by modules that routes requests
%%% for specific entity type to given middleware_handler based on operation,
%%% aspect and scope of request.
%%%
%%% NOTE !!!
%%% Each new supported entity_type should have such router which in turn should
%%% be registered in `middleware:get_router` function.
%%% @end
%%%-------------------------------------------------------------------
-module(middleware_router).


%%--------------------------------------------------------------------
%% @doc
%% Determines middleware handler responsible for handling request based on
%% operation, aspect and scope (entity type is known based on the router itself).
%% @end
%%--------------------------------------------------------------------
-callback resolve_handler(middleware:operation(), gri:aspect(), middleware:scope()) ->
    module().
