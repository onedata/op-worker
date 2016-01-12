%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Public api for management of request flow, available in
%%% protocol plugins.
%%% @end
%%%--------------------------------------------------------------------
-module(onedata_handler_management_api).
-author("Tomasz Lichon").

%% API
-export([set_handler/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Change handler of actual request
%%--------------------------------------------------------------------
-spec set_handler(module()) -> ok.
set_handler(Handler) ->
    request_context:set_handler(Handler).

%%%===================================================================
%%% Internal functions
%%%===================================================================