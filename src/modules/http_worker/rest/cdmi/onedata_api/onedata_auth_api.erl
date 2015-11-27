%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Public api for authentication, available in
%%% protocol plugins.
%%% @end
%%%--------------------------------------------------------------------
-module(onedata_auth_api).
-author("Tomasz Lichon").

-include("http_common.hrl").

%% API
-export([is_authorized/2]).

-type identity() :: any().

-export_type([identity/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2, updates State
%%--------------------------------------------------------------------
-spec is_authorized(req(), #{}) -> {boolean(), req(), #{}}.
is_authorized(Req, State) ->
    rest_auth:is_authorized(Req, State).

%%%===================================================================
%%% Internal functions
%%%===================================================================