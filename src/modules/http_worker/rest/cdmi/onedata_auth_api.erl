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

%% API
-export([authenticate/1]).

-type identity() :: any().

-export_type([identity/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Authenticates user basing on request headers
%% @end
%%--------------------------------------------------------------------
-spec authenticate(Req :: cowboy_req:req()) -> {{ok, identity()} | {error, term()}, cowboy_req:req()}.
authenticate(Req) ->
    rest_auth:authenticate(Req).

%%%===================================================================
%%% Internal functions
%%%===================================================================