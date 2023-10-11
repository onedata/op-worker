%%%--------------------------------------------------------------------
%%% TODO VFS-11254
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc 
%%% This module contains definitions of atm store REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(atm_store_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of atm store REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Download atm store content
    {<<"/automation/execution/stores/:sid/content_dump">>, rest_handler, #rest_req{
        method = 'GET',
        accept_session_cookie_auth = true,
        produces = [<<"application/octet-stream">>],
        b_gri = #b_gri{
            type = op_atm_store,
            id = ?BINDING(sid),
            aspect = content_dump,
            scope = private
        }
    }}
].
