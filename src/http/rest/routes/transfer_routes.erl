%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module contains definitions of transfer REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_routes).

-include("http/rest.hrl").

-export([routes/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Definitions of transfer REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Cancel specific transfer
    {<<"/transfers/:tid">>, rest_handler, #rest_req{
        method = 'DELETE',
        b_gri = #b_gri{type = op_transfer, id = ?BINDING(tid), aspect = instance}
    }},
    %% Get transfer status
    {<<"/transfers/:tid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_transfer, id = ?BINDING(tid), aspect = instance}
    }},
    %% Rerun ended transfer
    {<<"/transfers/:tid/rerun">>, rest_handler, #rest_req{
        method = 'POST',
        produces = [<<"application/json">>],
        b_gri = #b_gri{type = op_transfer, id = ?BINDING(tid), aspect = rerun}
    }}
].
