%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc 
%%% This module contains definitions of monitoring REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(monitoring_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of monitoring REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Get space metrics
    {<<"/metrics/space/:sid">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>, <<"application/xml">>],
        b_gri = #b_gri{
            type = op_metrics, 
            id = ?BINDING(sid), 
            aspect = space, 
            scope = private
        }
    }}
].
