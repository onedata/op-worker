%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module contains definitions of oneprovider REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(oneprovider_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Definitions of oneprovider REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Get public information
    {<<"/configuration">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_provider, 
            id = undefined, 
            aspect = configuration, 
            scope = public
        }
    }},
    %% Get test image
    {<<"/test_image">>, rest_handler, #rest_req{
        method = 'GET',
        produces = [<<"image/png">>],
        b_gri = #b_gri{
            type = op_provider, 
            id = undefined, 
            aspect = test_image, 
            scope = public
        }
    }}
].
