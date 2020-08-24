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
%%% This module contains definitions of file_path_resolution REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(file_path_resolution_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of file_path_resolution REST paths.
%% @end
%%--------------------------------------------------------------------
-spec routes() -> [{binary(), module(), #rest_req{}}].
routes() -> [
    %% Lookup file id
    {<<"/lookup-file-id/[...]">>, rest_handler, #rest_req{
        method = 'POST',
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_file, 
            id = ?PATH_BINDING, 
            aspect = object_id, 
            scope = private
        }
    }}
].
