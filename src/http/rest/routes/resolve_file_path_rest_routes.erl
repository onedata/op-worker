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
%%% This module contains definitions of resolve_file_path REST methods.
%%% @end
%%%--------------------------------------------------------------------
-module(resolve_file_path_rest_routes).

-include("http/rest.hrl").

-export([routes/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Definitions of resolve_file_path REST paths.
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
