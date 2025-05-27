%%%--------------------------------------------------------------------
%%% This file has been automatically generated from Swagger
%%% specification - DO NOT EDIT!
%%%
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
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
    %% Subscribe to file events
    {<<"/changes/metadata/:sid">>, space_monitoring_stream_handler, #rest_req{
        method = 'POST',
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_metrics, 
            id = ?BINDING(sid), 
            aspect = changes, 
            scope = private
        }
    }},
    %% Subscribe to space file events
    {<<"/spaces/:sid/events/files">>, space_file_events_stream_handler, #rest_req{
        method = 'POST',
        consumes = [<<"application/json">>],
        produces = [<<"application/json">>],
        b_gri = #b_gri{
            type = op_metrics,
            id = ?BINDING(sid),
            aspect = file_events,
            scope = private
        }
    }}
].
