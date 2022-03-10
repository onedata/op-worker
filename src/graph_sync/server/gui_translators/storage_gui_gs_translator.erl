%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% storage entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_gui_gs_translator).
-author("Lukasz Opiola").

-include("middleware/middleware.hrl").

%% API
-export([
    translate_resource/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = shared}, #{
    <<"name">> := Name,
    <<"provider">> := ProviderId
}) ->
    #{
        <<"name">> => Name,
        <<"provider">> => gri:serialize(#gri{
            type = op_provider,
            id = ProviderId,
            aspect = instance,
            scope = protected
        })
    }.
