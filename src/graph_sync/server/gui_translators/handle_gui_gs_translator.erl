%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% handle entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(handle_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([translate_resource/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = public}, #{
    <<"url">> := PublicHandle,
    <<"metadataPrefix">> := MetadataPrefix,
    <<"metadataString">> := MetadataString,
    <<"handleServiceId">> := HandleServiceId
}) ->
    #{
        <<"url">> => PublicHandle,
        <<"metadataPrefix">> => MetadataPrefix,
        <<"metadataString">> => MetadataString,
        <<"handleService">> => gri:serialize(#gri{
            type = op_handle_service,
            id = HandleServiceId,
            aspect = instance,
            scope = public
        })
    }.
