%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% provider entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([translate_resource/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = protected}, #od_provider{
    name = Name,
    latitude = Latitude,
    longitude = Longitude,
    online = Online
}) ->
    #{
        <<"name">> => Name,
        <<"latitude">> => Latitude,
        <<"longitude">> => Longitude,
        <<"online">> => Online
    }.
