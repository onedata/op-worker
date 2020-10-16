%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% replica entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_gui_gs_translator).
-author("Lukasz Opiola").
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([
    translate_value/2,
    translate_resource/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_value(gri:gri(), Value :: term()) -> gs_protocol:data().
translate_value(#gri{aspect = As}, TransferId) when
    As =:= instance;
    As =:= replicate_by_view;
    As =:= evict_by_view
->
    #{<<"transferId">> => TransferId}.


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = distribution, scope = private}, Distribution) ->
    file_gui_gs_translator:translate_distribution(Distribution).
