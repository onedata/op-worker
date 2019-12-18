%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% share entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(share_gui_gs_translator).
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
translate_value(#gri{aspect = shared_dir}, ShareId) ->
    #{<<"shareId">> => ShareId}.


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = private}, #{
    <<"name">> := ShareName,
    <<"publicUrl">> := SharePublicUrl,
    <<"rootFileId">> := RootFileGuid
}) ->
    #{
        <<"name">> => ShareName,
        <<"publicUrl">> => SharePublicUrl,
        <<"rootFile">> => gri:serialize(#gri{
            type = op_file,
            id = RootFileGuid,
            aspect = instance,
            scope = private
        })
    }.
