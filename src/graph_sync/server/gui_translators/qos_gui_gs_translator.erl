%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% QoS entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_gui_gs_translator).
-author("Michal Stanisz").

-include("middleware/middleware.hrl").
-include("modules/fslogic/file_details.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    translate_resource/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = private}, QosDetails) ->
    #{
        <<"expressionRpn">> := ExpressionRpn,
        <<"replicasNum">> := ReplicasNum,
        <<"fileId">> := QosRootFileObjectId,
        <<"fulfilled">> := Fulfilled
    } = QosDetails,
    {ok, FileGuid} = file_id:objectid_to_guid(QosRootFileObjectId),
    #{
        <<"expressionRpn">> => ExpressionRpn,
        <<"replicasNum">> => ReplicasNum,
        <<"file">> => gri:serialize(#gri{
            type = op_file,
            id = FileGuid,
            aspect = instance,
            scope = private
        }),
        <<"fulfilled">> => Fulfilled
    }.