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
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([
    translate_value/2,
    translate_resource/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_value(gri:gri(), Value :: term()) -> gs_protocol:data().
translate_value(#gri{aspect = audit_log}, ListedEntries) ->
    ListedEntries;
translate_value(#gri{aspect = {transfer_stats_collection_schema, _}}, TimeSeriesCollectionSchema) ->
    jsonable_record:to_json(TimeSeriesCollectionSchema);
translate_value(#gri{aspect = {transfer_stats_collection, _}}, TSBrowseResult) ->
    ts_browse_result:to_json(TSBrowseResult).


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = private}, QosDetails) ->
    #{
        <<"expression">> := Expression,
        <<"replicasNum">> := ReplicasNum,
        <<"fileId">> := QosRootFileObjectId,
        <<"status">> := Status
    } = QosDetails,
    {ok, FileGuid} = file_id:objectid_to_guid(QosRootFileObjectId),
    #{
        <<"expressionRpn">> => qos_expression:to_rpn(Expression),
        <<"replicasNum">> => ReplicasNum,
        <<"file">> => gri:serialize(#gri{
            type = op_file,
            id = FileGuid,
            aspect = instance,
            scope = private
        }),
        <<"status">> => Status
    }.
