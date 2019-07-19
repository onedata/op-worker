%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module performs qos-related operations of lfm_submodules.
%%% @end
%%%--------------------------------------------------------------------
-module(lfm_qos).
-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("common_test/include/ct.hrl").

%% API
-export([add_qos/4, get_qos_details/2, remove_qos/2, get_file_qos/2,
    check_qos_fulfilled/2, check_qos_fulfilled/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds new qos for file or directory, returns QoS ID.
%% @end
%%--------------------------------------------------------------------
-spec add_qos(session:id(), logical_file_manager:file_key(), binary(), qos_entry:replicas_num()) ->
    {ok, qos_entry:id()} | logical_file_manager:error_reply().
add_qos(SessId, FileKey, Expression, ReplicasNum) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, Guid,
        #add_qos{expression = Expression, replicas_num = ReplicasNum},
        fun(#qos_id{id = QosId}) ->
            {ok, QosId}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Gets information about QoS defined for file.
%% @end
%%--------------------------------------------------------------------
-spec get_file_qos(session:id(), logical_file_manager:file_key()) ->
    {ok, {file_qos:qos_list(), file_qos:target_storages()}} | logical_file_manager:error_reply().
get_file_qos(SessId, FileKey) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, Guid, #get_effective_file_qos{},
        fun(#effective_file_qos{qos_list = QosList, target_storages = TargetStorages}) ->
            {ok, {QosList, TargetStorages}}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Get details of specific qos.
%% @end
%%--------------------------------------------------------------------
-spec get_qos_details(session:id(), qos_entry:id()) ->
    {ok, qos_entry:record()} | logical_file_manager:error_reply().
get_qos_details(SessId, QosId) ->
    case qos_entry:get_file_guid(QosId) of
        {ok, FileGuid} ->
            remote_utils:call_fslogic(SessId, provider_request, FileGuid, #get_qos{id = QosId},
                fun(Resp) ->
                    {ok, #qos_entry{
                        file_guid = Resp#get_qos_resp.file_guid,
                        expression = Resp#get_qos_resp.expression,
                        replicas_num = Resp#get_qos_resp.replicas_num,
                        status = Resp#get_qos_resp.status
                    }}
                end);
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Remove single qos.
%% @end
%%--------------------------------------------------------------------
-spec remove_qos(session:id(), qos_entry:id()) -> ok | logical_file_manager:error_reply().
remove_qos(SessId, QosId) ->
    case qos_entry:get_file_guid(QosId) of
        {ok, FileGuid} ->
            remote_utils:call_fslogic(SessId, provider_request, FileGuid, #remove_qos{id = QosId},
                fun(_) -> ok end);
        {error, _} = Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check if given qos is fulfilled.
%% @end
%%--------------------------------------------------------------------
-spec check_qos_fulfilled(session:id(), qos_entry:id() | [qos_entry:id()]) -> boolean().
check_qos_fulfilled(SessId, QosList) ->
    check_qos_fulfilled(SessId, QosList, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Check if given qos is fulfilled for given file.
%% @end
%%--------------------------------------------------------------------
-spec check_qos_fulfilled(session:id(), qos_entry:id() | [qos_entry:id()],
    logical_file_manager:file_key()) -> boolean() | lfm:error_reply().
check_qos_fulfilled(SessId, QosList, FileKey) when is_list(QosList) ->
    lists:all(fun(QosId) -> check_qos_fulfilled(SessId, QosId, FileKey) end, QosList);
check_qos_fulfilled(SessId, QosId, undefined) ->
    case get_qos_details(SessId, QosId) of
        {ok, #qos_entry{file_guid = QosOriginFileGuid}} ->
            check_qos_fulfilled(SessId, QosId, {guid, QosOriginFileGuid});
        {error, _} = Error ->
            Error
    end;
check_qos_fulfilled(SessId, QosId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid, #check_qos_fulfillment{qos_id = QosId},
        fun(#qos_fulfillment{fulfilled = FulfillmentStatus}) -> FulfillmentStatus end).

