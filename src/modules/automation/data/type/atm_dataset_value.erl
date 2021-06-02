%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` functionality for
%%% `atm_dataset_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_dataset_value).
-author("Michal Stanisz").

-behaviour(atm_data_validator).
-behaviour(atm_tree_forest_container_iterator).

-include("modules/automation/atm_execution.hrl").
-include("modules/automation/atm_tmp.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% atm_data_validator callbacks
-export([sanitize/3, map_value/2]).

%% atm_tree_forest_container_iterator callbacks
-export([
    list_children/4, check_object_existence/2,
    initial_listing_options/0,
    encode_listing_options/1, decode_listing_options/1
]).

-type list_opts() :: atm_tree_forest_container_iterator:list_opts().

%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================

-spec sanitize(
    atm_workflow_execution_ctx:record(),
    atm_api:item(),
    atm_data_type:value_constraints()
) ->
    atm_api:item() | no_return().
sanitize(AtmWorkflowExecutionCtx, #{<<"dataset_id">> := DatasetId} = Value, _ValueConstraints) ->
    try
        case check_object_existence(AtmWorkflowExecutionCtx, DatasetId) of
            true -> {ok, DatasetId};
            false -> ?ERROR_NOT_FOUND
        end
    of
        {ok, D} -> D;
        {error, _} = Error -> throw(Error)
    catch _:_ ->
        throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_dataset_type))
    end;
sanitize(_AtmWorkflowExecutionCtx, Value, _ValueConstraints) ->
    throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_dataset_type)).


-spec list_children(atm_workflow_execution_ctx:record(), dataset:id(), list_opts(), non_neg_integer()) ->
    {[{dataset:id(), binary()}], [dataset:id()], list_opts(), IsLast :: boolean()} | no_return().
list_children(AtmWorkflowExecutionCtx, DatasetId, ListOpts, BatchSize) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    case lfm:list_children_datasets(SessionId, DatasetId, ListOpts#{limit => BatchSize}) of
        {ok, Entries, IsLast} when length(Entries) > 0 ->
            PrevOffset = maps:get(offset, ListOpts),
            ResultEntries = lists:map(fun({Id, Name, _}) -> {Id, Name} end, Entries),
            {ResultEntries, [], #{offset => PrevOffset + length(Entries)}, IsLast};
        {ok, [], _} ->
            {[], [], #{}, true};
        {error, ?EACCES} ->
            {[], [], #{}, true};
        {error, ?EPERM} ->
            {[], [], #{}, true};
        {error, ?ENOENT} ->
            {[], [], #{}, true};
        {error, _} = Error ->
            throw(Error)
    end.


-spec check_object_existence(atm_workflow_execution_ctx:record(), dataset:id()) -> boolean().
check_object_existence(AtmWorkflowExecutionCtx, DatasetId) ->
    SpaceId = atm_workflow_execution_ctx:get_space_id(AtmWorkflowExecutionCtx),
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    case lfm:get_file_eff_dataset_summary(SessionId, #file_ref{guid = file_id:pack_guid(DatasetId, SpaceId)}) of
        {ok, #file_eff_dataset_summary{direct_dataset = DatasetId}} -> true;
        {ok, #file_eff_dataset_summary{direct_dataset = undefined}} -> false;
        {error, ?ENOENT} -> false;
        {error, ?EACCES} -> false;
        {error, ?EPERM} -> false;
        {error, _} = Error -> throw(Error)
    end.


-spec initial_listing_options() -> list_opts().
initial_listing_options() ->
    #{
        offset => 0
    }.


-spec encode_listing_options(list_opts()) -> json_utils:json_term().
encode_listing_options(#{offset := Offset}) ->
    #{<<"offset">> => Offset}.


-spec decode_listing_options(json_utils:json_term()) -> list_opts().
decode_listing_options(#{<<"offset">> := Offset}) ->
    #{offset => Offset}.


-spec map_value(atm_workflow_execution_ctx:record(), dataset:id()) -> {true, atm_api:item()} | false.
map_value(AtmWorkflowExecutionCtx, DatasetId) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    case lfm:get_dataset_info(SessionId, DatasetId) of
        {ok, DatasetInfo} -> {true, map_dataset_info(DatasetInfo)};
        {error, _} -> false
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec map_dataset_info(lfm_datasets:info()) -> atm_api:item().
map_dataset_info(#dataset_info{
    id = DatasetId,
    state = DatasetState,
    root_file_guid = RootFileGuid,
    root_file_path = RootFilePath,
    root_file_type = RootFileType,
    creation_time = CreationTime,
    protection_flags = ProtectionFlags,
    eff_protection_flags = EffProtectionFlags,
    parent = Parent,
    archive_count = ArchiveCount,
    index = Index
}) ->
    #{
        <<"dataset_id">> => DatasetId,
        <<"state">> => DatasetState,
        <<"root_file_guid">> => RootFileGuid,
        <<"root_file_path">> => RootFilePath,
        <<"root_file_type">> => RootFileType,
        <<"creation_time">> => CreationTime,
        <<"protection_flags">> => ProtectionFlags,
        <<"eff_protection_flags">> => EffProtectionFlags,
        <<"parent">> => utils:undefined_to_null(Parent),
        <<"archive_count">> => ArchiveCount,
        <<"index">> => Index
    }.
    
