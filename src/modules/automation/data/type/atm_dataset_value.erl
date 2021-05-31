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
-export([assert_meets_constraints/3]).

%% atm_tree_forest_container_iterator callbacks
-export([
    list_children/4, check_object_existence/2,
    initial_listing_options/0,
    encode_listing_options/1, decode_listing_options/1
]).

-type object_id() :: dataset:id().
-type list_opts() :: atm_tree_forest_container_iterator:list_opts().

%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================

-spec assert_meets_constraints(
    atm_workflow_execution_ctx:record(),
    atm_api:item(),
    atm_data_type:value_constraints()
) ->
    ok | no_return().
assert_meets_constraints(AtmWorkflowExecutionCtx, Value, _ValueConstraints) when is_binary(Value) ->
    try
        case check_object_existence(AtmWorkflowExecutionCtx, Value) of
            true -> ok;
            false -> ?ERROR_NOT_FOUND
        end
    of
        ok -> ok;
        {error, _} = Error -> throw(Error)
    catch _:_ ->
        throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_dataset_type))
    end;
assert_meets_constraints(_AtmWorkflowExecutionCtx, Value, _ValueConstraints) ->
    throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_dataset_type)).


-spec list_children(atm_workflow_execution_ctx:record(), object_id(), list_opts(), non_neg_integer()) ->
    {[{object_id(), binary()}], [object_id()], list_opts(), IsLast :: boolean()} | no_return().
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


-spec check_object_existence(atm_workflow_execution_ctx:record(), object_id()) -> boolean().
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
    
