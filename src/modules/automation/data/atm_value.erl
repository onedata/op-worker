%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles operations on automation values.
%%% Atm values are Oneprovider specific implementation of atm_data_type's.
%%% Each must implement `atm_data_validator` behaviour and, if kept in store,
%%% also `atm_data_compressor` behaviour (each value is saved in store in its
%%% compressed form and is expanded upon listing).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_value).
-author("Michal Stanisz").

-include("modules/automation/atm_tmp.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([validate/3]).
-export([compress/2, expand/3, expand_list/3]).


-type compressed() :: term().
-type expanded() :: automation:item().

-export_type([compressed/0, expanded/0]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec validate(atm_workflow_execution_ctx:record(), expanded(), atm_data_spec:record()) ->
    ok | no_return().
validate(AtmWorkflowExecutionCtx, Value, AtmDataSpec) ->
    AtmDataType = atm_data_spec:get_type(AtmDataSpec),

    case atm_data_type:is_instance(AtmDataType, Value) of
        true ->
            Module = get_callback_module(AtmDataType),
            ValueConstraints = atm_data_spec:get_value_constraints(AtmDataSpec),
            Module:assert_meets_constraints(AtmWorkflowExecutionCtx, Value, ValueConstraints);
        false ->
            throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, AtmDataType))
    end.


-spec compress(expanded(), atm_data_spec:record()) -> compressed() | no_return().
compress(Value, AtmDataSpec) ->
    Module = get_callback_module(atm_data_spec:get_type(AtmDataSpec)),
    Module:compress(Value).


-spec expand(atm_workflow_execution_ctx:record(), compressed(), atm_data_spec:record()) ->
    {ok, expanded()} | {error, term()}.
expand(AtmWorkflowExecutionCtx, Value, AtmDataSpec) ->
    Module = get_callback_module(atm_data_spec:get_type(AtmDataSpec)),
    Module:expand(AtmWorkflowExecutionCtx, Value).


-spec expand_list(atm_workflow_execution_ctx:record(), [compressed()] | compressed(), atm_data_spec:record()) ->
    {ok, [expanded()]} | {error, term()}.
expand_list(AtmWorkflowExecutionCtx, CompressedItems, AtmDataSpec) ->
    lists:filtermap(fun(CompressedItem) ->
        case atm_value:expand(AtmWorkflowExecutionCtx, CompressedItem, AtmDataSpec) of
            {ok, ExpandedItem} -> {true, ExpandedItem};
            {error, _} -> false
        end
    end, utils:ensure_list(CompressedItems)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_callback_module(atm_data_type:type()) -> module().
get_callback_module(atm_dataset_type) -> atm_dataset_value;
get_callback_module(atm_file_type) -> atm_file_value;
get_callback_module(atm_integer_type) -> atm_integer_value;
get_callback_module(atm_object_type) -> atm_object_value;
get_callback_module(atm_onedatafs_credentials_type) -> atm_onedatafs_credentials_value;
get_callback_module(atm_string_type) -> atm_string_value.
