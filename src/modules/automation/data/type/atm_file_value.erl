%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` functionality for
%%% `atm_file_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_file_value).
-author("Michal Stanisz").

-behaviour(atm_data_validator).
-behaviour(atm_tree_forest_container_iterator).

-include("modules/automation/atm_execution.hrl").
-include("modules/automation/atm_tmp.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_data_validator callbacks
-export([assert_meets_constraints/3]).

%% atm_tree_forest_container_iterator callbacks
-export([
    list_children/4, check_object_existence/2, 
    initial_listing_options/0,
    encode_listing_options/1, decode_listing_options/1
]).

-type object_id() :: file_id:file_guid().
-type list_opts() :: file_meta:list_opts().

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
    SpaceId = atm_workflow_execution_ctx:get_space_id(AtmWorkflowExecutionCtx),
    try
        case file_id:guid_to_space_id(Value) of
            SpaceId -> ok;
            _ -> ?ERROR_NOT_FOUND
        end,
        case check_object_existence(AtmWorkflowExecutionCtx, Value) of
            true -> ok;
            false -> ?ERROR_NOT_FOUND
        end
    of
        ok -> ok;
        {error, _} = Error -> throw(Error)
    catch _:_ ->
        throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_file_type))
    end;
assert_meets_constraints(_AtmWorkflowExecutionCtx, Value, _ValueConstraints) ->
    throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_file_type)).


-spec list_children(atm_workflow_execution_ctx:record(), object_id(), list_opts(), non_neg_integer()) ->
    {[{object_id(), file_meta:name()}], [object_id()], list_opts(), IsLast :: boolean()} | no_return().
list_children(AtmWorkflowExecutionCtx, Guid, ListOpts, BatchSize) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    try
        list_children_unsafe(SessionId, Guid, ListOpts#{size => BatchSize})
    catch _:Error ->
        case datastore_runner:normalize_error(Error) of
            ?EACCES ->
                {[], [], #{}, true};
            ?EPERM ->
                {[], [], #{}, true};
            ?ENOENT ->
                {[], [], #{}, true};
            _ -> 
                throw(Error)
        end
    end.


-spec check_object_existence(atm_workflow_execution_ctx:record(), object_id()) -> boolean().
check_object_existence(AtmWorkflowExecutionCtx, FileGuid) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    case lfm:stat(SessionId, ?FILE_REF(FileGuid)) of
        {ok, _} -> true;
        {error, ?EACCES} -> false;
        {error, ?EPERM} -> false;
        {error, ?ENOENT} -> false;
        {error, _} = Error -> throw(Error)
    end.


-spec initial_listing_options() -> list_opts().
initial_listing_options() ->
    #{
        last_name => <<>>,
        last_tree => <<>>
    }.


-spec encode_listing_options(list_opts()) -> json_utils:json_term().
encode_listing_options(#{last_name := LastName, last_tree := LastTree}) ->
    #{
        <<"last_name">> => LastName,
        <<"last_tree">> => LastTree
    }.


-spec decode_listing_options(json_utils:json_term()) -> list_opts().
decode_listing_options(#{<<"last_name">> := LastName, <<"last_tree">> := LastTree}) ->
    #{
        last_name => LastName,
        last_tree => LastTree
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec list_children_unsafe(session:id(), object_id(), list_opts()) ->
    {[{object_id(), file_meta:name()}], [object_id()], list_opts(), boolean()}.
list_children_unsafe(SessionId, Guid, ListOpts) ->
    case file_ctx:is_dir(file_ctx:new_by_guid(Guid)) of
        {false, _Ctx} ->
            {[], [], #{}, true};
        {true, Ctx} ->
            {Children, ExtendedListInfo, _Ctx1} = dir_req:get_children_ctxs(
                user_ctx:new(SessionId),
                Ctx,
                ListOpts),
            {ReversedDirsAndNames, ReversedFiles} = lists:foldl(fun(ChildCtx, {DirsSoFar, FilesSoFar}) ->
                case file_ctx:is_dir(ChildCtx) of
                    {true, ChildCtx1} ->
                        {Name, ChildCtx2} = file_ctx:get_aliased_name(ChildCtx1, undefined),
                        {[{file_ctx:get_logical_guid_const(ChildCtx2), Name} | DirsSoFar], FilesSoFar};
                    {false, _} ->
                        {DirsSoFar, [file_ctx:get_logical_guid_const(ChildCtx) | FilesSoFar]}
                end
            end, {[], []}, Children),
            {
                lists:reverse(ReversedDirsAndNames), 
                lists:reverse(ReversedFiles), 
                maps:without([is_last], ExtendedListInfo), 
                maps:get(is_last, ExtendedListInfo)
            }
    end.
