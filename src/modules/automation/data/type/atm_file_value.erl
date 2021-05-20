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

-include("modules/automation/atm_tmp.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_data_validator callbacks
-export([assert_meets_constraints/2]).

%% atm_tree_forest_container_iterator callbacks
-export([list_children/2, check_object_existence/1]).

-type object_id() :: file_id:file_guid().
-type list_opts() :: atm_tree_forest_container_iterator:list_opts().

%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================

-spec assert_meets_constraints(atm_api:item(), atm_data_type:value_constraints()) ->
    ok | no_return().
assert_meets_constraints(Value, _ValueConstraints) when is_binary(Value) ->
    case check_object_existence(Value) of
        true -> ok;
        false -> throw(?ERROR_NOT_FOUND)
    end;
assert_meets_constraints(Value, _ValueConstraints) ->
    throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_file_type)).


-spec list_children(object_id(), list_opts()) -> 
    {[{object_id(), binary()}], [object_id()], list_opts()} | no_return().
list_children(Guid, ListOpts) ->
    try
        list_children_unsafe(Guid, ListOpts)
    catch _:Error ->
        case datastore_runner:normalize_error(Error) of
            ?EACCES -> 
                {[], [], #{is_last => true}};
            ?EPERM ->
                {[], [], #{is_last => true}};
            ?ENOENT ->
                {[], [], #{is_last => true}};
            _ -> 
                throw(Error)
        end
    end.


-spec check_object_existence(object_id()) -> boolean().
check_object_existence(FileGuid) ->
    %% @TODO VFS-7676 use user credentials
    case lfm:stat(?ROOT_SESS_ID, ?FILE_REF(FileGuid)) of
        {ok, _} -> true;
        {error, ?ENOENT} -> false
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec list_children_unsafe(object_id(), list_opts()) ->
    {[{object_id(), binary()}], [object_id()], list_opts()}.
list_children_unsafe(Guid, ListOpts) ->
    case file_ctx:is_dir(file_ctx:new_by_guid(Guid)) of
        {false, _Ctx} ->
            {[], [], #{is_last => true}};
        {true, Ctx} ->
            {Children, ExtendedListInfo, _Ctx1} = dir_req:get_children_ctxs(
                %% @TODO VFS-7676 use user credentials
                user_ctx:new(session_utils:root_session_id()),
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
            {lists:reverse(ReversedDirsAndNames), lists:reverse(ReversedFiles), ExtendedListInfo}
    end.
