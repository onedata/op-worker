%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator`, `atm_tree_forest_container_iterator` 
%%% and `atm_data_compressor` functionality for `atm_file_type`.
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
-export([validate/3]).

%% atm_tree_forest_container_iterator callbacks
-export([
    list_children/4, exists/2,
    initial_listing_options/0,
    encode_listing_options/1, decode_listing_options/1
]).

%% atm_data_compressor callbacks
-export([compress/1, expand/2]).

-type list_opts() :: file_meta:list_opts().

%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================

-spec validate(
    atm_workflow_execution_ctx:record(),
    atm_api:item(),
    atm_data_type:value_constraints()
) ->
    ok | no_return().
validate(AtmWorkflowExecutionCtx, #{<<"file_id">> := ObjectId} = Value, _ValueConstraints) ->
    SpaceId = atm_workflow_execution_ctx:get_space_id(AtmWorkflowExecutionCtx),
    try
        {ok, Guid} = file_id:objectid_to_guid(ObjectId),
        case file_id:guid_to_space_id(Guid) of
            SpaceId ->
                case exists(AtmWorkflowExecutionCtx, Guid) of
                    true -> ok;
                    false -> ?ERROR_NOT_FOUND
                end;
            _ -> 
                ?ERROR_NOT_FOUND
        end
    of
        ok -> ok;
        {error, _} = Error -> throw(Error)
    catch _:_ ->
        throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_file_type))
    end;
validate(_AtmWorkflowExecutionCtx, Value, _ValueConstraints) ->
    throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_file_type)).


%%%===================================================================
%%% atm_tree_forest_container_iterator callbacks
%%%===================================================================

-spec list_children(atm_workflow_execution_ctx:record(), file_id:file_guid(), list_opts(), non_neg_integer()) ->
    {[{file_id:file_guid(), file_meta:name()}], [file_id:file_guid()], list_opts(), IsLast :: boolean()} | no_return().
list_children(AtmWorkflowExecutionCtx, Guid, ListOpts, BatchSize) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    try
        list_children_unsafe(SessionId, Guid, ListOpts#{size => BatchSize})
    catch _:Error ->
        case atm_data_utils:is_error_ignored(datastore_runner:normalize_error(Error)) of
            true ->
                {[], [], #{}, true};
            _ -> 
                throw(Error)
        end
    end.


-spec exists(atm_workflow_execution_ctx:record(), file_id:file_guid()) -> boolean().
exists(AtmWorkflowExecutionCtx, FileGuid) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    case lfm:stat(SessionId, ?FILE_REF(FileGuid)) of
        {ok, _} -> true;
        {error, Type} = Error -> 
            case atm_data_utils:is_error_ignored(Type) of
                true -> false;
                false -> throw(Error)
            end
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
%%% atm_data_compressor callbacks
%%%===================================================================

-spec compress(atm_api:item()) -> file_id:file_guid().
compress(#{<<"file_id">> := ObjectId}) ->
    {ok, Guid} = file_id:objectid_to_guid(ObjectId),
    Guid.


-spec expand(atm_workflow_execution_ctx:record(), file_id:file_guid()) -> 
    {ok, atm_api:item()} | {error, term()}.
expand(AtmWorkflowExecutionCtx, Guid) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    case lfm:stat(SessionId, #file_ref{guid = Guid}) of
        {ok, FileAttrs} -> {ok, translate_file_attrs(FileAttrs)};
        {error, _} = Error -> Error 
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec list_children_unsafe(session:id(), file_id:file_guid(), list_opts()) ->
    {[{file_id:file_guid(), file_meta:name()}], [file_id:file_guid()], list_opts(), boolean()}.
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


%% @private
-spec translate_file_attrs(lfm_attrs:file_attributes()) -> atm_api:item().
translate_file_attrs(#file_attr{
    guid = Guid, 
    name = Name, 
    mode = Mode, 
    parent_guid = ParentGuid, 
    uid = Uid, 
    gid = Gid, 
    atime = Atime, 
    mtime = Mtime, 
    ctime = Ctime, 
    type = Type, 
    size = Size, 
    shares = Shares, 
    provider_id = ProviderId, 
    owner_id = Owner_id, 
    nlink = Nlink
}) ->
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
    {ok, ParentObjectId} = file_id:guid_to_objectid(ParentGuid),
    #{
        <<"file_id">> => ObjectId,
        <<"name">> => Name,
        <<"mode">> => Mode,
        <<"parent_id">> => ParentObjectId,
        <<"storage_user_id">> => Uid,
        <<"storage_group_id">> => Gid,
        <<"atime">> => Atime,
        <<"mtime">> => Mtime,
        <<"ctime">> => Ctime,
        <<"type">> => Type,
        <<"size">> => Size,
        <<"shares">> => Shares,
        <<"provider_id">> => ProviderId,
        <<"owner_id">> => Owner_id,
        <<"hardlinks_count">> => Nlink
    }.
