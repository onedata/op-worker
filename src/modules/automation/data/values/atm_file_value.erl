%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_value` and `atm_tree_forest_container_iterator`
%%% functionality for `atm_file_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_file_value).
-author("Michal Stanisz").

-behaviour(atm_value).
-behaviour(atm_tree_forest_store_container_iterator).

-include("modules/automation/atm_execution.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% atm_value callbacks
-export([
    validate_constraints/3,
    to_store_item/2,
    from_store_item/3,
    describe_store_item/3,
    transform_to_data_spec_conformant/3
]).

%% atm_tree_forest_store_container_iterator callbacks
-export([list_tree/4]).


%%%===================================================================
%%% atm_value callbacks
%%%===================================================================


-spec validate_constraints(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_file_data_spec:record()
) ->
    ok | no_return().
validate_constraints(AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    resolve_internal(AtmWorkflowExecutionAuth, Value, AtmDataSpec#atm_file_data_spec{
        attributes = []  %% validate constraints but don't fetch any attrs
    }),
    ok.


-spec to_store_item(automation:item(), atm_file_data_spec:record()) ->
    atm_store:item().
to_store_item(#{<<"file_id">> := ObjectId}, _AtmDataSpec) ->
    {ok, Guid} = file_id:objectid_to_guid(ObjectId),
    Guid.


-spec from_store_item(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_file_data_spec:record()
) ->
    {ok, automation:item()}.
from_store_item(_AtmWorkflowExecutionAuth, Guid, _AtmDataSpec) ->
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
    {ok, #{<<"file_id">> => ObjectId}}.


-spec describe_store_item(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_file_data_spec:record()
) ->
    {ok, automation:item()}.
describe_store_item(AtmWorkflowExecutionAuth, Guid, _AtmDataSpec) ->
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),

    case lfm:stat(SessionId, ?FILE_REF(Guid)) of
        {ok, FileAttrs} -> {ok, file_attr_translator:to_json(FileAttrs)};
        {error, Errno} -> ?ERROR_POSIX(Errno)
    end.


-spec transform_to_data_spec_conformant(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_file_data_spec:record()
) ->
    automation:item().
transform_to_data_spec_conformant(AtmWorkflowExecutionAuth, Value, AtmDataSpec = #atm_file_data_spec{
    attributes = Attrs
}) ->
    FileAttrs = resolve_internal(AtmWorkflowExecutionAuth, Value, AtmDataSpec),

    maps:with(
        lists:map(fun str_utils:to_binary/1, Attrs),
        file_attr_translator:to_json(FileAttrs)
    ).


%%%===================================================================
%%% atm_tree_forest_store_container_iterator callbacks
%%%===================================================================


-spec list_tree(
    atm_workflow_execution_auth:record(),
    recursive_listing:pagination_token() | undefined,
    atm_store:item(),
    atm_store_container_iterator:batch_size()
) ->
    {[automation:item()], recursive_listing:pagination_token() | undefined}.
list_tree(AtmWorkflowExecutionAuth, PrevToken, CompressedRoot, BatchSize) ->
    list_internal(AtmWorkflowExecutionAuth, CompressedRoot,
        maps_utils:remove_undefined(#{
            limit => BatchSize, 
            pagination_token => PrevToken,
            include_directories => true
        })
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec list_internal(atm_workflow_execution_auth:record(), atm_store:item(), dir_req:recursive_listing_opts()) ->
    {[automation:item()], recursive_listing:pagination_token() | undefined}.
list_internal(AtmWorkflowExecutionAuth, CompressedRoot, Opts) ->
    UserCtx = user_ctx:new(atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth)),
    FileCtx0 = file_ctx:new_by_guid(CompressedRoot),
    try
        {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx0),
        AccessRequirements = case IsDir of
            true -> [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, ?list_container_mask)];
            false -> [?TRAVERSE_ANCESTORS]
        end,
        {_CanonicalChildrenWhiteList, FileCtx2} = fslogic_authz:ensure_authorized_readdir(
            UserCtx, FileCtx1, AccessRequirements
        ),

        #recursive_listing_result{
            entries = Entries,
            pagination_token = PaginationToken
        } = recursive_listing:list(recursive_file_listing_node, UserCtx, FileCtx2, kv_utils:move_found(
            include_directories, include_branching_nodes, Opts
        )),
        MappedEntries = lists:map(fun({_, ChildFileCtx}) ->
            ChildGuid = file_ctx:get_logical_guid_const(ChildFileCtx),
            {ok, ChildObjectId} = file_id:guid_to_objectid(ChildGuid),
            #{<<"file_id">> => ChildObjectId}
        end, Entries),

        {MappedEntries, PaginationToken}
    catch _:Error ->
        case datastore_runner:normalize_error(Error) of
            {badmatch, not_found} -> {[], undefined};
            not_found -> {[], undefined};
            ?EPERM -> {[], undefined};
            ?EACCES -> {[], undefined};
            _ -> error(Error)
        end
    end.


%% @private
-spec resolve_internal(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_file_data_spec:record()
) ->
    lfm_attrs:file_attributes() | no_return().
resolve_internal(AtmWorkflowExecutionAuth, #{<<"file_id">> := ObjectId} = Value, AtmDataSpec) ->
    try
        {ok, Guid} = file_id:objectid_to_guid(ObjectId),

        check_in_space_constraint(AtmWorkflowExecutionAuth, Guid),

        %% TODO VFS-10945 refactor attr_req/allow to select attributes to fetch
        FileAttrs = fetch_attributes(AtmWorkflowExecutionAuth, Guid),

        check_file_type_constraint(FileAttrs, AtmDataSpec),

        FileAttrs
    catch
        throw:{unverified_constraints, UnverifiedConstraints} ->
            throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(Value, atm_file_type, UnverifiedConstraints));
        throw:Error ->
            throw(Error);
        _:_ ->
            throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_file_type))
    end.


%% @private
-spec check_in_space_constraint(atm_workflow_execution_auth:record(), file_id:file_guid()) ->
    ok | no_return().
check_in_space_constraint(AtmWorkflowExecutionAuth, FileGuid) ->
    SpaceId = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),

    case file_id:guid_to_space_id(FileGuid) of
        SpaceId -> ok;
        _ -> throw({unverified_constraints, #{<<"inSpace">> => SpaceId}})
    end.


%% @private
-spec fetch_attributes(atm_workflow_execution_auth:record(), file_id:file_guid()) ->
    lfm_attrs:file_attributes() | no_return().
fetch_attributes(AtmWorkflowExecutionAuth, FileGuid) ->
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),

    case lfm:stat(SessionId, ?FILE_REF(FileGuid)) of
        {ok, FileAttrs} ->
            FileAttrs;
        {error, Errno} ->
            case fslogic_errors:is_access_error(Errno) of
                true -> throw({unverified_constraints, #{<<"hasAccess">> => true}});
                false -> throw(?ERROR_POSIX(Errno))
            end
    end.


%% @private
-spec check_file_type_constraint(lfm_attrs:file_attributes(), atm_file_data_spec:record()) ->
    ok | no_return().
check_file_type_constraint(_, #atm_file_data_spec{file_type = 'ANY'}) ->
    ok;
check_file_type_constraint(#file_attr{type = FileType}, #atm_file_data_spec{file_type = FileType}) ->
    ok;
check_file_type_constraint(_, #atm_file_data_spec{file_type = ConstraintType}) ->
    throw({unverified_constraints, #{<<"fileType">> => str_utils:to_binary(ConstraintType)}}).
