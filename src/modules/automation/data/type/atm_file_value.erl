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
-behaviour(atm_data_compressor).
-behaviour(atm_tree_forest_store_container_iterator).

-include("modules/automation/atm_execution.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% atm_data_validator callbacks
-export([assert_meets_constraints/3]).

%% atm_tree_forest_store_container_iterator callbacks
-export([
    list_tree/4
]).

%% atm_data_compressor callbacks
-export([compress/2, expand/3]).


%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================


-spec assert_meets_constraints(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_file_data_spec:record()
) ->
    ok | no_return().
assert_meets_constraints(AtmWorkflowExecutionAuth, #{<<"file_id">> := ObjectId} = Value, AtmDataSpec) ->
    try
        {ok, Guid} = file_id:objectid_to_guid(ObjectId),
        FileAttrs = check_implicit_constraints(AtmWorkflowExecutionAuth, Guid),
        check_explicit_constraints(FileAttrs, AtmDataSpec)
    catch
        throw:{unverified_constraints, UnverifiedConstraints} ->
            throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(Value, atm_file_type, UnverifiedConstraints));
        throw:Error ->
            throw(Error);
        _:_ ->
            throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_file_type))
    end.


%%%===================================================================
%%% atm_tree_forest_store_container_iterator callbacks
%%%===================================================================

-spec list_tree(
    atm_workflow_execution_auth:record(),
    recursive_listing:pagination_token() | undefined,
    atm_value:compressed(),
    atm_store_container_iterator:batch_size()
) ->
    {[atm_value:expanded()], recursive_listing:pagination_token() | undefined}.
list_tree(AtmWorkflowExecutionAuth, PrevToken, CompressedRoot, BatchSize) ->
    list_internal(AtmWorkflowExecutionAuth, CompressedRoot,
        maps_utils:remove_undefined(#{
            limit => BatchSize, 
            pagination_token => PrevToken,
            include_directories => true
        })
    ).


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


-spec compress(atm_value:expanded(), atm_file_data_spec:record()) ->
    file_id:file_guid().
compress(#{<<"file_id">> := ObjectId}, _AtmDataSpec) ->
    {ok, Guid} = file_id:objectid_to_guid(ObjectId),
    Guid.


-spec expand(
    atm_workflow_execution_auth:record(),
    file_id:file_guid(),
    atm_file_data_spec:record()
) ->
    {ok, atm_value:expanded()} | {error, term()}.
expand(AtmWorkflowExecutionAuth, Guid, _AtmDataSpec) ->
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),

    case lfm:stat(SessionId, ?FILE_REF(Guid)) of
        {ok, FileAttrs} -> {ok, file_attr_translator:to_json(FileAttrs)};
        {error, Errno} -> ?ERROR_POSIX(Errno)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec list_internal(atm_workflow_execution_auth:record(), atm_value:compressed(), dir_req:recursive_listing_opts()) ->
    {[atm_value:expanded()], recursive_listing:pagination_token() | undefined}.
list_internal(AtmWorkflowExecutionAuth, CompressedRoot, Opts) ->
    UserCtx = user_ctx:new(atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth)),
    FileCtx = file_ctx:new_by_guid(CompressedRoot),
    try
        #fuse_response{fuse_response = #recursive_listing_result{
            entries = Entries, pagination_token = PaginationToken}
        } = dir_req:list_recursively(UserCtx, FileCtx, Opts, [size]),
        MappedEntries = lists:map(fun({_Path, FileAttrs}) ->
            file_attr_translator:to_json(FileAttrs)
        end, Entries),
        {MappedEntries, PaginationToken}
    catch _:Error ->
        case datastore_runner:normalize_error(Error) of
            not_found -> {[], undefined};
            ?EPERM -> {[], undefined};
            ?EACCES -> {[], undefined};
            _ -> error(Error)
        end
    end.


%% @private
-spec check_implicit_constraints(atm_workflow_execution_auth:record(), file_id:file_guid()) ->
    lfm_attrs:file_attributes() | no_return().
check_implicit_constraints(AtmWorkflowExecutionAuth, FileGuid) ->
    SpaceId = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),

    case file_id:guid_to_space_id(FileGuid) of
        SpaceId -> ok;
        _ -> throw({unverified_constraints, #{<<"inSpace">> => SpaceId}})
    end,

    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),

    case lfm:stat(SessionId, ?FILE_REF(FileGuid)) of
        {ok, FileAttrs} ->
            FileAttrs;
        {error, Errno} ->
            case fslogic_errors:is_access_error(Errno) of
                true ->
                    throw({unverified_constraints, #{<<"hasAccess">> => true}});
                false ->
                    throw(?ERROR_POSIX(Errno))
            end
    end.


%% @private
-spec check_explicit_constraints(lfm_attrs:file_attributes(), atm_file_data_spec:record()) ->
    ok | no_return().
check_explicit_constraints(_, #atm_file_data_spec{file_type = 'ANY'}) ->
    ok;
check_explicit_constraints(#file_attr{type = FileType}, #atm_file_data_spec{file_type = FileType}) ->
    ok;
check_explicit_constraints(_, #atm_file_data_spec{file_type = ConstraintType}) ->
    throw({unverified_constraints, #{<<"file_type">> => str_utils:to_binary(ConstraintType)}}).
