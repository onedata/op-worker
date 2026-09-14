%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Functions extending #data_spec{} of an API test scenario with bad
%%% file id values and their expected errors.
%%% @end
%%%-------------------------------------------------------------------
-module(api_data_spec_test_utils).
-author("Bartosz Walkowicz").

-include("api/api_test_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("test_utils/initializer.hrl").
-include_lib("ctool/include/posix/errno.hrl").

-export([
    add_file_id_errors_for_operations_available_in_share_mode/3,
    add_file_id_errors_for_operations_available_in_share_mode/4,

    add_file_id_errors_for_operations_not_available_in_share_mode/3,
    add_file_id_errors_for_operations_not_available_in_share_mode/4,

    add_cdmi_id_errors_for_operations_not_available_in_share_mode/4,
    add_cdmi_id_errors_for_operations_not_available_in_share_mode/5,

    replace_enoent_with_error_not_found_in_error_expectations/1,
    maybe_substitute_bad_id/2
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Adds to data_spec() errors for invalid file id's (guid, path, cdmi_id) for
%% either normal and share mode (since operation is available in both modes
%% it is expected that it will have distinct tests for each mode).
%%
%% ATTENTION !!!
%%
%% Bad ids are available under 'bad_id' atom key - test implementation should
%% make sure to substitute them for fileId component in rest path or #gri.id
%% before making test call.
%% @end
%%--------------------------------------------------------------------
-spec add_file_id_errors_for_operations_available_in_share_mode(
    file_id:file_guid(),
    undefined | od_share:id(),
    undefined | onenv_api_test_runner:data_spec()
) ->
    onenv_api_test_runner:data_spec().
add_file_id_errors_for_operations_available_in_share_mode(FileGuid, ShareId, DataSpec) ->
    add_file_id_errors_for_operations_available_in_share_mode(<<"id">>, FileGuid, ShareId, DataSpec).


-spec add_file_id_errors_for_operations_available_in_share_mode(
    IdKey :: binary(),
    file_id:file_guid(),
    undefined | od_share:id(),
    undefined | onenv_api_test_runner:data_spec()
) ->
    onenv_api_test_runner:data_spec().
add_file_id_errors_for_operations_available_in_share_mode(IdKey, FileGuid, ShareId, DataSpec) ->
    InvalidFileIdErrors = get_invalid_file_id_errors(IdKey),
    NonExistentSpaceDirGuid = file_id:pack_share_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID, ShareId),
    SpaceId = file_id:guid_to_space_id(FileGuid),
    {ok, NonExistentSpaceObjectId} = file_id:guid_to_objectid(NonExistentSpaceDirGuid),
    NonExistentSpaceExpError = case ShareId of
        undefined ->
            % For authenticated users it should fail on authorization step
            % (checks if user belongs to space)
            ?ERR_FORBIDDEN;
        _ ->
            % For share request it should fail on validation step
            % (checks if space is supported by provider)
            {error_fun, fun(#api_test_ctx{node = Node}) ->
                ProvId = opw_test_rpc:get_provider_id(Node),
                ?ERR_SPACE_NOT_SUPPORTED_BY(?NOT_SUPPORTED_SPACE_ID, ProvId)
            end}
    end,

    NonExistentFileGuid = file_id:pack_share_guid(<<"InvalidUuid">>, SpaceId, ShareId),
    {ok, NonExistentFileObjectId} = file_id:guid_to_objectid(NonExistentFileGuid),

    BadFileIdErrors = InvalidFileIdErrors ++ [
        {bad_id, NonExistentSpaceObjectId, {rest, NonExistentSpaceExpError}},
        {bad_id, NonExistentSpaceDirGuid, {gs, NonExistentSpaceExpError}},

        % Errors thrown by internal logic (all middleware checks were passed)
        {bad_id, NonExistentFileObjectId, {rest, ?ERR_POSIX(?ENOENT)}},
        {bad_id, NonExistentFileGuid, {gs, ?ERR_POSIX(?ENOENT)}}
    ],

    add_bad_values_to_data_spec(BadFileIdErrors, DataSpec).


%%--------------------------------------------------------------------
%% @doc
%% Adds to data_spec() errors for invalid file id's (guid, path, cdmi_id)
%% for both normal and share mode (since operation is not available in share
%% mode there is no need to write distinct test for share mode - access errors
%% when using share file id can be checked along with other bad_values errors).
%%
%% ATTENTION !!!
%%
%% Bad ids are available under 'bad_id' atom key - test implementation should
%% make sure to substitute them for fileId component in rest path or #gri.id
%% before making test call.
%% @end
%%--------------------------------------------------------------------
-spec add_file_id_errors_for_operations_not_available_in_share_mode(
    file_id:file_guid(),
    undefined | od_share:id(),
    undefined | onenv_api_test_runner:data_spec()
) ->
    onenv_api_test_runner:data_spec().
add_file_id_errors_for_operations_not_available_in_share_mode(FileGuid, ShareId, DataSpec) ->
    add_file_id_errors_for_operations_not_available_in_share_mode(<<"id">>, FileGuid, ShareId, DataSpec).


-spec add_file_id_errors_for_operations_not_available_in_share_mode(
    IdKey :: binary(),
    file_id:file_guid(),
    undefined | od_share:id(),
    undefined | onenv_api_test_runner:data_spec()
) ->
    onenv_api_test_runner:data_spec().
add_file_id_errors_for_operations_not_available_in_share_mode(IdKey, FileGuid, ShareId, DataSpec) ->
    InvalidFileIdErrors = get_invalid_file_id_errors(IdKey),

    NonExistentSpaceDirGuid = file_id:pack_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID),
    {ok, NonExistentSpaceObjectId} = file_id:guid_to_objectid(NonExistentSpaceDirGuid),

    NonExistentSpaceErrors = add_share_file_id_errors_for_operations_not_available_in_share_mode(
        NonExistentSpaceDirGuid, ShareId, [
            % Errors in normal mode - thrown by middleware auth checks
            % (checks whether authenticated user belongs to space)
            {bad_id, NonExistentSpaceObjectId, {rest, ?ERR_FORBIDDEN}},
            {bad_id, NonExistentSpaceDirGuid, {gs, ?ERR_FORBIDDEN}}
        ]
    ),

    SpaceId = file_id:guid_to_space_id(FileGuid),
    NonExistentFileGuid = file_id:pack_guid(<<"InvalidUuid">>, SpaceId),
    {ok, NonExistentFileObjectId} = file_id:guid_to_objectid(NonExistentFileGuid),

    NonExistentFileErrors = add_share_file_id_errors_for_operations_not_available_in_share_mode(
        NonExistentFileGuid, ShareId, [
            % Errors in normal mode - thrown by internal logic
            % (all middleware checks were passed)
            {bad_id, NonExistentFileObjectId, {rest, ?ERR_POSIX(?ENOENT)}},
            {bad_id, NonExistentFileGuid, {gs, ?ERR_POSIX(?ENOENT)}}
        ]
    ),

    ShareFileErrors = add_share_file_id_errors_for_operations_not_available_in_share_mode(
        FileGuid, ShareId, []
    ),

    BadFileIdErrors = lists:flatten([
        InvalidFileIdErrors,
        NonExistentSpaceErrors,
        NonExistentFileErrors,
        ShareFileErrors
    ]),

    add_bad_values_to_data_spec(BadFileIdErrors, DataSpec).


%%--------------------------------------------------------------------
%% @doc
%% Extends data_spec() with file id bad values and errors for operations
%% not available in share mode that provide file id as parameter in data spec map.
%% All added bad values are in cdmi form and are stored under <<"fileId">> key.
%% @end
%%--------------------------------------------------------------------
-spec add_cdmi_id_errors_for_operations_not_available_in_share_mode(
    file_id:file_guid(),
    od_space:id(),
    od_share:id(),
    onenv_api_test_runner:data_spec()
) ->
    onenv_api_test_runner:data_spec().
add_cdmi_id_errors_for_operations_not_available_in_share_mode(FileGuid, SpaceId, ShareId, DataSpec) ->
    add_cdmi_id_errors_for_operations_not_available_in_share_mode(<<"fileId">>, FileGuid, SpaceId, ShareId, DataSpec).


-spec add_cdmi_id_errors_for_operations_not_available_in_share_mode(
    IdKey :: binary(),
    file_id:file_guid(),
    od_space:id(),
    od_share:id(),
    onenv_api_test_runner:data_spec()
) ->
    onenv_api_test_runner:data_spec().
add_cdmi_id_errors_for_operations_not_available_in_share_mode(IdKey, FileGuid, SpaceId, ShareId, DataSpec) ->
    {ok, DummyObjectId} = file_id:guid_to_objectid(<<"DummyGuid">>),

    NonExistentSpaceDirGuid = file_id:pack_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID),
    {ok, NonExistentSpaceObjectId} = file_id:guid_to_objectid(NonExistentSpaceDirGuid),

    NonExistentSpaceShareGuid = file_id:guid_to_share_guid(NonExistentSpaceDirGuid, ShareId),
    {ok, NonExistentSpaceShareObjectId} = file_id:guid_to_objectid(NonExistentSpaceShareGuid),

    NonExistentFileGuid = file_id:pack_guid(<<"InvalidUuid">>, SpaceId),
    {ok, NonExistentFileObjectId} = file_id:guid_to_objectid(NonExistentFileGuid),

    NonExistentFileShareGuid = file_id:guid_to_share_guid(NonExistentFileGuid, ShareId),
    {ok, NonExistentFileShareObjectId} = file_id:guid_to_objectid(NonExistentFileShareGuid),

    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, ShareFileObjectId} = file_id:guid_to_objectid(ShareFileGuid),
    BadFileIdValues = [
        {IdKey, <<"InvalidObjectId">>, ?ERR_BAD_VALUE_IDENTIFIER(IdKey)},
        {IdKey, DummyObjectId, ?ERR_BAD_VALUE_IDENTIFIER(IdKey)},

        % user has no privileges in non existent space and so he should receive ?ERR_FORBIDDEN
        {IdKey, NonExistentSpaceObjectId, ?ERR_FORBIDDEN},
        {IdKey, NonExistentSpaceShareObjectId, ?ERR_FORBIDDEN},

        {IdKey, NonExistentFileObjectId, ?ERR_POSIX(?ENOENT)},

        % operation is not available in share mode - it should result in ?ENOTSUP
        {IdKey, ShareFileObjectId, ?ERR_POSIX(?ENOTSUP)},
        {IdKey, NonExistentFileShareObjectId, ?ERR_POSIX(?ENOTSUP)}
    ],

    add_bad_values_to_data_spec(BadFileIdValues, DataSpec).


-spec replace_enoent_with_error_not_found_in_error_expectations(onenv_api_test_runner:data_spec()) ->
    onenv_api_test_runner:data_spec().
replace_enoent_with_error_not_found_in_error_expectations(DataSpec = #data_spec{bad_values = BadValues}) ->
    DataSpec#data_spec{bad_values = lists:map(fun
        ({Key, Value, ?ERR_POSIX(?ENOENT)}) -> {Key, Value, ?ERROR_NOT_FOUND};
        ({Key, Value, {Interface, ?ERR_POSIX(?ENOENT)}}) -> {Key, Value, {Interface, ?ERROR_NOT_FOUND}};
        (Spec) -> Spec
    end, BadValues)}.


maybe_substitute_bad_id(ValidId, undefined) ->
    {ValidId, undefined};
maybe_substitute_bad_id(ValidId, Data) ->
    case maps:take(bad_id, Data) of
        {BadId, LeftoverData} -> {BadId, LeftoverData};
        error -> {ValidId, Data}
    end.



%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
add_bad_values_to_data_spec(BadValuesToAdd, undefined) ->
    #data_spec{bad_values = BadValuesToAdd};
add_bad_values_to_data_spec(BadValuesToAdd, #data_spec{bad_values = BadValues} = DataSpec) ->
    DataSpec#data_spec{bad_values = BadValuesToAdd ++ BadValues}.


%% @private
get_invalid_file_id_errors(IdKey) ->
    InvalidGuid = <<"InvalidGuid">>,
    {ok, InvalidObjectId} = file_id:guid_to_objectid(InvalidGuid),

    [
        % Errors thrown by rest_handler, which failed to convert file path/cdmi_id to guid
        {bad_id, <<"/NonExistentPath">>, {rest_with_file_path, ?ERR_POSIX(?ENOENT)}},
        {bad_id, <<"InvalidObjectId">>, {rest, ?ERR_SPACE_NOT_SUPPORTED_BY(<<"InvalidObjectId">>, provider_id_placeholder)}},

        % Errors thrown by middleware and internal logic
        {bad_id, InvalidObjectId, {rest, ?ERR_SPACE_NOT_SUPPORTED_BY(InvalidObjectId, provider_id_placeholder)}},
        {bad_id, InvalidGuid, {gs, ?ERR_BAD_VALUE_IDENTIFIER(IdKey)}}
    ].


%% @private
add_share_file_id_errors_for_operations_not_available_in_share_mode(_FileGuid, undefined, Errors) ->
    Errors;
add_share_file_id_errors_for_operations_not_available_in_share_mode(FileGuid, ShareId, Errors) ->
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, ShareFileObjectId} = file_id:guid_to_objectid(ShareFileGuid),

    [
        % Errors in share mode:
        % - rest: thrown by middleware operation_supported check (rest_handler
        %   changes scope to public when using share object id)
        % - gs: scope is left intact (in contrast to rest) but client is changed
        %   to ?GUEST. Then it fails middleware auth checks (whether user belongs
        %   to space or has some space privileges)
        {bad_id, ShareFileObjectId, {rest, ?ERROR_NOT_SUPPORTED}},
        {bad_id, ShareFileGuid, {gs, ?ERR_UNAUTHORIZED(undefined)}}

        | Errors
    ].
