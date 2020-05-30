%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions used in API (REST + gs) tests.
%%% @end
%%%-------------------------------------------------------------------
-module(api_test_utils).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("test_utils/initializer.hrl").

-export([
    randomly_choose_file_type_for_test/0,
    randomly_choose_file_type_for_test/1,
    create_file/4, create_file/5,
    wait_for_file_sync/3,
    guids_to_object_ids/1,
    ensure_defined/2,

    add_bad_file_id_and_path_error_values/3
]).
-export([
    add_file_id_errors_for_operations_available_in_share_mode/3,
    add_file_id_errors_for_operations_not_available_in_share_mode/3,
    maybe_substitute_id/2
]).


-define(ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


randomly_choose_file_type_for_test() ->
    randomly_choose_file_type_for_test(true).


randomly_choose_file_type_for_test(LogSelectedFileType) ->
    FileType = ?RANDOM_FILE_TYPE(),
    LogSelectedFileType andalso ct:pal("Choosen file type for test: ~s", [FileType]),
    FileType.


create_file(FileType, Node, SessId, Path) ->
    create_file(FileType, Node, SessId, Path, 8#777).


create_file(<<"file">>, Node, SessId, Path, Mode) ->
    lfm_proxy:create(Node, SessId, Path, Mode);
create_file(<<"dir">>, Node, SessId, Path, Mode) ->
    lfm_proxy:mkdir(Node, SessId, Path, Mode).


-spec wait_for_file_sync(node(), session:id(), file_id:file_guid()) -> ok.
wait_for_file_sync(Node, SessId, FileGuid) ->
    ?assertMatch({ok, _}, lfm_proxy:stat(Node, SessId, {guid, FileGuid}), ?ATTEMPTS),
    ok.


guids_to_object_ids(Guids) ->
    lists:map(fun(Guid) ->
        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        ObjectId
    end, Guids).


-spec add_bad_file_id_and_path_error_values(undefined | data_spec(), od_space:id(),
    undefined | od_share:id()) -> data_spec().
add_bad_file_id_and_path_error_values(DataSpec, SpaceId, ShareId) ->
    InvalidFileIdErrors = get_invalid_file_id_errors(),

    NonExistentSpaceGuid = file_id:pack_share_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID, ShareId),
    {ok, NonExistentSpaceObjectId} = file_id:guid_to_objectid(NonExistentSpaceGuid),
    NonExistentSpaceExpError = case ShareId of
        undefined ->
            % For authenticated users it should fail on authorization step
            % (checks if user belongs to space)
            ?ERROR_FORBIDDEN;
        _ ->
            % For share request it should fail on validation step
            % (checks if space is supported by provider)
            {error_fun, fun(#api_test_ctx{node = Node}) ->
                ?ERROR_SPACE_NOT_SUPPORTED_BY(?GET_DOMAIN_BIN(Node))
            end}
    end,

    NonExistentFileGuid = file_id:pack_share_guid(<<"InvalidUuid">>, SpaceId, ShareId),
    {ok, NonExistentFileObjectId} = file_id:guid_to_objectid(NonExistentFileGuid),

    BadFileIdErrors = InvalidFileIdErrors ++ [
        {bad_id, NonExistentSpaceObjectId, {rest, NonExistentSpaceExpError}},
        {bad_id, NonExistentSpaceGuid, {gs, NonExistentSpaceExpError}},

        % Errors thrown by internal logic (all middleware checks were passed)
        {bad_id, NonExistentFileObjectId, {rest, ?ERROR_POSIX(?ENOENT)}},
        {bad_id, NonExistentFileGuid, {gs, ?ERROR_POSIX(?ENOENT)}}
    ],

    case DataSpec of
        undefined ->
            #data_spec{bad_values = BadFileIdErrors};
        #data_spec{bad_values = BadValues} ->
            DataSpec#data_spec{bad_values = BadFileIdErrors ++ BadValues}
    end.


ensure_defined(undefined, DefaultValue) -> DefaultValue;
ensure_defined(Value, _DefaultValue) -> Value.


%%--------------------------------------------------------------------
%% @doc
%% Adds to data_spec() errors for invalid file id's (guid, path, cdmi_id) for
%% either normal and share mode (since operation is available in both modes
%% it is expected that it will have distinct tests for each mode).
%%
%% ATTENTION !!!
%%
%% Bad ids are available undef 'bad_id' atom key - test implementation should
%% make sure to substitute them for fileId component in rest path or #gri.id
%% before making test call.
%% @end
%%--------------------------------------------------------------------
add_file_id_errors_for_operations_available_in_share_mode(FileGuid, ShareId, DataSpec) ->
    InvalidFileIdErrors = get_invalid_file_id_errors(),

    NonExistentSpaceGuid = file_id:pack_share_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID, ShareId),
    {ok, NonExistentSpaceObjectId} = file_id:guid_to_objectid(NonExistentSpaceGuid),
    NonExistentSpaceExpError = case ShareId of
        undefined ->
            % For authenticated users it should fail on authorization step
            % (checks if user belongs to space)
            ?ERROR_FORBIDDEN;
        _ ->
            % For share request it should fail on validation step
            % (checks if space is supported by provider)
            {error_fun, fun(#api_test_ctx{node = Node}) ->
                ?ERROR_SPACE_NOT_SUPPORTED_BY(?GET_DOMAIN_BIN(Node))
            end}
    end,

    SpaceId = file_id:guid_to_space_id(FileGuid),
    NonExistentFileGuid = file_id:pack_share_guid(<<"InvalidUuid">>, SpaceId, ShareId),
    {ok, NonExistentFileObjectId} = file_id:guid_to_objectid(NonExistentFileGuid),

    BadFileIdErrors = InvalidFileIdErrors ++ [
        {bad_id, NonExistentSpaceObjectId, {rest, NonExistentSpaceExpError}},
        {bad_id, NonExistentSpaceGuid, {gs, NonExistentSpaceExpError}},

        % Errors thrown by internal logic (all middleware checks were passed)
        {bad_id, NonExistentFileObjectId, {rest, ?ERROR_POSIX(?ENOENT)}},
        {bad_id, NonExistentFileGuid, {gs, ?ERROR_POSIX(?ENOENT)}}
    ],

    case DataSpec of
        undefined ->
            #data_spec{bad_values = BadFileIdErrors};
        #data_spec{bad_values = BadValues} ->
            DataSpec#data_spec{bad_values = BadFileIdErrors ++ BadValues}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Adds to data_spec() errors for invalid file id's (guid, path, cdmi_id)
%% for both normal and share mode (since operation is not available in share
%% mode there is no need to write distinct test for share mode - access errors
%% when using share file id can be checked along with other bad_values errors).
%%
%% ATTENTION !!!
%%
%% Bad ids are available undef 'bad_id' atom key - test implementation should
%% make sure to substitute them for fileId component in rest path or #gri.id
%% before making test call.
%% @end
%%--------------------------------------------------------------------
add_file_id_errors_for_operations_not_available_in_share_mode(FileGuid, ShareId, DataSpec) ->
    InvalidFileIdErrors = get_invalid_file_id_errors(),

    NonExistentSpaceGuid = file_id:pack_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID),
    {ok, NonExistentSpaceObjectId} = file_id:guid_to_objectid(NonExistentSpaceGuid),

    NonExistentSpaceErrors = add_share_file_id_errors_for_operations_not_available_in_share_mode(
        NonExistentSpaceGuid, ShareId, [
            % Errors in normal mode - thrown by middleware auth checks
            % (checks whether authenticated user belongs to space)
            {bad_id, NonExistentSpaceObjectId, {rest, ?ERROR_FORBIDDEN}},
            {bad_id, NonExistentSpaceGuid, {gs, ?ERROR_FORBIDDEN}}
        ]
    ),

    SpaceId = file_id:guid_to_space_id(FileGuid),
    NonExistentFileGuid = file_id:pack_guid(<<"InvalidUuid">>, SpaceId),
    {ok, NonExistentFileObjectId} = file_id:guid_to_objectid(NonExistentFileGuid),

    NonExistentFileErrors = add_share_file_id_errors_for_operations_not_available_in_share_mode(
        NonExistentFileGuid, ShareId, [
            % Errors in normal mode - thrown by internal logic
            % (all middleware checks were passed)
            {bad_id, NonExistentFileObjectId, {rest, ?ERROR_POSIX(?ENOENT)}},
            {bad_id, NonExistentFileGuid, {gs, ?ERROR_POSIX(?ENOENT)}}
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

    case DataSpec of
        undefined ->
            #data_spec{bad_values = BadFileIdErrors};
        #data_spec{bad_values = BadValues} ->
            DataSpec#data_spec{bad_values = BadFileIdErrors ++ BadValues}
    end.


maybe_substitute_id(ValidId, undefined) ->
    {ValidId, undefined};
maybe_substitute_id(ValidId, Data) ->
    case maps:take(bad_id, Data) of
        {BadId, LeftoverData} -> {BadId, LeftoverData};
        error -> {ValidId, Data}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
get_invalid_file_id_errors() ->
    InvalidGuid = <<"InvalidGuid">>,
    {ok, InvalidObjectId} = file_id:guid_to_objectid(InvalidGuid),
    InvalidIdExpError = ?ERROR_BAD_VALUE_IDENTIFIER(<<"id">>),

    [
        % Errors thrown by rest_handler, which failed to convert file path/cdmi_id to guid
        {bad_id, <<"/NonExistentPath">>, {rest_with_file_path, ?ERROR_BAD_VALUE_IDENTIFIER(<<"urlFilePath">>)}},
        {bad_id, <<"InvalidObjectId">>, {rest, ?ERROR_BAD_VALUE_IDENTIFIER(<<"id">>)}},

        % Errors thrown by middleware and internal logic
        {bad_id, InvalidObjectId, {rest, InvalidIdExpError}},
        {bad_id, InvalidGuid, {gs, InvalidIdExpError}}
    ].


%% @private
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
        {bad_id, ShareFileGuid, {gs, ?ERROR_UNAUTHORIZED}}

        | Errors
    ].
