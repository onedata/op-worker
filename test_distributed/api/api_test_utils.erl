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

    add_bad_file_id_and_path_error_values/3
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
    InvalidGuid = <<"InvalidGuid">>,
    {ok, InvalidObjectId} = file_id:guid_to_objectid(InvalidGuid),
    InvalidIdExpError = ?ERROR_BAD_VALUE_IDENTIFIER(<<"id">>),

    % Request with guid properly formed but from invalid/nonexistent parts
    % should pass sanitization step and fail only on later steps
    NonExistentFileAndSpaceGuid = file_id:pack_share_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID, ShareId),
    {ok, NonExistentFileAndSpaceObjectId} = file_id:guid_to_objectid(NonExistentFileAndSpaceGuid),
    NonExistentFileAndSpaceExpError = case ShareId of
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

    % Request with properly formed guid with valid and supported space
    % but invalid file_uuid should pass all checks and be forwarded to
    % internal logic (ids are not checks for validity like e.g. length).
    % Then due to failed file doc fetch (nonexistent file_uuid) ?ENOENT
    % should be returned.
    NonExistentFileGuid = file_id:pack_share_guid(<<"InvalidUuid">>, SpaceId, ShareId),
    {ok, NonExistentFileObjectId} = file_id:guid_to_objectid(NonExistentFileGuid),
    NonExistentFileExpError = ?ERROR_POSIX(?ENOENT),

    BadFileIdAndPathValues = [
        % Errors thrown by rest_handler, which failed to convert file path/cdmi_id to guid
        {bad_id, <<"/NonExistentPath">>, {rest_with_file_path, ?ERROR_BAD_VALUE_IDENTIFIER(<<"urlFilePath">>)}},
        {bad_id, <<"InvalidObjectId">>, {rest, ?ERROR_BAD_VALUE_IDENTIFIER(<<"id">>)}},

        % Errors thrown by middleware and internal logic
        {bad_id, InvalidObjectId, {rest, InvalidIdExpError}},
        {bad_id, NonExistentFileAndSpaceObjectId, {rest, NonExistentFileAndSpaceExpError}},
        {bad_id, NonExistentFileObjectId, {rest, NonExistentFileExpError}},

        {bad_id, InvalidGuid, {gs, InvalidIdExpError}},
        {bad_id, NonExistentFileAndSpaceGuid, {gs, NonExistentFileAndSpaceExpError}},
        {bad_id, NonExistentFileGuid, {gs, NonExistentFileExpError}}
    ],
    case DataSpec of
        undefined ->
            #data_spec{bad_values = BadFileIdAndPathValues};
        #data_spec{bad_values = BadValues} ->
            DataSpec#data_spec{bad_values = BadFileIdAndPathValues ++ BadValues}
    end.
