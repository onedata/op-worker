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

-export([init_env/0, set_env_var/3, get_env_var/2]).

-export([load_module_from_test_distributed_dir/2]).

-export([
    randomly_choose_file_type_for_test/0,
    randomly_choose_file_type_for_test/1,
    create_file/4, create_file/5,
    wait_for_file_sync/3,

    fill_file_with_dummy_data/4,
    fill_file_with_dummy_data/5,
    read_file/4,

    guids_to_object_ids/1,
    ensure_defined/2
]).
-export([
    add_file_id_errors_for_operations_available_in_share_mode/3,
    add_file_id_errors_for_operations_not_available_in_share_mode/3,
    add_cdmi_id_errors_for_operations_not_available_in_share_mode/4,
    maybe_substitute_id/2
]).

-opaque env_ref() :: integer().
-type file_type() :: binary(). % <<"file">> | <<"dir">>

-export_type([env_ref/0, file_type/0]).


-define(ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


-spec init_env() -> env_ref().
init_env() ->
    erlang:unique_integer([positive]).


-spec set_env_var(env_ref(), Key :: term(), Value :: term()) -> ok.
set_env_var(EnvRef, Key, Value) ->
    simple_cache:put({EnvRef, Key}, Value).


-spec get_env_var(env_ref(), Key :: term()) ->
    {ok, Value :: term()} | {error, not_found}.
get_env_var(EnvRef, Key) ->
    simple_cache:get({EnvRef, Key}).


%%% TODO VFS-6385 Reorganize and fix includes and loading modules from other dirs in tests
-spec load_module_from_test_distributed_dir(proplists:proplist(), module()) ->
    ok.
load_module_from_test_distributed_dir(Config, ModuleName) ->
    DataDir = ?config(data_dir, Config),
    ProjectRoot = filename:join(lists:takewhile(fun(Token) ->
        Token /= "test_distributed"
    end, filename:split(DataDir))),
    TestsRootDir = filename:join([ProjectRoot, "test_distributed"]),

    code:add_pathz(TestsRootDir),

    CompileOpts = [
        verbose,report_errors,report_warnings,
        {i, TestsRootDir},
        {i, filename:join([TestsRootDir, "..", "include"])},
        {i, filename:join([TestsRootDir, "..", "_build", "default", "lib"])}
    ],
    case compile:file(filename:join(TestsRootDir, ModuleName), CompileOpts) of
        {ok, ModuleName} ->
            code:purge(ModuleName),
            code:load_file(ModuleName),
            ok;
        _ ->
            ct:fail("Couldn't load module: ~p", [ModuleName])
    end.


-spec randomly_choose_file_type_for_test() -> file_type().
randomly_choose_file_type_for_test() ->
    randomly_choose_file_type_for_test(true).


-spec randomly_choose_file_type_for_test(boolean()) -> file_type().
randomly_choose_file_type_for_test(LogSelectedFileType) ->
    FileType = ?RANDOM_FILE_TYPE(),
    LogSelectedFileType andalso ct:pal("Chosen file type for test: ~s", [FileType]),
    FileType.


-spec create_file(file_type(), node(), session:id(), file_meta:path()) ->
    {ok, file_id:file_guid()} | {error, term()}.
create_file(FileType, Node, SessId, Path) ->
    create_file(FileType, Node, SessId, Path, 8#777).


-spec create_file(file_type(), node(), session:id(), file_meta:path(), file_meta:mode()) ->
    {ok, file_id:file_guid()} | {error, term()}.
create_file(<<"file">>, Node, SessId, Path, Mode) ->
    lfm_proxy:create(Node, SessId, Path, Mode);
create_file(<<"dir">>, Node, SessId, Path, Mode) ->
    lfm_proxy:mkdir(Node, SessId, Path, Mode).


-spec wait_for_file_sync(node(), session:id(), file_id:file_guid()) -> ok.
wait_for_file_sync(Node, SessId, FileGuid) ->
    ?assertMatch({ok, _}, lfm_proxy:stat(Node, SessId, {guid, FileGuid}), ?ATTEMPTS),
    ok.


-spec fill_file_with_dummy_data(node(), session:id(), file_id:file_guid(), Size :: non_neg_integer()) ->
    WrittenContent :: binary().
fill_file_with_dummy_data(Node, SessId, FileGuid, Size) ->
    fill_file_with_dummy_data(Node, SessId, FileGuid, 0, Size).


-spec fill_file_with_dummy_data(node(), session:id(), file_id:file_guid(),
    Offset :: non_neg_integer(), Size :: non_neg_integer()) -> WrittenContent :: binary().
fill_file_with_dummy_data(Node, SessId, FileGuid, Offset, Size) ->
    Content = crypto:strong_rand_bytes(Size),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {guid, FileGuid}, write)),
    ?assertMatch({ok, _}, lfm_proxy:write(Node, Handle, Offset, Content)),
    ?assertMatch(ok, lfm_proxy:fsync(Node, Handle)),
    ?assertMatch(ok, lfm_proxy:close(Node, Handle)),
    Content.


-spec read_file(node(), session:id(), file_id:file_guid(), Size :: non_neg_integer()) ->
    Content :: binary().
read_file(Node, SessId, FileGuid, Size) ->
    {ok, ReadHandle} = lfm_proxy:open(Node, SessId, {guid, FileGuid}, read),
    {ok, Content} = lfm_proxy:read(Node, ReadHandle, 0, Size),
    ok = lfm_proxy:close(Node, ReadHandle),
    Content.


-spec guids_to_object_ids([file_id:file_guid()]) -> [file_id:objectid()].
guids_to_object_ids(Guids) ->
    lists:map(fun(Guid) ->
        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        ObjectId
    end, Guids).


-spec ensure_defined
    (undefined, DefaultValue) -> DefaultValue when DefaultValue :: term();
    (Value, DefaultValue :: term()) -> Value when Value :: term().
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
%% Bad ids are available under 'bad_id' atom key - test implementation should
%% make sure to substitute them for fileId component in rest path or #gri.id
%% before making test call.
%% @end
%%--------------------------------------------------------------------
-spec add_file_id_errors_for_operations_available_in_share_mode(file_id:file_guid(),
    undefined | od_share:id(), undefined | data_spec()) -> data_spec().
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
-spec add_file_id_errors_for_operations_not_available_in_share_mode(file_id:file_guid(),
    od_share:id(), undefined | data_spec()) -> data_spec().
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
    
    add_bad_values_to_data_spec(BadFileIdErrors, DataSpec).


%%--------------------------------------------------------------------
%% @doc
%% Extends data_spec() with file id bad values and errors for operations 
%% not available in share mode that provide file id as parameter in data spec map.
%% All added bad values are in cdmi form and are stored under <<"fileId">> key.
%% @end
%%--------------------------------------------------------------------
-spec add_cdmi_id_errors_for_operations_not_available_in_share_mode(file_id:file_guid(), od_space:id(),
    od_share:id(), data_spec()) -> data_spec().
add_cdmi_id_errors_for_operations_not_available_in_share_mode(FileGuid, SpaceId, ShareId, DataSpec) ->
    {ok, DummyObjectId} = file_id:guid_to_objectid(<<"DummyGuid">>),

    NonExistentSpaceGuid = file_id:pack_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID),
    {ok, NonExistentSpaceObjectId} = file_id:guid_to_objectid(NonExistentSpaceGuid),
    
    NonExistentSpaceShareGuid = file_id:guid_to_share_guid(NonExistentSpaceGuid, ShareId),
    {ok, NonExistentSpaceShareObjectId} = file_id:guid_to_objectid(NonExistentSpaceShareGuid),
    
    NonExistentFileGuid = file_id:pack_guid(<<"InvalidUuid">>, SpaceId),
    {ok, NonExistentFileObjectId} = file_id:guid_to_objectid(NonExistentFileGuid),
    
    NonExistentFileShareGuid = file_id:guid_to_share_guid(NonExistentFileGuid, ShareId),
    {ok, NonExistentFileShareObjectId} = file_id:guid_to_objectid(NonExistentFileShareGuid),
    
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, ShareFileObjectId} = file_id:guid_to_objectid(ShareFileGuid),
    
    BadFileIdValues = [
        {<<"fileId">>, <<"InvalidObjectId">>, ?ERROR_BAD_VALUE_IDENTIFIER(<<"fileId">>)},
        {<<"fileId">>, DummyObjectId, ?ERROR_BAD_VALUE_IDENTIFIER(<<"fileId">>)},

        % user has no privileges in non existent space and so he should receive ?ERROR_FORBIDDEN
        {<<"fileId">>, NonExistentSpaceObjectId, ?ERROR_FORBIDDEN},
        {<<"fileId">>, NonExistentSpaceShareObjectId, ?ERROR_FORBIDDEN},
        
        {<<"fileId">>, NonExistentFileObjectId, ?ERROR_POSIX(?ENOENT)},
        
        % operation on shared file is forbidden - it should result in ?EACCES
        {<<"fileId">>, ShareFileObjectId, ?ERROR_POSIX(?EACCES)},
        {<<"fileId">>, NonExistentFileShareObjectId, ?ERROR_POSIX(?EACCES)}
    ],

    add_bad_values_to_data_spec(BadFileIdValues, DataSpec).


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
add_bad_values_to_data_spec(BadValuesToAdd, undefined) ->
    #data_spec{bad_values = BadValuesToAdd};
add_bad_values_to_data_spec(BadValuesToAdd, #data_spec{bad_values = BadValues} = DataSpec) ->
    DataSpec#data_spec{bad_values = BadValuesToAdd ++ BadValues}.


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
