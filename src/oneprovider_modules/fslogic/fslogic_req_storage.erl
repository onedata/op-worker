%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: FSLogic request handlers for storage operations.
%% @end
%% ===================================================================
-module(fslogic_req_storage).
-author("Rafal Slota").

-include("oneprovider_modules/dao/dao.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create_storage_test_file/1, storage_test_file_modified/3, client_storage_info/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% create_storage_test_file/1
%% ====================================================================
%% @doc Creates test file on given storage for the user, so that
%%      it would be possible to verify storage file access on client side.
%% @end
-spec create_storage_test_file(StorageId :: non_neg_integer()) ->
    #createstoragetestfileresponse{} | no_return().
%% ====================================================================
create_storage_test_file(StorageId) ->
    ?debug("create_storage_test_file(StorageId: ~p)", [StorageId]),
    Length = 20,
    {A, B, C} = now(),
    random:seed(A, B, C),
    Text = list_to_binary(fslogic_utils:random_ascii_lowercase_sequence(Length)),
    {ok, DeleteStorageTestFileTime} = application:get_env(?APP_Name, delete_storage_test_file_time),
    {ok, #db_document{} = UserDoc} = fslogic_objects:get_user(),
    Login = user_logic:get_login(UserDoc),
    {ok, #db_document{record = StorageInfo}} = fslogic_objects:get_storage({id, StorageId}),
    StorageHelperInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, StorageInfo),
    {ok, Path} = do_create_storage_test_file(StorageHelperInfo, Login),
    % Delete storage test file after 'delete_storage_test_file_time' seconds
    spawn(fun() ->
        timer:sleep(DeleteStorageTestFileTime * 1000),
        storage_files_manager:delete(StorageHelperInfo, Path)
    end),
    Length = storage_files_manager:write(StorageHelperInfo, Path, 0, Text),
    #createstoragetestfileresponse{answer = true, relative_path = Path, text = Text}.


%% storage_test_file_modified/3
%% ====================================================================
%% @doc Handles test file modification and validates contents of the file.
%% @end
-spec storage_test_file_modified(StorageId :: non_neg_integer(), RelPath :: string(), Text :: string()) ->
    #storagetestfilemodifiedresponse{} | no_return().
%% ====================================================================
storage_test_file_modified(StorageId, RelPath, Text) ->
    ?debug("storage_test_file_modified(StorageId: ~p, RelPath: ~p, Text: ~p)", [StorageId, RelPath, Text]),
    {ok, #db_document{record = StorageInfo}} = fslogic_objects:get_storage({id, StorageId}),
    StorageHelperInfo = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, StorageInfo),
    {ok, Bytes} = storage_files_manager:read(StorageHelperInfo, RelPath, 0, length(Text)),
    Text = binary_to_list(Bytes),
    storage_files_manager:delete(StorageHelperInfo, RelPath),
    #storagetestfilemodifiedresponse{answer = true}.


%% client_storage_info/1
%% ====================================================================
%% @doc Handles client's storage availability request.
%%      Client's claims are saved to its session (#fuse_session{}).
%% @end
-spec client_storage_info(SInfo :: list()) ->
    #atom{} | no_return().
%% ====================================================================
client_storage_info(SInfo) ->
    ?debug("client_storage_info(SInfo: ~p)", [SInfo]),
    {ok, #db_document{record = FuseSession} = FuseSessionDoc} = dao_lib:apply(dao_cluster, get_fuse_session, [fslogic_context:get_fuse_id()], fslogic_context:get_protocol_version()),
    ClientStorageInfo = lists:map(fun({_, StorageId, Root}) ->
        {StorageId, #storage_helper_info{name = "DirectIO", init_args = [Root]}} end, SInfo),
    NewFuseSessionDoc = FuseSessionDoc#db_document{record = FuseSession#fuse_session{client_storage_info = ClientStorageInfo}},
    {ok, _} = dao_lib:apply(dao_cluster, save_fuse_session, [NewFuseSessionDoc], fslogic_context:get_protocol_version()),
    ?info("Client storage info saved in session."),
    #atom{value = ?VOK}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% do_create_storage_test_file/2
%% ====================================================================
%% @doc Creates storage test file with random filename in user home directory. If file already exists new name is generated.
-spec do_create_storage_test_file(StorageHelperInfo :: #storage_helper_info{}, Login :: string()) -> Result when
    Result :: {ok, Path :: string()} | {error, attempts_limit_excceded}.
%% ====================================================================
do_create_storage_test_file(StorageHelperInfo, Login) ->
    do_create_storage_test_file(StorageHelperInfo, Login, 20).

%% do_create_storage_test_file/3
%% ====================================================================
%% @doc Creates storage test file with random filename in user home directory. If file already exists new name is generated.
-spec do_create_storage_test_file(StorageHelperInfo :: #storage_helper_info{}, Login :: string(), Attempts :: integer()) -> Result when
    Result :: {ok, Path :: string()} | {error, attempts_limit_excceded}.
%% ====================================================================
do_create_storage_test_file(_, _, 0) ->
    {error, attempts_limit_exceeded};
do_create_storage_test_file(StorageHelperInfo, Login, Attempts) ->
    {A, B, C} = now(),
    random:seed(A, B, C),
    Filename = fslogic_utils:random_ascii_lowercase_sequence(8),
    Path = "users/" ++ Login ++ "/" ++ ?STORAGE_TEST_FILE_PREFIX ++ Filename,
    case storage_files_manager:create(StorageHelperInfo, Path) of
        ok -> {ok, Path};
        _ -> do_create_storage_test_file(StorageHelperInfo, Login, Attempts - 1)
    end.