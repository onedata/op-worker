%%%--------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests related to storage.
%%% It is used during remote client initialization.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_req).
-author("Krzysztof Trzepla").
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").

-define(REMOVE_STORAGE_TEST_FILE_DELAY, timer:seconds(application:get_env(?APP_NAME,
    remove_storage_test_file_delay_seconds, 300))).
-define(VERIFY_STORAGE_TEST_FILE_DELAY, timer:seconds(application:get_env(?APP_NAME,
    verify_storage_test_file_delay_seconds, 30))).
-define(VERIFY_STORAGE_TEST_FILE_ATTEMPTS, application:get_env(?APP_NAME,
    remove_storage_test_file_attempts, 10)).

%% API
-export([get_configuration/1, get_helper_params/3, create_storage_test_file/3,
    verify_storage_test_file/5, remove_storage_test_file/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns remote client configuration.
%% @end
%%--------------------------------------------------------------------
-spec get_configuration(session:id()) -> #configuration{}.
get_configuration(SessId) ->
    {ok, UserId} = session:get_user_id(SessId),
    {ok, Docs} = subscription:list(),
    Subs = lists:filtermap(fun(#document{value = #subscription{} = Sub}) ->
        case subscription_type:is_remote(Sub) of
            true -> {true, Sub};
            false -> false
        end
    end, Docs),
    #configuration{
        root_guid = fslogic_uuid:user_root_dir_guid(UserId),
        subscriptions = Subs,
        disabled_spaces = space_quota:get_disabled_spaces()
    }.

%%--------------------------------------------------------------------
%% @doc
%% Gets helper params based on given storage ID.
%% @end
%%--------------------------------------------------------------------
-spec get_helper_params(user_ctx:ctx(), storage:id(),
    ForceCL :: boolean()) -> #fuse_response{}.
get_helper_params(_UserCtx, StorageId, true = _ForceProxy) ->
    {ok, Helper} = case storage:get(StorageId) of
        {ok, StorageDoc} ->
            fslogic_storage:select_helper(StorageDoc);
        {error, not_found} ->
            {ok, undefined}
    end,
    HelperParams = helper:get_proxy_params(Helper, StorageId),
    #fuse_response{status = #status{code = ?OK}, fuse_response = HelperParams};
get_helper_params(_UserCtx, _StorageId, false = _ForceProxy) ->
    #fuse_response{status = #status{code = ?ENOTSUP}}.

%%--------------------------------------------------------------------
%% @doc
%% Creates test file on a given storage in the directory of a file referenced
%% by Guid. File is created on behalf of the user associated with the session.
%% @end
%%--------------------------------------------------------------------
-spec create_storage_test_file(user_ctx:ctx(), fslogic_worker:file_guid(),
    storage:id()) -> #fuse_response{}.
create_storage_test_file(UserCtx, Guid, StorageId) ->
    FileCtx = file_ctx:new_by_guid(Guid),
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = case file_ctx:get_space_id_const(FileCtx) of
        undefined -> throw(?ENOENT);
        <<_/binary>> = Id -> Id
    end,

    {ok, StorageDoc} = storage:get(StorageId),
    {ok, Helper} = fslogic_storage:select_helper(StorageDoc),
    HelperName = helper:get_name(Helper),

    case luma:get_client_user_ctx(UserId, SpaceId, StorageDoc, HelperName) of
        {ok, ClientStorageUserUserCtx} ->
            {ok, ServerStorageUserUserCtx} = luma:get_server_user_ctx(UserId, SpaceId,
                StorageDoc, HelperName),
            HelperParams = helper:get_params(Helper, ClientStorageUserUserCtx),

            {RawStoragePath, FileCtx2} = file_ctx:get_raw_storage_path(FileCtx),
            Dirname = filename:dirname(RawStoragePath),
            _FileCtx3 = sfm_utils:create_parent_dirs(FileCtx2),
            TestFileName = storage_detector:generate_file_id(),
            TestFileId = fslogic_path:join([Dirname, TestFileName]),
            FileContent = storage_detector:create_test_file(Helper, ServerStorageUserUserCtx, TestFileId),

            spawn(storage_req, remove_storage_test_file, [
                Helper, ServerStorageUserUserCtx, TestFileId, ?REMOVE_STORAGE_TEST_FILE_DELAY]),

            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = #storage_test_file{
                    helper_params = HelperParams, space_id = SpaceId,
                    file_id = TestFileId, file_content = FileContent
                }
            };
        {error, _} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates handle to the test file in ROOT context and tries to verify
%% its content VERIFY_STORAGE_TEST_FILE_ATTEMPTS times.
%% @end
%%--------------------------------------------------------------------
-spec verify_storage_test_file(user_ctx:ctx(), od_space:id(),
    storage:id(), helpers:file_id(), FileContent :: binary()) -> #fuse_response{}.
verify_storage_test_file(UserCtx, SpaceId, StorageId, FileId, FileContent) ->
    UserId = user_ctx:get_user_id(UserCtx),
    {ok, StorageDoc} = storage:get(StorageId),
    {ok, Helper} = fslogic_storage:select_helper(StorageDoc),
    HelperName = helper:get_name(Helper),
    {ok, StorageUserCtx} = luma:get_server_user_ctx(UserId, SpaceId, StorageDoc, HelperName),
    verify_storage_test_file_loop(Helper, StorageUserCtx, FileId, FileContent, ?ENOENT,
        ?VERIFY_STORAGE_TEST_FILE_ATTEMPTS).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes test file referenced by handle after specified delay.
%% @end
%%--------------------------------------------------------------------
-spec remove_storage_test_file(helpers:helper(), helper:user_ctx(), helpers:file_id(),
    Delay :: timeout()) -> ok.
remove_storage_test_file(Helper, StorageUserCtx, FileId, Delay) ->
    timer:sleep(Delay),
    storage_detector:remove_test_file(Helper, StorageUserCtx, FileId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Verifies storage test file by reading its content and checking it with the
%% one sent by the client. Retires 'Attempts' times if file is not found or
%% its content doesn't match expected one.
%% @end
%%--------------------------------------------------------------------
-spec verify_storage_test_file_loop(helpers:helper(), helper:user_ctx(),
    helpers:file_id(), FileContent :: binary(), Code :: atom(),
    Attempts :: non_neg_integer()) -> #fuse_response{}.
verify_storage_test_file_loop(_, _, _, _, Code, 0) ->
    #fuse_response{status = #status{code = Code}};
verify_storage_test_file_loop(Helper, StorageUserCtx, FileId, FileContent, _, Attempts) ->
    try storage_detector:read_test_file(Helper, StorageUserCtx, FileId) of
        FileContent ->
            storage_detector:remove_test_file(Helper, StorageUserCtx, FileId),
            #fuse_response{status = #status{code = ?OK}};
        _ ->
            timer:sleep(?VERIFY_STORAGE_TEST_FILE_DELAY),
            verify_storage_test_file_loop(Helper, StorageUserCtx, FileId, FileContent,
                ?EINVAL, Attempts - 1)
    catch
        {_, {error, ?ENOENT}} ->
            timer:sleep(?VERIFY_STORAGE_TEST_FILE_DELAY),
            verify_storage_test_file_loop(Helper, StorageUserCtx, FileId, FileContent,
                ?ENOENT, Attempts - 1)
    end.
