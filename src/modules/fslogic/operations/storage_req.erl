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
-include("modules/storage/helpers/helpers.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include_lib("ctool/include/logging.hrl").

-define(REMOVE_STORAGE_TEST_FILE_DELAY, timer:seconds(application:get_env(?APP_NAME,
    remove_storage_test_file_delay_seconds, 300))).
-define(VERIFY_STORAGE_TEST_FILE_DELAY, timer:seconds(application:get_env(?APP_NAME,
    verify_storage_test_file_delay_seconds, 30))).
-define(VERIFY_STORAGE_TEST_FILE_ATTEMPTS, application:get_env(?APP_NAME,
    remove_storage_test_file_attempts, 10)).

%% API
-export([get_configuration/1, get_helper_params/4, create_storage_test_file/3,
    verify_storage_test_file/5, remove_storage_test_file/5]).

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
    {ok, Docs} = subscription:list_durable_subscriptions(),
    Subs = lists:filtermap(fun(#document{value = #subscription{} = Sub}) ->
        case subscription_type:is_remote(Sub) of
            true -> {true, Sub};
            false -> false
        end
    end, Docs),
    DisabledSpaces = case space_quota:get_disabled_spaces() of
        {ok, Spaces} -> Spaces;
        {error, _} -> []
    end,
    #configuration{
        root_guid = fslogic_uuid:user_root_dir_guid(UserId),
        subscriptions = Subs,
        disabled_spaces = DisabledSpaces
    }.

%%--------------------------------------------------------------------
%% @doc
%% Gets helper params based on given storage id and space id.
%% @end
%%--------------------------------------------------------------------
-spec get_helper_params(user_ctx:ctx(), storage:id(),
    od_space:id(), atom()) -> #fuse_response{}.
get_helper_params(UserCtx, StorageId, SpaceId, HelperMode) ->
    {ok, Storage} = storage:get(StorageId),
    Helper = storage:get_helper(Storage),
    case HelperMode of
        ?FORCE_DIRECT_HELPER_MODE when Helper =/= undefined ->
            SessionId = user_ctx:get_session_id(UserCtx),
            UserId = user_ctx:get_user_id(UserCtx),
            case luma:get_client_user_ctx(SessionId, UserId, SpaceId, Storage) of
                {ok, ClientStorageUserCtx} ->
                    HelperParams = helper:get_params(Helper, ClientStorageUserCtx),
                    #fuse_response{
                        status = #status{code = ?OK},
                        fuse_response = HelperParams};
                {error, _} ->
                    #fuse_response{status = #status{code = ?ENOENT}}
            end;
        _ProxyOrAutoMode ->
            HelperParams = helper:get_proxy_params(Helper, StorageId),
            #fuse_response{
               status = #status{code = ?OK},
               fuse_response = HelperParams}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates test file on a given storage in the directory of a file referenced
%% by Guid. File is created on behalf of the user associated with the session.
%% @end
%%--------------------------------------------------------------------
-spec create_storage_test_file(user_ctx:ctx(), fslogic_worker:file_guid(),
    storage:id()) -> #fuse_response{}.
create_storage_test_file(UserCtx, Guid, StorageId) ->
    % TODO VFS-6121 pass SpaceId instead of Guid here
    SpaceId = try file_id:guid_to_space_id(Guid) of
        <<_/binary>> = Id -> Id
        catch
            error:{invalid_guid, _Guid} ->
            % TODO VFS-6121 log not existing SpaceId here
            ?error("Detecting storage ~p failed due to not existing space.", [StorageId]),
            throw(?ENOENT)
    end,
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    SpaceCtx = file_ctx:new_by_guid(SpaceGuid),
    UserId = user_ctx:get_user_id(UserCtx),
    SessionId = user_ctx:get_session_id(UserCtx),

    {ok, Storage} = case storage:get(StorageId) of
        {ok, Doc} -> {ok, Doc};
        {error, not_found} ->
            ?error("Detecting storage ~p failed due to not found storage document.", [StorageId]),
            throw(?ENOENT)
    end,

    Helper = storage:get_helper(Storage),

    case luma:get_client_user_ctx(SessionId, UserId, SpaceId, Storage) of
        {ok, ClientStorageUserCtx} ->
            {ok, ServerStorageUserCtx} = luma:get_server_user_ctx(SessionId, UserId, SpaceId, Storage),
            HelperParams = helper:get_params(Helper, ClientStorageUserCtx),
            {SpaceStorageFileId, _SpaceCtx2} = file_ctx:get_storage_file_id(SpaceCtx),
            DirName = filename:dirname(SpaceStorageFileId),
            TestFileName = storage_detector:generate_file_id(),
            TestFileId = fslogic_path:join([DirName, TestFileName]),
            try
                FileContent = storage_detector:create_test_file(Helper, ServerStorageUserCtx, TestFileId),
                spawn(storage_req, remove_storage_test_file, [
                    Helper, ServerStorageUserCtx, TestFileId, byte_size(FileContent), ?REMOVE_STORAGE_TEST_FILE_DELAY]),
                #fuse_response{
                    status = #status{code = ?OK},
                    fuse_response = #storage_test_file{
                        helper_params = HelperParams, space_id = SpaceId,
                        file_id = TestFileId, file_content = FileContent
                    }
                }
            catch
                error:{badmatch, {error, Reason}} ->
                    ?error_stacktrace("Detecting storage ~p failed due to ~p", [StorageId, Reason]),
                    #fuse_response{status = #status{code = Reason}}
            end;
        {error, Reason} ->
            ?error("Detecting storage ~p failed with error ~p when getting client user context.", [StorageId, Reason]),
            #fuse_response{status = #status{code = ?EACCES}}
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
    SessionId = user_ctx:get_session_id(UserCtx),
    {ok, Storage} = storage:get(StorageId),
    Helper = storage:get_helper(Storage),
    {ok, StorageUserCtx} = luma:get_server_user_ctx(SessionId, UserId, SpaceId, Storage),
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
    Size::non_neg_integer(), Delay :: timeout()) -> ok.
remove_storage_test_file(Helper, StorageUserCtx, FileId, Size, Delay) ->
    timer:sleep(Delay),
    storage_detector:remove_test_file(Helper, StorageUserCtx, FileId, Size).

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
            storage_detector:remove_test_file(Helper, StorageUserCtx, FileId, size(FileContent)),
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
