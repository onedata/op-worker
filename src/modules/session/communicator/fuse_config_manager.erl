%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains utility functions used during remote client
%%% initialization.
%%% @end
%%%-------------------------------------------------------------------
-module(fuse_config_manager).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([get_configuration/0]).
-export([create_storage_test_file/3, remove_storage_test_file/2,
    verify_storage_test_file/4]).

-define(TEST_FILE_NAME_SIZE, application:get_env(?APP_NAME,
    storage_test_file_name_size, 32)).
-define(TEST_FILE_CONTENT_SIZE, application:get_env(?APP_NAME,
    storage_test_file_content_size, 100)).
-define(REMOVE_STORAGE_TEST_FILE_DELAY, timer:seconds(application:get_env(?APP_NAME,
    remove_storage_test_file_delay_seconds, 300))).
-define(VERIFY_STORAGE_TEST_FILE_DELAY, timer:seconds(application:get_env(?APP_NAME,
    verify_storage_test_file_delay_seconds, 30))).
-define(VERIFY_STORAGE_TEST_FILE_ATTEMPTS, application:get_env(?APP_NAME,
    remove_storage_test_file_attempts, 10)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns remote client configuration.
%% @end
%%--------------------------------------------------------------------
-spec get_configuration() -> Configuration :: #configuration{}.
get_configuration() ->
    {ok, Docs} = subscription:list(),
    Subs = lists:filtermap(fun
        (#document{value = #subscription{object = undefined}}) -> false;
        (#document{value = #subscription{} = Sub}) -> {true, Sub}
    end, Docs),
    #configuration{subscriptions = Subs}.

%%--------------------------------------------------------------------
%% @doc
%% Creates test file on a given storage in the directory of a file referenced by UUID.
%% File is created on behalf of the user associated with the session. 
%% @end
%%--------------------------------------------------------------------
-spec create_storage_test_file(Ctx :: fslogic_worker:ctx(), StorageId :: storage:id(),
    FileUuid :: file_meta:uuid()) -> #fuse_response{}.
create_storage_test_file(#fslogic_ctx{session_id = SessId} = Ctx, StorageId, FileUuid) ->
    UserId = fslogic_context:get_user_id(Ctx),
    {ok, #document{key = SpaceUuid}} = fslogic_spaces:get_space({uuid, FileUuid}, UserId),
    {ok, Storage} = storage:get(StorageId),
    #fuse_response{fuse_response = HelperParams} =
        fslogic_req_regular:get_helper_params(Ctx, StorageId, false),

    FileId = fslogic_utils:gen_storage_file_id({uuid, FileUuid}),
    Dirname = fslogic_path:dirname(FileId),
    TestFileName = random_alphanumeric_sequence(?TEST_FILE_NAME_SIZE),
    TestFileId = fslogic_path:join([Dirname, TestFileName]),
    FileContent = random_alphanumeric_sequence(?TEST_FILE_CONTENT_SIZE),

    Handle = storage_file_manager:new_handle(SessId, SpaceUuid, undefined, Storage, TestFileId),
    ok = storage_file_manager:create(Handle, 8#600),
    RootHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceUuid, undefined, Storage, TestFileId),
    {ok, RootWriteHandle} = storage_file_manager:open(RootHandle, write),
    {ok, _} = storage_file_manager:write(RootWriteHandle, 0, FileContent),

    spawn(?MODULE, remove_storage_test_file, [RootHandle, ?REMOVE_STORAGE_TEST_FILE_DELAY]),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #storage_test_file{
            helper_params = HelperParams, space_uuid = SpaceUuid,
            file_id = TestFileId, file_content = FileContent
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Creates handle to the test file in ROOT context and tries to verify
%% its content ?VERIFY_STORAGE_TEST_FILE_ATTEMPTS times.
%% @end
%%--------------------------------------------------------------------
-spec verify_storage_test_file(StorageId :: storage:id(), SpaceUuid :: file_meta:uuid(),
    FileId :: helpers:file(), FileContent :: binary()) -> #fuse_response{}.
verify_storage_test_file(StorageId, SpaceUuid, FileId, FileContent) ->
    {ok, Storage} = storage:get(StorageId),
    Handle = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceUuid, undefined, Storage, FileId),
    verify_storage_test_file_loop(Handle, FileContent, ?ENOENT, ?VERIFY_STORAGE_TEST_FILE_ATTEMPTS).

%%--------------------------------------------------------------------
%% @doc
%% Removes test file referenced by handle after specified delay.
%% @end
%%--------------------------------------------------------------------
-spec remove_storage_test_file(Handle :: storage_file_manager:handle(), Delay :: timeout()) ->
    ok | {error, Reason :: term()}.
remove_storage_test_file(Handle, Delay) ->
    timer:sleep(Delay),
    storage_file_manager:unlink(Handle).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Verifies storage test file by reading its content and checking it with the
%% one sent by the client. Retires 'Attempts' times if file is not found or
%% its content doesn't match expected one.
%% @end
%%--------------------------------------------------------------------
-spec verify_storage_test_file_loop(Handle :: storage_file_manager:handle(),
    FileContent :: binary(), Code :: atom(), Attempts :: non_neg_integer()) -> #fuse_response{}.
verify_storage_test_file_loop(_, _, Code, 0) ->
    #fuse_response{status = #status{code = Code}};

verify_storage_test_file_loop(Handle, FileContent, _, Attempts) ->
    case storage_file_manager:open(Handle, read) of
        {ok, ReadHandle} ->
            {ok, ActualFileContent} = storage_file_manager:read(ReadHandle, 0, size(FileContent)),
            case FileContent of
                ActualFileContent ->
                    remove_storage_test_file(Handle, 0),
                    #fuse_response{status = #status{code = ?OK}};
                _ ->
                    timer:sleep(?VERIFY_STORAGE_TEST_FILE_DELAY),
                    verify_storage_test_file_loop(Handle, FileContent, ?EINVAL, Attempts - 1)
            end;
        {error, ?ENOENT} ->
            timer:sleep(?VERIFY_STORAGE_TEST_FILE_DELAY),
            verify_storage_test_file_loop(Handle, FileContent, ?ENOENT, Attempts - 1)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns random alphanumeric sequence of given length.
%% @end
%%--------------------------------------------------------------------
-spec random_alphanumeric_sequence(Size :: non_neg_integer()) -> Seq :: binary().
random_alphanumeric_sequence(Size) ->
    re:replace(http_utils:base64url_encode(crypto:rand_bytes(Size)), "\\W", "", [global, {return, binary}]).