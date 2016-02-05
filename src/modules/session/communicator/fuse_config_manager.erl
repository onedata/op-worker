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

-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([get_configuration/0]).
-export([create_storage_test_file/3, verify_storage_test_file/4]).

-define(TEST_FILE_NAME_SIZE, 32).
-define(TEST_FILE_CONTENT_SIZE, 100).

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
    TestFileName = re:replace(base64:encode(
        crypto:rand_bytes(?TEST_FILE_NAME_SIZE)), "\\W", "", [global, {return, binary}]),
    TestFileId = fslogic_path:join([Dirname, TestFileName]),
    FileContent = base64:encode(crypto:rand_bytes(?TEST_FILE_CONTENT_SIZE)),

    Handle = storage_file_manager:new_handle(SessId, SpaceUuid, undefined, Storage, TestFileId),
    ok = storage_file_manager:create(Handle, 8#600),
    RootHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceUuid, undefined, Storage, TestFileId),
    {ok, RootWriteHandle} = storage_file_manager:open(RootHandle, write),
    {ok, _} = storage_file_manager:write(RootWriteHandle, 0, FileContent),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #storage_test_file{
            helper_params = HelperParams, space_uuid = SpaceUuid,
            file_id = TestFileId, file_content = FileContent
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Verifies storage test file by reading its content and checking it with the one sent by the client.
%% @end
%%--------------------------------------------------------------------
-spec verify_storage_test_file(StorageId :: storage:id(), SpaceUuid :: file_meta:uuid(),
    FileId :: helpers:file(), FileContent :: binary()) -> #fuse_response{}.
verify_storage_test_file(StorageId, SpaceUuid, FileId, FileContent) ->
    {ok, Storage} = storage:get(StorageId),

    RootHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceUuid, undefined, Storage, FileId),
    {ok, RootReadHandle} = storage_file_manager:open(RootHandle, read),
    {ok, ActualFileContent} = storage_file_manager:read(RootReadHandle, 0, size(FileContent)),

    Code = case FileContent of
        ActualFileContent -> ?OK;
        _ -> ?EINVAL
    end,

    #fuse_response{status = #status{code = Code}}.
