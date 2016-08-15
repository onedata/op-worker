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
-export([create_storage_test_file/2, remove_storage_test_file/3,
    verify_storage_test_file/2]).

-define(TEST_FILE_NAME_LEN, application:get_env(?APP_NAME,
    storage_test_file_name_size, 32)).
-define(TEST_FILE_CONTENT_LEN, application:get_env(?APP_NAME,
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
    #configuration{subscriptions = Subs, disabled_spaces = space_quota:get_disabled_spaces()}.

%%--------------------------------------------------------------------
%% @doc
%% Creates test file on a given storage in the directory of a file referenced
%% by UUID. File is created on behalf of the user associated with the session.
%% @end
%%--------------------------------------------------------------------
-spec create_storage_test_file(SessId :: session:id(), #create_storage_test_file{}) ->
    #fuse_response{}.
create_storage_test_file(SessId, #create_storage_test_file{storage_id = StorageId, file_uuid = FileGUID}) ->
    {ok, UserId} = session:get_user_id(SessId),
    FileUUID = fslogic_uuid:file_guid_to_uuid(FileGUID),
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space({uuid, FileUUID}, UserId),
    {ok, StorageDoc} = storage:get(StorageId),
    {ok, HelperInit} = fslogic_storage:select_helper(StorageDoc),
    HelperName = helpers:name(HelperInit),
    HelperArgs = helpers:args(HelperInit),
    UserCtx = fslogic_storage:new_user_ctx(HelperInit, SessId, SpaceUUID),
    Handle = helpers_utils:create_test_file_handle(HelperName, HelperArgs, UserCtx),
    HelperParams = helpers_utils:get_params(HelperInit, UserCtx),
    FileId = fslogic_utils:gen_storage_file_id({uuid, FileUUID}),
    Dirname = fslogic_path:dirname(FileId),
    TestFileName = fslogic_utils:random_ascii_lowercase_sequence(?TEST_FILE_NAME_LEN),
    TestFileId = fslogic_path:join([Dirname, TestFileName]),

    FileContent = helpers_utils:create_test_file(Handle, TestFileId),

    spawn(?MODULE, remove_storage_test_file, [Handle, TestFileId,
        ?REMOVE_STORAGE_TEST_FILE_DELAY]),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #storage_test_file{
            helper_params = HelperParams, space_uuid = SpaceUUID,
            file_id = TestFileId, file_content = FileContent
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Creates handle to the test file in ROOT context and tries to verify
%% its content ?VERIFY_STORAGE_TEST_FILE_ATTEMPTS times.
%% @end
%%--------------------------------------------------------------------
-spec verify_storage_test_file(SessId :: session:id(), #verify_storage_test_file{}) ->
    #fuse_response{}.
verify_storage_test_file(SessId, #verify_storage_test_file{storage_id = StorageId,
    space_uuid = SpaceUUID, file_id = FileId, file_content = FileContent}) ->
    {ok, StorageDoc} = storage:get(StorageId),
    {ok, HelperInit} = fslogic_storage:select_helper(StorageDoc),
    HelperName = helpers:name(HelperInit),
    HelperArgs = helpers:args(HelperInit),
    UserCtx = fslogic_storage:new_user_ctx(HelperInit, SessId, SpaceUUID),
    Handle = helpers_utils:create_test_file_handle(HelperName, HelperArgs, UserCtx),
    verify_storage_test_file_loop(Handle, FileId, FileContent, ?ENOENT, ?VERIFY_STORAGE_TEST_FILE_ATTEMPTS).

%%--------------------------------------------------------------------
%% @doc
%% Removes test file referenced by handle after specified delay.
%% @end
%%--------------------------------------------------------------------
-spec remove_storage_test_file(Handle :: helpers:handle(), FileId :: helpers:file(),
    Delay :: timeout()) -> ok.
remove_storage_test_file(Handle, FileId, Delay) ->
    timer:sleep(Delay),
    helpers_utils:remove_test_file(Handle, FileId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private @doc
%% Verifies storage test file by reading its content and checking it with the
%% one sent by the client. Retires 'Attempts' times if file is not found or
%% its content doesn't match expected one.
%% @end
%%--------------------------------------------------------------------
-spec verify_storage_test_file_loop(Handle :: helpers:handle(), FileId :: helpers:file(),
    FileContent :: binary(), Code :: atom(), Attempts :: non_neg_integer()) -> #fuse_response{}.
verify_storage_test_file_loop(_, _, _, Code, 0) ->
    #fuse_response{status = #status{code = Code}};

verify_storage_test_file_loop(Handle, FileId, FileContent, _, Attempts) ->
    try helpers_utils:read_test_file(Handle, FileId) of
        FileContent ->
            helpers_utils:remove_test_file(Handle, FileId),
            #fuse_response{status = #status{code = ?OK}};
        _ ->
            timer:sleep(?VERIFY_STORAGE_TEST_FILE_DELAY),
            verify_storage_test_file_loop(Handle, FileId, FileContent, ?EINVAL, Attempts - 1)
    catch
        {_, {error, ?ENOENT}} ->
            timer:sleep(?VERIFY_STORAGE_TEST_FILE_DELAY),
            verify_storage_test_file_loop(Handle, FileId, FileContent, ?ENOENT, Attempts - 1)
    end.