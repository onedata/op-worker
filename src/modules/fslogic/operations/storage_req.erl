%%%--------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests related to storage
%%% This module contains functions used during remote client
%%% initialization.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_req).
-author("Krzysztof Trzepla").
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("annotations/include/annotations.hrl").

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

%% API
-export([get_configuration/1, get_helper_params/3, create_storage_test_file/3,
    verify_storage_test_file/5, remove_storage_test_file/3]).

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
        root_uuid = fslogic_uuid:uuid_to_guid(fslogic_uuid:user_root_dir_uuid(UserId)),
        subscriptions = Subs,
        disabled_spaces = space_quota:get_disabled_spaces()
    }.

%%--------------------------------------------------------------------
%% @doc
%% Gets helper params based on given storage ID.
%% @end
%%--------------------------------------------------------------------
-spec get_helper_params(fslogic_context:ctx(), storage:id(),
    ForceCL :: boolean()) -> #fuse_response{}.
get_helper_params(_Ctx, StorageId, true = _ForceProxy) ->
    {ok, StorageDoc} = storage:get(StorageId),
    {ok, #helper_init{args = Args}} = fslogic_storage:select_helper(StorageDoc),
    Timeout = helpers_utils:get_timeout(Args),
    {ok, Latency} = application:get_env(?APP_NAME, proxy_helper_latency_milliseconds),
    #fuse_response{status = #status{code = ?OK}, fuse_response = #helper_params{
        helper_name = <<"ProxyIO">>,
        helper_args = [
            #helper_arg{key = <<"storage_id">>, value = StorageId},
            #helper_arg{key = <<"timeout">>, value = integer_to_binary(Timeout + Latency)}
        ]
    }};
get_helper_params(_Ctx, _StorageId, false = _ForceProxy) ->
    #fuse_response{status = #status{code = ?ENOTSUP}}.

%%--------------------------------------------------------------------
%% @doc
%% Creates test file on a given storage in the directory of a file referenced
%% by UUID. File is created on behalf of the user associated with the session.
%% @end
%%--------------------------------------------------------------------
-spec create_storage_test_file(fslogic_context:ctx(), fslogic_worker:file_guid(), storage:id()) ->
    #fuse_response{}.
create_storage_test_file(Ctx, Guid, StorageId) ->
    SessId = fslogic_context:get_session_id(Ctx),
    File = file_info:new_by_guid(Guid),
    SpaceDirUuid = file_info:get_space_dir_uuid(File),

    {ok, StorageDoc} = storage:get(StorageId),
    {ok, HelperInit} = fslogic_storage:select_helper(StorageDoc),
    HelperName = helpers:name(HelperInit),
    HelperArgs = helpers:args(HelperInit),
    UserCtx = fslogic_storage:new_user_ctx(HelperInit, SessId, SpaceDirUuid),
    Handle = helpers:new_handle(HelperName, HelperArgs, UserCtx),
    HelperParams = helpers_utils:get_params(HelperInit, UserCtx),

    {FileId, _NewFile} = file_info:get_storage_file_id(File),
    Dirname = fslogic_path:dirname(FileId),
    TestFileName = fslogic_utils:random_ascii_lowercase_sequence(?TEST_FILE_NAME_LEN),
    TestFileId = fslogic_path:join([Dirname, TestFileName]),

    FileContent = helpers_utils:create_test_file(Handle, TestFileId),

    spawn(storage_req, remove_storage_test_file, [Handle, TestFileId,
        ?REMOVE_STORAGE_TEST_FILE_DELAY]),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #storage_test_file{
            helper_params = HelperParams, space_uuid = SpaceDirUuid,
            file_id = TestFileId, file_content = FileContent
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Creates handle to the test file in ROOT context and tries to verify
%% its content ?VERIFY_STORAGE_TEST_FILE_ATTEMPTS times.
%% @end
%%--------------------------------------------------------------------
-spec verify_storage_test_file(fslogic_context:ctx(), SpaceDirUuid :: file_meta:uuid(),
    storage:id(), helpers:file(), FileConent :: binary()) ->
    #fuse_response{}.
verify_storage_test_file(Ctx, SpaceDirUuid, StorageId, FileId, FileContent) ->
    SessId = fslogic_context:get_session_id(Ctx),
    {ok, StorageDoc} = storage:get(StorageId),
    {ok, HelperInit} = fslogic_storage:select_helper(StorageDoc),
    HelperName = helpers:name(HelperInit),
    HelperArgs = helpers:args(HelperInit),
    UserCtx = fslogic_storage:new_user_ctx(HelperInit, SessId, SpaceDirUuid),
    Handle = helpers:new_handle(HelperName, HelperArgs, UserCtx),
    verify_storage_test_file_loop(Handle, FileId, FileContent, ?ENOENT, ?VERIFY_STORAGE_TEST_FILE_ATTEMPTS).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes test file referenced by handle after specified delay.
%% @end
%%--------------------------------------------------------------------
-spec remove_storage_test_file(Handle :: helpers:handle(), FileId :: helpers:file(),
    Delay :: timeout()) -> ok.
remove_storage_test_file(Handle, FileId, Delay) ->
    timer:sleep(Delay),
    helpers_utils:remove_test_file(Handle, FileId).

%%--------------------------------------------------------------------
%% @private
%% @doc
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
