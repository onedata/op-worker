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
-export([create_storage_test_file/3, verify_storage_test_file/5]).

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
-spec create_storage_test_file(SessId :: session:id(), StorageId :: storage:id(),
    FileUuid :: file_meta:uuid()) -> ok | {error, Reason :: term()}.
create_storage_test_file(SessId, StorageId, FileUuid) ->
    Response = try
        {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} =
            session:get(SessId),
        {ok, #document{key = SpaceUuid}} = fslogic_spaces:get_space({uuid, FileUuid}, UserId),
        {ok, Storage} = storage:get(StorageId),
        #fuse_response{fuse_response = HelperParams} =
            fslogic_req_regular:get_helper_params(fslogic_context:new(SessId), StorageId, false),
        
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
        
        wrap_response(#storage_test_file{
            storage_id = StorageId, helper_params = HelperParams,
            space_uuid = SpaceUuid, file_id = TestFileId, file_content = FileContent
        }, ?OK)
    catch
        _:Reason ->
            ?error_stacktrace("Cannot create storage test file due to: ~p", [Reason]),
            wrap_response(#storage_test_file{storage_id = StorageId}, ?EAGAIN)
    end,
    communicator:send(Response, SessId).

%%--------------------------------------------------------------------
%% @doc
%% Verifies storage test file by reading its content and checking it with the one sent by the client.
%% @end
%%--------------------------------------------------------------------
-spec verify_storage_test_file(SessId :: session:id(), StorageId :: storage:id(),
    SpaceUuid :: file_meta:uuid(), FileId :: helpers:file(),
    FileContent :: binary()) -> ok | {error, Reason :: term()}.
verify_storage_test_file(SessId, StorageId, SpaceUuid, FileId, FileContent) ->
    Response = try
        {ok, Storage} = storage:get(StorageId),
        
        RootHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceUuid, undefined, Storage, FileId),
        {ok, RootReadHandle} = storage_file_manager:open(RootHandle, read),
        {ok, ActualFileContent} = storage_file_manager:read(RootReadHandle, 0, size(FileContent)),
        
        case FileContent of
            ActualFileContent ->
                wrap_response(#storage_test_file_verification{storage_id = StorageId}, ?OK);
            _ ->
                wrap_response(#storage_test_file_verification{storage_id = StorageId}, ?EINVAL,
                    <<"File content mismatch.">>)
        end
    catch
        _:{badmatch, {error, enoent}} ->
            wrap_response(#storage_test_file_verification{storage_id = StorageId}, ?ENOENT);
        _:Reason ->
            ?error_stacktrace("Cannot verify storage test file due to: ~p", [Reason]),
            wrap_response(#storage_test_file_verification{storage_id = StorageId}, ?EAGAIN)
    end,
    communicator:send(Response, SessId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv wrap_response(Response, Code, undefined)
%% @end
%%--------------------------------------------------------------------
-spec wrap_response(Response :: tuple(), Code :: atom()) -> Msg :: #fuse_response{}.
wrap_response(Response, Code) ->
    wrap_response(Response, Code, undefined).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Wraps response in a fuse response record.
%% @end
%%--------------------------------------------------------------------
-spec wrap_response(Response :: tuple(), Code :: atom(),
    Description :: binary() | undefined) -> Msg :: #fuse_response{}.
wrap_response(Response, Code, Description) ->
    #fuse_response{
        status = #status{code = Code, description = Description},
        fuse_response = Response
    }.
