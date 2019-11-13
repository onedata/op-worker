%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains helpers utility functions.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_detector).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([generate_file_id/0, create_test_file/3, read_test_file/3,
    update_test_file/3, remove_test_file/4]).

%% Onepanel RPC
-export([verify_storage_on_all_nodes/1]).

%% exported for RPC
-export([verify_test_file/4]).


-define(TEST_FILE_NAME_LEN, application:get_env(?APP_NAME,
    storage_test_file_name_size, 32)).
-define(TEST_FILE_CONTENT_LEN, application:get_env(?APP_NAME,
    storage_test_file_content_size, 100)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Generates random file ID consisting of ASCII lowercase letters.
%% @end
%%--------------------------------------------------------------------
-spec generate_file_id() -> binary().
generate_file_id() ->
    file_meta:hidden_file_name(random_ascii_lowercase_sequence(?TEST_FILE_NAME_LEN)).

%%--------------------------------------------------------------------
%% @doc
%% Creates storage test file.
%% @end
%%--------------------------------------------------------------------
-spec create_test_file(helpers:helper(), helper:user_ctx(), helpers:file_id()) ->
    Content :: binary().
create_test_file(Helper, UserCtx, FileId) ->
    Handle = helpers:get_helper_handle(Helper, UserCtx),
    Content = random_ascii_lowercase_sequence(?TEST_FILE_CONTENT_LEN),
    ok = helpers:mknod(Handle, FileId, 8#666, reg),
    {ok, FileHandle} = helpers:open(Handle, FileId, write),
    {ok, _} = helpers:write(FileHandle, 0, Content),
    ok = helpers:release(FileHandle),
    Content.

%%--------------------------------------------------------------------
%% @doc
%% Returns content of storage test file.
%% @end
%%--------------------------------------------------------------------
-spec read_test_file(helpers:helper(), helper:user_ctx(), helpers:file_id()) ->
    Content :: binary().
read_test_file(Helper, UserCtx, FileId) ->
    Handle = helpers:get_helper_handle(Helper, UserCtx),
    {ok, FileHandle} = helpers:open(Handle, FileId, read),
    {ok, Content} = helpers:read(FileHandle, 0, ?TEST_FILE_CONTENT_LEN),
    ok = helpers:release(FileHandle),
    Content.

%%--------------------------------------------------------------------
%% @doc
%% Updates and returns content of a storage test file.
%% @end
%%--------------------------------------------------------------------
-spec update_test_file(helpers:helper(), helper:user_ctx(), helpers:file_id()) ->
    Content :: binary().
update_test_file(Helper, UserCtx, FileId) ->
    Handle = helpers:get_helper_handle(Helper, UserCtx),
    Content = random_ascii_lowercase_sequence(?TEST_FILE_CONTENT_LEN),
    {ok, FileHandle} = helpers:open(Handle, FileId, write),
    {ok, _} = helpers:write(FileHandle, 0, Content),
    ok = helpers:release(FileHandle),
    Content.

%%--------------------------------------------------------------------
%% @doc
%% Removes storage test file.
%% @end
%%--------------------------------------------------------------------
-spec remove_test_file(helpers:helper(), helper:user_ctx(), helpers:file_id(),
    Size :: non_neg_integer()) -> ok | no_return().
remove_test_file(Helper, UserCtx, FileId, Size) ->
    Handle = helpers:get_helper_handle(Helper, UserCtx),
    case helpers:unlink(Handle, FileId, Size) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, Reason} ->
            ?error("Storage test file removal failed: ~tp", [Reason]),
            throw(?ERROR_STORAGE_TEST_FAILED(remove))
    end.

%%-------------------------------------------------------------------
%% @doc
%% Verifies whether storage is accessible for all nodes in the cluster.
%% This function is called by onepanel, BEFORE adding new storage.
%% @end
%%-------------------------------------------------------------------
-spec verify_storage_on_all_nodes(helpers:helper()) ->
    ok | ?ERROR_STORAGE_TEST_FAILED(_).
verify_storage_on_all_nodes(Helper) ->
    {ok, AdminCtx} = luma:get_admin_ctx(?ROOT_USER_ID, Helper),
    {ok, AdminCtx2} = luma:add_helper_specific_fields(?ROOT_USER_ID,
        ?ROOT_SESS_ID, AdminCtx, Helper),
    {ok, [Node | Nodes]} = node_manager:get_cluster_nodes(),
    FileId = generate_file_id(),
    case create_test_file(Node, Helper, AdminCtx2, FileId) of
        {ok, FileContent} ->
            case verify_storage_internal(Helper, AdminCtx2, Nodes, FileId, FileContent) of
                {ok, {FileId2, FileContent2}} ->
                    verify_test_file(Node, Helper, AdminCtx2, FileId2, FileContent2);
                {error, _} = Error ->
                    Error
            end;
        Error ->
            Error
    end.

%%%===================================================================
%%% RPC
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Verifies content of storage test file and removes it.
%% @end
%%--------------------------------------------------------------------
-spec verify_test_file(helpers:helper(), helper:user_ctx(), helpers:file_id(),
    Content :: binary()) -> ok | ?ERROR_STORAGE_TEST_FAILED(read).
verify_test_file(Helper, UserCtx, FileId, ExpectedFileContent) ->
    try
        case read_test_file(Helper, UserCtx, FileId) of
            ExpectedFileContent ->
                ok;
            UnexpectedFileContent ->
                ?error("Unexpected storage test file content in ~tp~n" ++
                "Expected: ~tp~nObtained: ~tp", [FileId, ExpectedFileContent, UnexpectedFileContent]),
                ?ERROR_STORAGE_TEST_FAILED(read)
        end
    after
        remove_test_file(Helper, UserCtx, FileId, byte_size(ExpectedFileContent))
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Create random sequence consisting of lowercase ASCII letters.
%% @end
%%--------------------------------------------------------------------
-spec random_ascii_lowercase_sequence(Length :: integer()) -> binary().
random_ascii_lowercase_sequence(Length) ->
    rand:seed(exs1024),
    lists:foldl(fun(_, Acc) ->
        <<Acc/binary, (rand:uniform(26) + 96)>>
    end, <<>>, lists:seq(1, Length)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function is used to verify whether storage is accessible on
%% all nodes. It verifies whether it can read the file referenced by
%% FileId and whether its content is equal to FileContent.
%% Next, it creates a new test file and recursively calls itself
%% on the next node from the Nodes list.
%% @end
%%-------------------------------------------------------------------
-spec verify_storage_internal(helpers:helper(), helper:user_ctx(), [node()],
    helpers:file_id(), Content :: binary()) ->
    {ok, {helpers:file_id(), binary()}} | ?ERROR_STORAGE_TEST_FAILED(_).
verify_storage_internal(_Helper, _AdminCtx, [], FileId, FileContent) ->
    {ok, {FileId, FileContent}};
verify_storage_internal(Helper, AdminCtx, [Node | Nodes], FileId, ExpectedFileContent) ->
    case verify_test_file(Node, Helper, AdminCtx, FileId, ExpectedFileContent) of
        ok ->
            NewFileId = generate_file_id(),
            case create_test_file(Node, Helper, AdminCtx, NewFileId) of
                {ok, NewFileContent} ->
                    verify_storage_internal(Helper, AdminCtx,
                        Nodes, NewFileId, NewFileContent);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec create_test_file(node(), helpers:helper(), helper:user_ctx(), helpers:file_id()) ->
    {ok, Content :: binary()} | ?ERROR_STORAGE_TEST_FAILED(write).
create_test_file(Node, Helper, UserCtx, FileId) ->
    case rpc:call(Node, ?MODULE, create_test_file, [Helper, UserCtx, FileId]) of
        {badrpc, {'EXIT', {Reason, Stacktrace}}} ->
            ?error("Storage test file creation failed: ~tp~n~tp", [Reason, Stacktrace]),
            ?ERROR_STORAGE_TEST_FAILED(write);
        <<Content/binary>> ->
            {ok, Content}
    end.

-spec verify_test_file(node(), helpers:helper(), helper:user_ctx(),
    helpers:file_id(), Content :: binary()) ->
    ok | ?ERROR_STORAGE_TEST_FAILED(read) | ?ERROR_STORAGE_TEST_FAILED(delete).
verify_test_file(Node, Helper, UserCtx, FileId, ExpectedFileContent) ->
    case rpc:call(Node, ?MODULE, verify_test_file,
        [Helper, UserCtx, FileId, ExpectedFileContent]) of
        {badrpc, {'EXIT', {Reason, Stacktrace}}} ->
            ?error("Storage test file read failed: ~tp~n~tp", [Reason, Stacktrace]),
            ?ERROR_STORAGE_TEST_FAILED(read);
        Result ->
            % either success or a thrown delete error
            Result
    end.
