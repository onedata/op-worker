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

% errors understood by Onepanel
-define(ERR_STORAGE_TEST_FILE_CREATE(Node, Reason),
    {error, {storage_test_file_create, Node, Reason}}).
-define(ERR_STORAGE_TEST_FILE_CREATE(Node, Reason, Stacktrace),
    {error, {storage_test_file_create, Node, Reason}, Stacktrace}).
-define(ERR_STORAGE_TEST_FILE_READ(Node, Reason),
    {error, {storage_test_file_read, Node, Reason}}).
-define(ERR_STORAGE_TEST_FILE_READ(Node, Reason, Stacktrace),
    {error, {storage_test_file_read, Node, Reason}, Stacktrace}).
-define(ERR_STORAGE_TEST_FILE_REMOVE(Node, Reason),
    {error, {storage_test_file_remove, Node, Reason}}).

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
-spec create_test_file(helpers:helper(), helpers:user_ctx(), helpers:file_id()) ->
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
-spec read_test_file(helpers:helper(), helpers:user_ctx(), helpers:file_id()) ->
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
-spec update_test_file(helpers:helper(), helpers:user_ctx(), helpers:file_id()) ->
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
-spec remove_test_file(helpers:helper(), helpers:user_ctx(), helpers:file_id(),
    Size :: non_neg_integer()) -> ok | no_return().
remove_test_file(Helper, UserCtx, FileId, Size) ->
    Handle = helpers:get_helper_handle(Helper, UserCtx),
    case helpers:unlink(Handle, FileId, Size) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, Reason} ->
            throw(?ERR_STORAGE_TEST_FILE_REMOVE(node(), Reason))
    end.

%%-------------------------------------------------------------------
%% @doc
%% Verifies whether storage is accessible for all nodes in the cluster.
%% This function is called by onepanel, BEFORE adding new storage.
%% @end
%%-------------------------------------------------------------------
-spec verify_storage_on_all_nodes(helpers:helper()) ->
    ok | {error, term()} | {error, term(), Stacktrace :: list()}.
verify_storage_on_all_nodes(Helper) ->
    {ok, AdminCtx} = luma:get_admin_ctx(?ROOT_USER_ID, Helper),
    {ok, AdminCtx2} = luma:add_helper_specific_fields(?ROOT_USER_ID,
        ?ROOT_SESS_ID, AdminCtx, Helper),
    {ok, [Node | Nodes]} = node_manager:get_cluster_nodes(),
    FileId = generate_file_id(),
    case create_test_file(Node, Helper, AdminCtx2, FileId) of
        {ok, FileContent} ->
            case verify_storage_internal(Helper, AdminCtx2, Nodes, FileId, FileContent) of
                {ok, {FileId, FileContent}} ->
                    verify_test_file(Node, Helper, AdminCtx2, FileId, FileContent);
                Error ->
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
-spec verify_test_file(helpers:helper(), helpers:user_ctx(), helpers:file_id(),
    Content :: binary()) -> ok | {error, term()}.
verify_test_file(Helper, UserCtx, FileId, ExpectedFileContent) ->
    try
        case read_test_file(Helper, UserCtx, FileId) of
            ExpectedFileContent ->
                ok;
            UnexpectedFileContent ->
                ?error("Unexpected test file content in ~tp", [FileId]),
                ?ERR_STORAGE_TEST_FILE_READ(node(),
                    {invalid_content, ExpectedFileContent, UnexpectedFileContent})
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
-spec verify_storage_internal(helpers:helper(), helpers:user_ctx(), [node()],
    helpers:file_id(), Content :: binary()) ->
    {ok, {helpers:file_id(), binary()}} |
    {error, term()} | {error, term(), Stacktrace :: list()}.
verify_storage_internal(_Helper, _AdminCtx, [], FileId, FileContent) ->
    {ok, {FileId, FileContent}};
verify_storage_internal(Helper, AdminCtx, [Node | Nodes], FileId, ExpectedFileContent) ->
    case verify_test_file(Node, Helper, AdminCtx, FileId, ExpectedFileContent) of
        ok ->
            FileId2 = generate_file_id(),
            case create_test_file(Node, Helper, AdminCtx, FileId) of
                {ok, FileContent} ->
                    verify_storage_internal(Helper, AdminCtx,
                        Nodes, FileId2, FileContent);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec create_test_file(node(), helpers:helper(), helpers:user_ctx(), helpers:file_id()) ->
    {ok, Content :: binary()} | {error, Reason :: term(), Stacktrace :: list()}.
create_test_file(Node, Helper, UserCtx, FileId) ->
    case rpc:call(Node, ?MODULE, create_test_file, [Helper, UserCtx, FileId]) of
        {badrpc, {'EXIT', {Reason, Stacktrace}}} ->
            ?ERR_STORAGE_TEST_FILE_CREATE(Node, Reason, Stacktrace);
        <<Content/binary>> ->
            {ok, Content}
    end.

-spec verify_test_file(node(), helpers:helper(), helpers:user_ctx(),
    helpers:file_id(), Content :: binary()) ->
    ok | {error, term()} | {error, term(), Stacktrace :: list()}.
verify_test_file(Node, Helper, UserCtx, FileId, ExpectedFileContent) ->
    case rpc:call(Node, ?MODULE, verify_test_file,
        [Helper, UserCtx, FileId, ExpectedFileContent]) of
        {badrpc, {'EXIT', {Reason, Stacktrace}}} ->
            ?ERR_STORAGE_TEST_FILE_READ(Node, Reason, Stacktrace);
        Result ->
            Result
    end.
