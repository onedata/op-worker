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

%% API
-export([generate_file_id/0, create_test_file/3, read_test_file/3,
    update_test_file/3, remove_test_file/3]).

-define(TEST_FILE_ID_LEN, 32).
-define(TEST_FILE_LEN, 100).

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
    fslogic_utils:random_ascii_lowercase_sequence(?TEST_FILE_ID_LEN).

%%--------------------------------------------------------------------
%% @doc
%% Creates storage test file.
%% @end
%%--------------------------------------------------------------------
-spec create_test_file(helpers:helper(), helpers:user_ctx(), helpers:file_id()) ->
    Content :: binary().
create_test_file(Helper, UserCtx, FileId) ->
    Handle = helpers:get_helper_handle(Helper, UserCtx),
    Content = fslogic_utils:random_ascii_lowercase_sequence(?TEST_FILE_LEN),
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
    {ok, Content} = helpers:read(FileHandle, 0, ?TEST_FILE_LEN),
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
    Content = fslogic_utils:random_ascii_lowercase_sequence(?TEST_FILE_LEN),
    {ok, FileHandle} = helpers:open(Handle, FileId, write),
    {ok, _} = helpers:write(FileHandle, 0, Content),
    ok = helpers:release(FileHandle),
    Content.

%%--------------------------------------------------------------------
%% @doc
%% Removes storage test file.
%% @end
%%--------------------------------------------------------------------
-spec remove_test_file(helpers:helper(), helpers:user_ctx(), helpers:file_id()) ->
    ok.
remove_test_file(Helper, UserCtx, FileId) ->
    Noop = fun(_) -> ok end,
    Handle = helpers:get_helper_handle(Helper, UserCtx),
    case helpers:unlink(Handle, FileId) of
        ok -> ok;
        {error, enoent} -> ok
    end,
    Noop(Handle),
    ok.