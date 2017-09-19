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

%% API
-export([generate_file_id/0, create_test_file/3, read_test_file/3,
    update_test_file/3, remove_test_file/3]).

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
    random_ascii_lowercase_sequence(?TEST_FILE_NAME_LEN).

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