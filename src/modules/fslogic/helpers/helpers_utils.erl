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
-module(helpers_utils).
-author("Krzysztof Trzepla").

-include("modules/fslogic/helpers.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").

%% API
-export([get_params/2]).
-export([create_test_file/2, create_test_file/3,
    create_test_file/4, read_test_file/4, read_test_file/2, update_test_file/4,
    remove_test_file/4, remove_test_file/2]).

-define(TEST_FILE_ID_LEN, 32).
-define(TEST_FILE_LEN, 100).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns helper parameters.
%% @end
%%--------------------------------------------------------------------
-spec get_params(HelperInit :: helpers:init(), UserCtx :: helpers_user:ctx()) ->
    #helper_params{}.
get_params(#helper_init{name = Name, args = Args}, UserCtx) ->
    HelperArgsMap = case UserCtx of
        #posix_user_ctx{} -> Args;
        _ -> maps:merge(Args, helpers:extract_args(UserCtx))
    end,
    HelperArgs = [#helper_arg{key = K, value = V} || {K, V} <- maps:to_list(HelperArgsMap)],
    #helper_params{helper_name = Name, helper_args = HelperArgs}.

%%--------------------------------------------------------------------
%% @doc
%% Creates storage test file.
%% @end
%%--------------------------------------------------------------------
-spec create_test_file(HelperName :: helpers:name(), HelperArgs :: helpers:args(),
    UserCtx :: helpers_user:ctx()) -> {FileId :: helpers:file(), Content :: binary()}.
create_test_file(HelperName, HelperArgs, UserCtx) ->
    FileId = fslogic_utils:random_ascii_lowercase_sequence(?TEST_FILE_ID_LEN),
    {FileId, create_test_file(HelperName, HelperArgs, UserCtx, FileId)}.

%%--------------------------------------------------------------------
%% @doc
%% Creates storage test file with given ID.
%% @end
%%--------------------------------------------------------------------
-spec create_test_file(HelperName :: helpers:name(), HelperArgs :: helpers:args(),
    UserCtx :: helpers_user:ctx(), FileId :: helpers:file()) -> Content :: binary().
create_test_file(HelperName, HelperArgs, UserCtx, FileId) ->
    Handle = helpers:new_handle(HelperName, HelperArgs, UserCtx),
    create_test_file(Handle, FileId).

%%--------------------------------------------------------------------
%% @doc
%% Creates storage test file with given ID.
%% @end
%%--------------------------------------------------------------------
-spec create_test_file(Handle :: helpers:handle(), FileId :: helpers:file()) ->
    Content :: binary().
create_test_file(Handle, FileId) ->
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
-spec read_test_file(HelperName :: helpers:name(), HelperArgs :: helpers:args(),
    UserCtx :: helpers_user:ctx(), FileId :: helpers:file()) -> Content :: binary().
read_test_file(HelperName, HelperArgs, UserCtx, FileId) ->
    Handle = helpers:new_handle(HelperName, HelperArgs, UserCtx),
    read_test_file(Handle, FileId).

%%--------------------------------------------------------------------
%% @doc
%% Returns content of storage test file.
%% @end
%%--------------------------------------------------------------------
-spec read_test_file(Handle :: helpers:handle(), FileId :: helpers:file()) ->
    Content :: binary().
read_test_file(Handle, FileId) ->
    {ok, FileHandle} = helpers:open(Handle, FileId, read),
    {ok, Content} = helpers:read(FileHandle, 0, ?TEST_FILE_LEN),
    ok = helpers:release(FileHandle),
    Content.

%%--------------------------------------------------------------------
%% @doc
%% Updates and returns content of a storage test file.
%% @end
%%--------------------------------------------------------------------
-spec update_test_file(HelperName :: helpers:name(), HelperArgs :: helpers:args(),
    UserCtx :: helpers_user:ctx(), FileId :: helpers:file()) -> Content :: binary().
update_test_file(HelperName, HelperArgs, UserCtx, FileId) ->
    Handle = helpers:new_handle(HelperName, HelperArgs, UserCtx),
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
-spec remove_test_file(HelperName :: helpers:name(), HelperArgs :: helpers:args(),
    UserCtx :: helpers_user:ctx(), FileId :: helpers:file()) -> ok.
remove_test_file(HelperName, HelperArgs, UserCtx, FileId) ->
    Handle = helpers:new_handle(HelperName, HelperArgs, UserCtx),
    remove_test_file(Handle, FileId).

%%--------------------------------------------------------------------
%% @doc
%% Removes storage test file.
%% @end
%%--------------------------------------------------------------------
-spec remove_test_file(Handle :: helpers:handle(), FileId :: helpers:file()) -> ok.
remove_test_file(Handle, FileId) ->
    ok = helpers:unlink(Handle, FileId).
