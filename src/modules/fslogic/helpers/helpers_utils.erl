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
-export([create_test_file_handle/3, create_test_file/2, create_test_file/3,
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
    HelperArgsMap = case {Name, UserCtx} of
        {?CEPH_HELPER_NAME, #ceph_user_ctx{user_name = UserName, user_key = UserKey}} ->
            Args#{<<"user_name">> => UserName, <<"key">> => UserKey};
        {?S3_HELPER_NAME, #s3_user_ctx{access_key = AccessKey, secret_key = SecretKey}} ->
            Args#{<<"access_key">> => AccessKey, <<"secret_key">> => SecretKey};
        {?SWIFT_HELPER_NAME, #swift_user_ctx{user_name = UserName, password = Password}} ->
            Args#{<<"user_name">> => UserName, <<"password">> => Password};
        {?DIRECTIO_HELPER_NAME, #posix_user_ctx{}} ->
            Args
    end,
    HelperArgs = [#helper_arg{key = K, value = V} || {K, V} <- maps:to_list(HelperArgsMap)],
    #helper_params{helper_name = Name, helper_args = HelperArgs}.

%%--------------------------------------------------------------------
%% @doc
%% Creates test file handle and sets user context.
%% @end
%%--------------------------------------------------------------------
-spec create_test_file_handle(HelperName :: helpers:name(), HelperArgs :: helpers:args(),
    UserCtx :: helpers_user:ctx()) -> Handle :: helpers:handle().
create_test_file_handle(HelperName, HelperArgs, UserCtx) ->
    Handle = helpers:new_handle(HelperName, HelperArgs),
    ok = helpers:set_user_ctx(Handle, UserCtx),
    Handle.

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
    Handle = create_test_file_handle(HelperName, HelperArgs, UserCtx),
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
    {ok, _} = helpers:open(Handle, FileId, write),
    {ok, _} = helpers:write(Handle, FileId, 0, Content),
    ok = helpers:release(Handle, FileId),
    Content.

%%--------------------------------------------------------------------
%% @doc
%% Returns content of storage test file.
%% @end
%%--------------------------------------------------------------------
-spec read_test_file(HelperName :: helpers:name(), HelperArgs :: helpers:args(),
    UserCtx :: helpers_user:ctx(), FileId :: helpers:file()) -> Content :: binary().
read_test_file(HelperName, HelperArgs, UserCtx, FileId) ->
    Handle = create_test_file_handle(HelperName, HelperArgs, UserCtx),
    read_test_file(Handle, FileId).

%%--------------------------------------------------------------------
%% @doc
%% Returns content of storage test file.
%% @end
%%--------------------------------------------------------------------
-spec read_test_file(Handle :: helpers:handle(), FileId :: helpers:file()) ->
    Content :: binary().
read_test_file(Handle, FileId) ->
    {ok, _} = helpers:open(Handle, FileId, read),
    {ok, Content} = helpers:read(Handle, FileId, 0, ?TEST_FILE_LEN),
    ok = helpers:release(Handle, FileId),
    Content.

%%--------------------------------------------------------------------
%% @doc
%% Updates and returns content of a storage test file.
%% @end
%%--------------------------------------------------------------------
-spec update_test_file(HelperName :: helpers:name(), HelperArgs :: helpers:args(),
    UserCtx :: helpers_user:ctx(), FileId :: helpers:file()) -> Content :: binary().
update_test_file(HelperName, HelperArgs, UserCtx, FileId) ->
    Handle = create_test_file_handle(HelperName, HelperArgs, UserCtx),
    Content = fslogic_utils:random_ascii_lowercase_sequence(?TEST_FILE_LEN),
    {ok, _} = helpers:open(Handle, FileId, write),
    {ok, _} = helpers:write(Handle, FileId, 0, Content),
    ok = helpers:release(Handle, FileId),
    Content.

%%--------------------------------------------------------------------
%% @doc
%% Removes storage test file.
%% @end
%%--------------------------------------------------------------------
-spec remove_test_file(HelperName :: helpers:name(), HelperArgs :: helpers:args(),
    UserCtx :: helpers_user:ctx(), FileId :: helpers:file()) -> ok.
remove_test_file(HelperName, HelperArgs, UserCtx, FileId) ->
    Handle = create_test_file_handle(HelperName, HelperArgs, UserCtx),
    remove_test_file(Handle, FileId).

%%--------------------------------------------------------------------
%% @doc
%% Removes storage test file.
%% @end
%%--------------------------------------------------------------------
-spec remove_test_file(Handle :: helpers:handle(), FileId :: helpers:file()) -> ok.
remove_test_file(Handle, FileId) ->
    ok = helpers:unlink(Handle, FileId).