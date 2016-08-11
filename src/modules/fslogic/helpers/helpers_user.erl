%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module contains utility functions for helpers' users
%%% management.
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_user).
-author("Krzysztof Trzepla").

-include("modules/fslogic/helpers.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").

%% API
-export([add/4, get/2, get_ctx/3]).

-type model() :: posix_user | swift_user | ceph_user | s3_user.
-type ctx() :: posix_user:ctx() | ceph_user:ctx() | s3_user:ctx() | swift_user:ctx().
-type ctx_map() :: #{binary() => binary()}.
-type type() :: posix_user:type() | ceph_user:type() | s3_user:type().

-export_type([model/0, ctx/0, ctx_map/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Add storage ctx to user document and saves in datastore.
%% @end
%%--------------------------------------------------------------------
-spec add(UserModel :: model(), UserId :: onedata_user:id(), StorageId :: storage:id(),
    UserCtx :: ctx()) -> {ok, UserId :: onedata_user:id()} | {error, Reason :: term()}.
add(UserModel, UserId, StorageId, UserCtx) ->
    Diff = fun(User) -> {ok, UserModel:add_ctx(StorageId, UserCtx, User)} end,
    case UserModel:update(UserId, Diff) of
        {ok, UserId} -> {ok, UserId};
        {error, {not_found, _}} ->
            UserDoc = UserModel:new(UserId, StorageId, UserCtx),
            case UserModel:create(UserDoc) of
                {ok, UserId} -> {ok, UserId};
                {error, already_exists} ->
                    add(UserModel, UserId, StorageId, UserCtx);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns user record from datastore.
%% @end
%%--------------------------------------------------------------------
-spec get(UserModel :: model(), UserId :: onedata_user:id()) ->
    {ok, User :: type()} | {error, Reason :: term()}.
get(UserModel, UserId) ->
    case UserModel:get(UserId) of
        {ok, #document{value = User}} -> {ok, User};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns user storage context from datastore.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx(UserModel :: model(), UserId :: onedata_user:id(), StorageId :: storage:id()) ->
    UserCtx :: ctx() | undefined.
get_ctx(UserModel, UserId, StorageId) ->
    case get(UserModel, UserId) of
        {ok, User} ->
            maps:get(StorageId, UserModel:get_all_ctx(User), undefined);
        _ -> undefined
    end.