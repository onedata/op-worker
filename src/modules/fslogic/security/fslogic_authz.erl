%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for authorization of fslogic operations.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_authz).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/errors.hrl").

%% API
-export([authorize/3, check_access/3]).

-type access_type() ::
    owner % Check whether user owns the item
    | traverse_ancestors % validates ancestors' exec permission.
    | owner_if_parent_sticky % Check whether user owns the item but only if parent of the item has sticky bit.
    | share % Check if the file (or its ancestor) is shared
    | write | read | exec | rdwr % posix perms
    | binary(). % acl perms

-type access_definition() ::
    root
    | access_type()
    | {access_type(), 'or', access_type()}.

-export_type([access_definition/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec authorize(user_ctx:ctx(), file_ctx:ctx(), [access_definition()]) ->
    file_ctx:ctx() | no_return().
authorize(UserCtx, FileCtx0, AccessDefinitions0) ->
    AccessDefinitions1 = case user_ctx:is_root(UserCtx) of
        true ->
            [];
        false ->
            case user_ctx:is_guest(UserCtx) of
                true -> [share | AccessDefinitions0];
                false -> AccessDefinitions0
            end
    end,
    check_access_defs(UserCtx, FileCtx0, AccessDefinitions1).


-spec check_access(user_ctx:ctx(), file_ctx:ctx(), access_definition()) ->
    file_ctx:ctx() | no_return().
check_access(UserCtx, FileCtx0, AccessDef) ->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_guid_const(FileCtx0),
    CacheKey = {AccessDef, UserId, Guid},
    case permissions_cache:check_permission(CacheKey) of
        {ok, ok} ->
            FileCtx0;
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            try
                {ok, FileCtx1} = rules:check_normal_def(
                    AccessDef, UserCtx, FileCtx0
                ),
                permissions_cache:cache_permission(CacheKey, ok),
                FileCtx1
            catch _:?EACCES ->
                permissions_cache:cache_permission(CacheKey, ?EACCES),
                throw(?EACCES)
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_access_defs(user_ctx:ctx(), file_ctx:ctx(), [access_definition()]) ->
    file_ctx:ctx().
check_access_defs(_UserCtx, FileCtx, []) ->
    FileCtx;
check_access_defs(UserCtx, FileCtx0, [AccessDefinition | Rest]) ->
    FileCtx1 = check_access(UserCtx, FileCtx0, AccessDefinition),
    check_access_defs(UserCtx, FileCtx1, Rest).
