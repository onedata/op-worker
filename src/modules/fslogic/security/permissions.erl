%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME.
%%% @end
%%%-------------------------------------------------------------------
-module(permissions).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/errors.hrl").

%% API
-export([check_permission/3, check/3]).

-type check_type() ::
    owner % Check whether user owns the item
    | traverse_ancestors % validates ancestors' exec permission.
    | owner_if_parent_sticky % Check whether user owns the item but only if parent of the item has sticky bit.
    | share % Check if the file (or its ancestor) is shared
    | write | read | exec | rdwr
    | acl_access_mask().
-type acl_access_mask() :: binary().

-type access_definition() :: root | check_type() | {check_type(), 'or', check_type()}.

-export_type([check_type/0, access_definition/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec check(user_ctx:ctx(), file_ctx:ctx(), [access_definition()]) ->
    file_ctx:ctx() | no_return().
check(UserCtx, FileCtx0, AccessDefinitions0) ->
    AccessDefinitions1 = case user_ctx:is_root(UserCtx) of
        true ->
            [];
        false ->
            case user_ctx:is_guest(UserCtx) of
                true -> [share | AccessDefinitions0];
                false -> AccessDefinitions0
            end
    end,
    check_permissions(UserCtx, FileCtx0, AccessDefinitions1).


-spec check_permission(access_definition(), user_ctx:ctx(), file_ctx:ctx()) ->
    file_ctx:ctx() | no_return().
check_permission(Perm, UserCtx, FileCtx0) ->
    UserId = user_ctx:get_user_id(UserCtx),
    Guid = file_ctx:get_guid_const(FileCtx0),
    case permissions_cache:check_permission({Perm, UserId, Guid}) of
        {ok, ok} ->
            FileCtx0;
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            try
                {ok, FileCtx1} = rules:check_normal_or_default_def(
                    Perm, UserCtx, FileCtx0
                ),
                permissions_cache:cache_permission({Perm, UserId, Guid}, ok),
                FileCtx1
            catch _:?EACCES ->
                permissions_cache:cache_permission({Perm, UserId, Guid}, ?EACCES),
                throw(?EACCES)
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_permissions(user_ctx:ctx(), file_ctx:ctx(), [access_definition()]) ->
    file_ctx:ctx().
check_permissions(_UserCtx, FileCtx, []) ->
    FileCtx;
check_permissions(UserCtx, FileCtx0, [AccessDefinition | Rest]) ->
    FileCtx1 = check_permission(AccessDefinition, UserCtx, FileCtx0),
    check_permissions(UserCtx, FileCtx1, Rest).
