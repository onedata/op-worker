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

%% API
-export([
    ensure_authorized/3, ensure_authorized/4,
    ensure_authorized_readdir/3
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% @equiv authorize(UserCtx, FileCtx0, AccessRequirements, true).
%% @end
%%--------------------------------------------------------------------
-spec ensure_authorized(user_ctx:ctx(), file_ctx:ctx(),
    [data_access:requirement()]) -> file_ctx:ctx() | no_return().
ensure_authorized(UserCtx, FileCtx0, AccessRequirements) ->
    ensure_authorized(UserCtx, FileCtx0, AccessRequirements, false).


%%--------------------------------------------------------------------
%% @doc
%% Checks access to specified file and verifies data constraints.
%% AllowAncestorsOfPaths means that access can be granted not only for
%% files/directories directly allowed by constraints but also to their
%% ancestors.
%% @end
%%--------------------------------------------------------------------
-spec ensure_authorized(user_ctx:ctx(), file_ctx:ctx(),
    AccessRequirements :: [data_access:requirement()],
    AllowAncestorsOfPaths :: boolean()
) ->
    file_ctx:ctx().
ensure_authorized(
    UserCtx, FileCtx0, AccessRequirements, AllowAncestorsOfPaths
) ->
    {_, FileCtx1} = data_constraints:verify(
        UserCtx, FileCtx0, AllowAncestorsOfPaths
    ),
    data_access:assert_granted(UserCtx, FileCtx1, AccessRequirements).


%%--------------------------------------------------------------------
%% @doc
%% Checks access to specified file, verifies data constraints for readdir
%% operation and returns children whitelist (files which can be listed).
%% It is necessary because readdir can be performed not only on files
%% directly allowed by constraints (in such case whitelist is 'undefined'
%% and all children can be freely listed) but also on their ancestors, in
%% case of which only children which lead to paths allowed by constraints
%% should be returned (such children are returned in whitelist).
%% @end
%%--------------------------------------------------------------------
-spec ensure_authorized_readdir(user_ctx:ctx(), file_ctx:ctx(),
    [data_access:requirement()]
) ->
    {ChildrenWhiteList :: undefined | [file_meta:name()], file_ctx:ctx()}.
ensure_authorized_readdir(UserCtx, FileCtx0, AccessRequirements) ->
    FileCtx1 = data_access:assert_granted(
        UserCtx, FileCtx0, AccessRequirements
    ),
    data_constraints:verify(UserCtx, FileCtx1, true).
