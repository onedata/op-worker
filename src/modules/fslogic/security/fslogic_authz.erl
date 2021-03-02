%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for authorization of fslogic operations.
%%%
%%% Onedata provides several file system security models that limit access to
%%% files and directories. Those are:
%%% - access token caveats (data constraints),
%%% - space ownership,
%%% - space privileges,
%%% - POSIX permissions,
%%% - CDMI access control lists (ACLs).
%%%
%%% These models fit together as follows:
%%% 1. If user access token caveats forbids the requested access,
%%%    the request is denied.
%%% 2. If user is space owner, the request is granted.
%%% 3. If user lacks `space_write_data` space privilege in case of operation
%%%    that modifies file or directory (content, attributes, metadata, etc.)
%%%    or `space_read_data` space privilege in case of operation that reads
%%%    file or directory (content, attributes, metadata, etc.), the request
%%%    is denied.
%%% 4. If an ACL exists on the file, it is evaluated and used to determine
%%%    whether access should be granted.
%%% 5. Otherwise, POSIX permissions are checked.
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
    AccessRequirements :: [data_access_control:requirement()]
) ->
    file_ctx:ctx().
ensure_authorized(UserCtx, FileCtx0, AccessRequirements) ->
    ensure_authorized(
        UserCtx, FileCtx0,
        AccessRequirements, disallow_ancestors
    ).


%%--------------------------------------------------------------------
%% @doc
%% Checks access to specified file and verifies data constraints.
%% AncestorPolicy tells whether access can be granted not only for
%% files/directories directly allowed by constraints but also to their
%% ancestors.
%% @end
%%--------------------------------------------------------------------
-spec ensure_authorized(user_ctx:ctx(), file_ctx:ctx(),
    AccessRequirements :: [data_access_control:requirement()],
    data_constraints:ancestor_policy()
) ->
    file_ctx:ctx().
ensure_authorized(UserCtx, FileCtx0, AccessRequirements, AncestorPolicy) ->
    {_, FileCtx1} = data_constraints:inspect(
        UserCtx, FileCtx0, AncestorPolicy, AccessRequirements
    ),
    data_access_control:assert_granted(UserCtx, FileCtx1, AccessRequirements).


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
    AccessRequirements :: [data_access_control:requirement()]
) ->
    {ChildrenWhiteList :: undefined | [file_meta:name()], file_ctx:ctx()}.
ensure_authorized_readdir(UserCtx, FileCtx0, AccessRequirements) ->
    {ChildrenWhitelist, FileCtx1} = data_constraints:inspect(
        UserCtx, FileCtx0, allow_ancestors, AccessRequirements
    ),
    FileCtx2 = data_access_control:assert_granted(
        UserCtx, FileCtx1, AccessRequirements
    ),
    {ChildrenWhitelist, FileCtx2}.
