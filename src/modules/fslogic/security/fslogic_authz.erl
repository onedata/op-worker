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
-export([ensure_authorized/3, ensure_authorized/4]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% @equiv authorize(UserCtx, FileCtx0, AccessRequirements, true).
%% @end
%%--------------------------------------------------------------------
-spec ensure_authorized(user_ctx:ctx(), file_ctx:ctx(),
    [fslogic_access:requirement()]) -> file_ctx:ctx() | no_return().
ensure_authorized(UserCtx, FileCtx0, AccessRequirements) ->
    ensure_authorized(UserCtx, FileCtx0, AccessRequirements, false).


%%--------------------------------------------------------------------
%% @doc
%% Checks access to specified file and verifies data caveats.
%% AllowAncestorsOfLocationCaveats means that permission can be granted
%% not only for files in subpaths allowed by caveats but also for their
%% ancestors.
%% @end
%%--------------------------------------------------------------------
-spec ensure_authorized(user_ctx:ctx(), file_ctx:ctx(),
    [fslogic_access:requirement()], boolean()) -> file_ctx:ctx() | no_return().
ensure_authorized(
    UserCtx, FileCtx0, AccessRequirements, AllowAncestorsOfLocationCaveats
) ->
    FileCtx2 = case fslogic_caveats:verify_data_caveats(
        UserCtx, FileCtx0, AllowAncestorsOfLocationCaveats
    ) of
        {subpath, FileCtx1} -> FileCtx1;
        {ancestor, FileCtx1, _} -> FileCtx1
    end,
    fslogic_access:assert_granted(UserCtx, FileCtx2, AccessRequirements).
