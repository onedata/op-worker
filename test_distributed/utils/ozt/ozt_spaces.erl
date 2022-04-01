%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common functions related to spaces operations in Onezone to be used 
%%% in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(ozt_spaces).
-author("Michal Stanisz").

-include_lib("ctool/include/test/assertions.hrl").

-export([set_privileges/3]).


%%%===================================================================
%%% API
%%%===================================================================

-spec set_privileges(od_space:id(), od_user:id(), privileges:privileges(privileges:space_privilege())) -> ok.
% hack necessary as unfortunately there are some ct_run.py suites not run using onenv :(
set_privileges(SpaceId, UserId, SpacePrivs) when is_binary(SpaceId), is_binary(UserId) ->
    ozw_test_rpc:space_set_user_privileges(SpaceId, UserId, SpacePrivs),
    opt:force_fetch_entity(od_space, SpaceId);

set_privileges(SpaceSelector, UserSelector, SpacePrivs) ->
    set_privileges(
        oct_background:get_space_id(SpaceSelector),
        oct_background:get_user_id(UserSelector),
        SpacePrivs
    ).
