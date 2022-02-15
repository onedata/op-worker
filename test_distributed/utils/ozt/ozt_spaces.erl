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
set_privileges(SpaceId, UserId, SpacePrivs) ->
    ozw_test_rpc:space_set_user_privileges(SpaceId, UserId, SpacePrivs),
    opt:force_fetch_entity(od_space, SpaceId).
