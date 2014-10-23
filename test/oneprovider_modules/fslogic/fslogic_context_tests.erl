%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of fslogic_context.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(fslogic_context_tests).
-author("Rafal Slota").

-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/dao/dao.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

user_dn_test() ->
    fslogic_context:clear_user_dn(),
    ?assertMatch(undefined, fslogic_context:get_user_dn()),
    ?assertMatch(undefined, fslogic_context:set_user_dn("abc")),
    ?assertMatch("abc", fslogic_context:get_user_dn()),
    fslogic_context:clear_user_dn(),
    ?assertMatch(undefined, fslogic_context:get_user_dn()).

fuse_id_test() ->
    ?assertMatch(undefined, fslogic_context:get_fuse_id()),
    ?assertMatch(undefined, fslogic_context:set_fuse_id("abc")),
    ?assertMatch("abc", fslogic_context:get_fuse_id()).

protocol_version_test() ->
    ?assertMatch(undefined, fslogic_context:get_protocol_version()),
    ?assertMatch(undefined, fslogic_context:set_protocol_version("abc")),
    ?assertMatch("abc", fslogic_context:get_protocol_version()).

user_id_test() ->
    fslogic_context:clear_user_ctx(),

    ?assertMatch({ok, ?CLUSTER_USER_ID}, fslogic_context:get_user_id()).


-endif.
