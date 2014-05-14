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

-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

user_dn_test() ->
    ?assertMatch(undefined, fslogic_context:get_user_dn()),
    ?assertMatch(undefined, fslogic_context:set_user_dn("abc")),
    ?assertMatch("abc", fslogic_context:get_user_dn()).

fuse_id_test() ->
    ?assertMatch(undefined, fslogic_context:get_fuse_id()),
    ?assertMatch(undefined, fslogic_context:set_fuse_id("abc")),
    ?assertMatch("abc", fslogic_context:get_fuse_id()).

protocol_version_test() ->
    ?assertMatch(undefined, fslogic_context:get_protocol_version()),
    ?assertMatch(undefined, fslogic_context:set_protocol_version("abc")),
    ?assertMatch("abc", fslogic_context:get_protocol_version()).

user_id_test() ->
    ?assertMatch({ok, ?CLUSTER_USER_ID}, fslogic_context:get_user_id()),

    fslogic_context:set_user_dn("dn"),
    meck:new(fslogic_objects),
    meck:expect(fslogic_objects, get_user,
        fun({dn, "dn"}) ->
            {ok, #veil_document{record = #user{}, uuid = "uuid"}}
        end),

    ?assertMatch({ok, "uuid"}, fslogic_context:get_user_id()),
    ?assert(meck:validate(fslogic_objects)),
    meck:unload().


-endif.