%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of fslogic_objects.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(fslogic_objects_tests).
-author("Rafal Slota").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").

setup() ->
    meck:new([dao_lib, user_logic]).

teardown(_) ->
    ok = meck:unload([dao_lib]).

getters_test_() ->
    {foreach, fun setup/0, fun teardown/1,
        [fun get_storage/0, get_user/0]}.

get_storage() ->
    meck:expect(dao_lib, apply,
        fun (dao_vfs, get_storage, [{id, StorageID}], _) ->
                {ok, #veil_document{record = #storage_info{id = StorageID}, uuid = "uuid"}};
            (dao_vfs, get_storage, [{uuid, StorageUUID}], _) ->
                {ok, #veil_document{record = #storage_info{id = 2}, uuid = StorageUUID}};
            (dao_vfs, get_storage, [{uuid, "non-ex"}], _) ->
                {error, reason}
        end),

    ?assertMatch({ok, #veil_document{uuid = "123"}}, fslogic_objects:get_storage({uuid, "123"})),
    ?assertMatch({ok, #veil_document{uuid = "uuid"}}, fslogic_objects:get_storage({id, 1})),
    ?assertMatch({error, {failed_to_get_storage, {reason, {storage, uuid, "non-ex"}}}},
        fslogic_objects:get_storage({uuid, "non-ex"})),

    ?assert(meck:validate(dao_lib)).

get_user() ->
    meck:expect(user_logic, get_user,
        fun ({dn, "dn"}) ->
                {ok, #veil_document{uuid = "uuid"}};
            (_) ->
                {error, reason}
        end),
    fslogic_context:set_user_dn("dn"),

    ?assertMatch({ok, #veil_document{uuid = "uuid"}}, fslogic_objects:get_user({dn, "dn"})),
    ?assertMatch({error, {get_user_error, {reason, {dn, "invalid"}}}}, fslogic_objects:get_user({dn, "invalid"})),
    ?assertMatch({ok, #veil_document{uuid = ?CLUSTER_USER_ID}}, fslogic_objects:get_user({dn, undefined})),

    ?assert(meck:validate(user_logic)).


update_file_descriptor_test() ->
    Time = vcn_utils:time(),
    Desc = fslogic_objects:update_file_descriptor(#file_descriptor{}, 123),
    ?assert(Time =< Desc#file_descriptor.create_time),
    ?assertEqual(123, Desc#file_descriptor.validity_time).

-endif.