%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of fslogic_file.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(fslogic_file_tests).
-author("Rafal Slota").

-include("veil_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("veil_modules/dao/dao.hrl").

setup() ->
    meck:new([user_logic]).

teardown(_) ->
    meck:unload().


normalize_file_type_test() ->
    ?assertEqual(?DIR_TYPE_PROT, fslogic_file:normalize_file_type(protocol, ?DIR_TYPE)),
    ?assertEqual(?DIR_TYPE_PROT, fslogic_file:normalize_file_type(protocol, ?DIR_TYPE_PROT)),
    ?assertEqual(?REG_TYPE_PROT, fslogic_file:normalize_file_type(protocol, ?REG_TYPE)),
    ?assertEqual(?REG_TYPE_PROT, fslogic_file:normalize_file_type(protocol, ?REG_TYPE_PROT)),
    ?assertEqual(?LNK_TYPE_PROT, fslogic_file:normalize_file_type(protocol, ?LNK_TYPE)),
    ?assertEqual(?LNK_TYPE_PROT, fslogic_file:normalize_file_type(protocol, ?LNK_TYPE_PROT)),

    ?assertEqual(?DIR_TYPE, fslogic_file:normalize_file_type(internal, ?DIR_TYPE)),
    ?assertEqual(?DIR_TYPE, fslogic_file:normalize_file_type(internal, ?DIR_TYPE_PROT)),
    ?assertEqual(?REG_TYPE, fslogic_file:normalize_file_type(internal, ?REG_TYPE)),
    ?assertEqual(?REG_TYPE, fslogic_file:normalize_file_type(internal, ?REG_TYPE_PROT)),
    ?assertEqual(?LNK_TYPE, fslogic_file:normalize_file_type(internal, ?LNK_TYPE)),
    ?assertEqual(?LNK_TYPE, fslogic_file:normalize_file_type(internal, ?LNK_TYPE_PROT)).


get_file_local_location_test() ->
    LocationField = #file_location{file_id = "123"},
    File = #file{location = LocationField},
    Doc = #veil_document{record = File},
    ?assertMatch(#file_location{file_id = "123"}, fslogic_file:get_file_local_location(Doc)),
    ?assertMatch(#file_location{file_id = "123"}, fslogic_file:get_file_local_location(File)),
    ?assertMatch(#file_location{file_id = "123"}, fslogic_file:get_file_local_location(LocationField)).


get_real_file_size_test() ->
    %% This call shall be logic-less for non-regular files
    ?assertEqual(0, fslogic_file:get_real_file_size(#file{type = ?DIR_TYPE})),
    ?assertEqual(0, fslogic_file:get_real_file_size(#file{type = ?LNK_TYPE})).


get_file_owner_test_() ->
    {foreach, fun setup/0, fun teardown/1, [fun get_file_owner/0]}.

get_file_owner() ->
    meck:expect(user_logic, get_login,
        fun (#veil_document{record = #user{login = Login}}) ->
            Login
        end),
    meck:expect(user_logic, get_user,
        fun ({uuid, "123"}) ->
                {ok, #veil_document{uuid = "123", record = #user{login = "login"}}};
            ({uuid, _}) ->
                {error, reason}
        end),

    ?assertMatch({"login", 123}, fslogic_file:get_file_owner(#file{uid = "123"})),
    ?assertMatch({"", -1}, fslogic_file:get_file_owner(#file{uid = "321"})),

    ?assert(meck:validate(user_logic)).

-endif.