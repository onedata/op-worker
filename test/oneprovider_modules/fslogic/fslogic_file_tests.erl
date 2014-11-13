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

-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("files_common.hrl").
-include("registered_names.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("oneprovider_modules/dao/dao.hrl").

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


get_real_file_size_test() ->
    %% This call shall be logic-less for non-regular files
    ?assertEqual({0, -1}, fslogic_file:get_real_file_size_and_uid(#db_document{record = #file{type = ?DIR_TYPE}})),
    ?assertEqual({0, -1}, fslogic_file:get_real_file_size_and_uid(#db_document{record = #file{type = ?LNK_TYPE}})).


get_file_owner_test_() ->
    {foreach, fun setup/0, fun teardown/1, [fun get_file_owner/0]}.

get_file_owner() ->
    application:set_env(?APP_Name ,lowest_generated_storage_gid, 70000),
    meck:expect(user_logic, get_user, fun
        ({uuid, "123"}) ->
            {ok, #db_document{uuid = "123", record = #user{}}};
        ({uuid, _}) ->
            {error, reason}
    end),
    meck:expect(user_logic, get_login_with_uid, fun(#db_document{uuid = "123", record = #user{}}) ->
        {{provider, "login"}, 123}
    end),

    %?assertMatch({"login", 123, 123}, fslogic_file:get_file_owner(#file{uid = "123"})), %todo repair
    ?assertMatch({"", -1, -1}, fslogic_file:get_file_owner(#file{uid = "321"})),

    ?assert(meck:validate(user_logic)).

-endif.
