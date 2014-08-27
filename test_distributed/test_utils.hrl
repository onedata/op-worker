%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains ct tests helper macros and defines.
%% @end
%% ===================================================================

-ifndef(TEST_UTILS_HRL).
-define(TEST_UTILS_HRL, 1).

-define(TEST, true).

-include("veil_modules/dao/dao_spaces.hrl").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Macro for cleaning test dirs before tests
-define(CLEAN_TEST_DIRS,begin
                            os:cmd("rm -rf /tmp/veilfs/*"),
                            os:cmd("rm -rf /tmp/veilfs2/*"),
                            os:cmd("rm -rf /tmp/veilfs3/*"),
                            os:cmd("./clear_test_db.sh")
                        end).

%% Veilcluster dependencies
-define(VEIL_DEPS, [sasl,lager,ssl,cowlib,ranch,cowboy,ibrowse]).

%% Returns absolute path to given file using virtual CWD which equals to current SUITE directory
-define(TEST_FILE(X), filename:join(ets:match(suite_state, {test_root, '$1'}) ++ [X])).

%% Returns absolute path to given file using virtual CWD which equals to ct_root/common_files
-define(COMMON_FILE(X), filename:join(ets:match(suite_state, {ct_root, '$1'}) ++ ["common_files"] ++ [X])).

%% Roots of test filesystem
-define(TEST_ROOT, "/tmp/veilfs/").
-define(TEST_ROOT2, "/tmp/veilfs2/").
-define(TEST_ROOT3, "/tmp/veilfs3/").

-define(ARG_TEST_ROOT, [?TEST_ROOT]).
-define(ARG_TEST_ROOT2, [?TEST_ROOT2]).
-define(ARG_TEST_ROOT3, [?TEST_ROOT3]).

% Test users and groups
-define(TEST_USER, "veilfstestuser").
-define(TEST_USER2, "veilfstestuser2").
-define(TEST_GROUP, "veilfstestgroup").
-define(TEST_GROUP2, "veilfstestgroup2").
-define(TEST_GROUP3, "veilfstestgroup3").
-define(TEST_GROUP_EXTENDED, "veilfstestgroup(Grp)").
-define(TEST_GROUP2_EXTENDED, "veilfstestgroup2(Grp2)").
-define(TEST_GROUP3_EXTENDED, "veilfstestgroup3(Grp3)").

-define(LOCAL_PROVIDER_ID, <<"providerId">>).
-define(ENABLE_PROVIDER(Config), ?ENABLE_PROVIDER(Config, ?LOCAL_PROVIDER_ID)).
-define(ENABLE_PROVIDER(Config, ProviderId), test_utils:ct_mock(Config, cluster_manager_lib, get_provider_id, fun() -> ProviderId end)).

-define(ADD_USER(Login, Cert, Spaces), ?ADD_USER(Login, Cert, Spaces, <<"access_token">>)).
-define(ADD_USER(Login, Cert, Spaces, AccessToken),
    begin
        SpacesBinary = [vcn_utils:ensure_binary(Space) || Space <- Spaces],
        SpacesList = [vcn_utils:ensure_list(Space) || Space <- Spaces],

        {ReadFileAns, PemBin} = file:read_file(Cert),
        ?assertEqual(ok, ReadFileAns),
        {ExtractAns, RDNSequence} = rpc:call(CCM, user_logic, extract_dn_from_cert, [PemBin]),
        ?assertEqual(rdnSequence, ExtractAns),
        {ConvertAns, DN} = rpc:call(CCM, user_logic, rdn_sequence_to_dn_string, [RDNSequence]),
        ?assertEqual(ok, ConvertAns),
        DnList = [DN],

        Name = Login ++ " " ++ Login,
        Teams = SpacesList,
        Email = Login ++ "@email.net",
        {CreateUserAns, NewUserDoc} = rpc:call(CCM, user_logic, create_user, ["global_id", Login, Name, Teams, Email, DnList, AccessToken]),
        ?assertMatch({ok, _}, {CreateUserAns, NewUserDoc}),


        test_utils:ct_mock(Config, gr_users, get_spaces, fun(_) -> {ok, #user_spaces{ids = SpacesBinary, default = lists:nth(1, SpacesBinary)}} end),
        test_utils:ct_mock(Config, gr_adapter, get_space_info, fun(SpaceId, _) -> {ok, #space_info{space_id = SpaceId, name = SpaceId, providers = [?LOCAL_PROVIDER_ID]}} end),

        _UserDoc = rpc:call(CCM, user_logic, synchronize_spaces_info, [NewUserDoc, AccessToken])
    end).

-endif.