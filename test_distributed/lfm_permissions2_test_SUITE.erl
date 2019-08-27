%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of posix and acl 
%%% permissions with corresponding lfm (logical_file_manager) functions
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_permissions2_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_file_test/1
]).

all() ->
    ?ALL([
        create_file_test
    ]).


-define(ALL_PERMS, [
    ?read_object,
    ?list_container,
    ?write_object,
    ?add_object,
    ?append_data,
    ?add_subcontainer,
    ?read_metadata,
    ?write_metadata,
    ?execute,
    ?traverse_container,
    ?delete_object,
    ?delete_subcontainer,
    ?read_attributes,
    ?write_attributes,
    ?delete,
    ?read_acl,
    ?write_acl,
    ?write_owner
]).


-define(ALLOW_ACE(__IDENTIFIER, __FLAGS, __MASK), #access_control_entity{
    acetype = ?allow_mask,
    identifier = __IDENTIFIER,
    aceflags = __FLAGS,
    acemask = __MASK
}).


-define(DENY_ACE(__IDENTIFIER, __FLAGS, __MASK), #access_control_entity{
    acetype = ?deny_mask,
    aceflags = __FLAGS,
    identifier = __IDENTIFIER,
    acemask = __MASK
}).


-record(test_spec, {
    space = <<"space1">> :: binary(),
    root_dir :: binary(),
    owner = <<"user1">> :: binary(),
    user = <<"user2">> :: binary(),
    user_group = <<"group2">> :: binary(),
    env :: map(),
    fn :: fun((UserId :: binary(), Path :: binary()) ->
        ok |
        {ok, term()} |
        {ok, term(), term()} |
        {ok, term(), term(), term()} |
        {error, term()}
    )
}).


%%%===================================================================
%%% Test functions
%%%===================================================================


create_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"dir1">> => #{privs => [?traverse_container, ?add_object]}
        },
        fn = fun(SessId, Path) ->
            lfm_proxy:create(W, SessId, <<Path/binary, "/dir1/t12_file">>, 8#777)
        end
    }, Config).


%%%===================================================================
%%% TEST MECHANISM
%%%===================================================================


run_tests(Node, #test_spec{
    space = Space,
    owner = Owner,
    root_dir = RootDir
} = Spec, Config) ->
    OwnerSessId = ?config({session_id, {Owner, ?GET_DOMAIN(Node)}}, Config),
    RootDirPath = <<"/", Space/binary, "/", RootDir/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, OwnerSessId, RootDirPath, 8#777)),

    run_acl_tests(Node, RootDirPath, Spec, Config),
    ok.


%%%===================================================================
%%% POSIX TESTS MECHANISM
%%%===================================================================


%%%===================================================================
%%% ACL TESTS MECHANISM
%%%===================================================================


run_acl_tests(Node, RootDirPath, #test_spec{
    owner = Owner,
    user = User,
    user_group = UserGroup,
    env = EnvDesc,
    fn = Fun
}, Config) ->
    OwnerSessId = ?config({session_id, {Owner, ?GET_DOMAIN(Node)}}, Config),
    UserSessId = ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config),

    % TEST OWNER ALLOW ACL
    OwnerAclAllowRootDir = <<RootDirPath/binary, "/owner_acl_allow">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, OwnerSessId, OwnerAclAllowRootDir)),
    run_acl_tests(
        Node, OwnerSessId, OwnerAclAllowRootDir, Fun, allow,
        setup_env(Node, OwnerSessId, OwnerAclAllowRootDir, EnvDesc),
        ?owner, ?no_flags_mask
    ),

    % TEST USER ALLOW ACL
    UserAclAllowRootDir = <<RootDirPath/binary, "/user_acl_allow">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, OwnerSessId, UserAclAllowRootDir)),
    run_acl_tests(
        Node, UserSessId, UserAclAllowRootDir, Fun, allow,
        setup_env(Node, OwnerSessId, UserAclAllowRootDir, EnvDesc),
        User, ?no_flags_mask
    ),

    % TEST USER GROUP ALLOW ACL
    UserGroupAclAllowRootDir = <<RootDirPath/binary, "/user_group_acl_allow">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, OwnerSessId, UserGroupAclAllowRootDir)),
    run_acl_tests(
        Node, UserSessId, UserGroupAclAllowRootDir, Fun, allow,
        setup_env(Node, OwnerSessId, UserGroupAclAllowRootDir, EnvDesc),
        UserGroup, ?identifier_group_mask
    ),

    % TEST EVERYONE ALLOW ACL
    EveryoneAclAllowRootDir = <<RootDirPath/binary, "/everyone_acl_allow">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, OwnerSessId, EveryoneAclAllowRootDir)),
    run_acl_tests(
        Node, UserSessId, EveryoneAclAllowRootDir, Fun, allow,
        setup_env(Node, OwnerSessId, EveryoneAclAllowRootDir, EnvDesc),
        ?everyone, ?no_flags_mask
    ),

    % TEST OWNER DENY ACL
    OwnerAclDenyRootDir = <<RootDirPath/binary, "/owner_acl_deny">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, OwnerSessId, OwnerAclDenyRootDir)),
    run_acl_tests(
        Node, OwnerSessId, OwnerAclDenyRootDir, Fun, deny,
        setup_env(Node, OwnerSessId, OwnerAclDenyRootDir, EnvDesc),
        ?owner, ?no_flags_mask
    ),

    % TEST USER DENY ACL
    UserAclDenyRootDir = <<RootDirPath/binary, "/user_acl_deny">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, OwnerSessId, UserAclDenyRootDir)),
    run_acl_tests(
        Node, UserSessId, UserAclDenyRootDir, Fun, deny,
        setup_env(Node, OwnerSessId, UserAclDenyRootDir, EnvDesc),
        User, ?no_flags_mask
    ),

    % TEST USER GROUP DENY ACL
    UserGroupAclDenyRootDir = <<RootDirPath/binary, "/user_group_acl_deny">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, OwnerSessId, UserGroupAclDenyRootDir)),
    run_acl_tests(
        Node, UserSessId, UserGroupAclDenyRootDir, Fun, deny,
        setup_env(Node, OwnerSessId, UserGroupAclDenyRootDir, EnvDesc),
        UserGroup, ?identifier_group_mask
    ),

    % TEST EVERYONE DENY ACL
    EveryoneAclDenyRootDir = <<RootDirPath/binary, "/everyone_acl_deny">>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, OwnerSessId, EveryoneAclDenyRootDir)),
    run_acl_tests(
        Node, UserSessId, EveryoneAclDenyRootDir, Fun, deny,
        setup_env(Node, OwnerSessId, EveryoneAclDenyRootDir, EnvDesc),
        ?everyone, ?no_flags_mask
    ).


run_acl_tests(Node, SessId, RootPath, Fun, Type, RequiredPermsPerFile, AceWho, AceFlags) ->
    {ComplementaryPermsPerFile, AllRequiredPerms} = lists:foldl(
        fun({FileGuid, FileRequiredPerms}, {BasePermsPerFileAcc, RequiredPermsAcc}) ->
            {
                BasePermsPerFileAcc#{FileGuid => complementary_perms(FileRequiredPerms)},
                    [{FileGuid, Perm} || Perm <- FileRequiredPerms] ++ RequiredPermsAcc
            }
        end,
        {#{}, []},
        RequiredPermsPerFile
    ),

    try
        run_acl_tests(
            Node, SessId, RootPath, Fun, ComplementaryPermsPerFile,
            AllRequiredPerms, AceWho, AceFlags, Type
        )
    catch T:R ->
        RequiredPermsPerFileMap = lists:foldl(fun({Guid, RequiredPerms}, Acc) ->
            {ok, Path} = lfm_proxy:get_file_path(Node, SessId, Guid),
            Acc#{Path => RequiredPerms}
        end, #{}, RequiredPermsPerFile),

        ct:pal(
            "ACL TESTS FAILURE~n"
            "   Type: ~p~n"
            "   Root path: ~p~n"
            "   Required Perms: ~p~n"
            "   Identifier: ~p~n"
            "   Is group identifier: ~p~n",
            [
                Type, RootPath,
                RequiredPermsPerFileMap,
                AceWho, AceFlags == ?identifier_group_mask
            ]
        ),
        erlang:T(R)
    end.


run_acl_tests(Node, SessId, RootPath, Fun, ComplementaryPermsPerFile, AllRequiredPerms, AceWho, AceFlags, allow) ->
    [AllRequiredPermsComb | EaccesPermsCombs] = combinations(AllRequiredPerms),

    % Granting all perms without required ones should result in eacces
    lists:foreach(fun(EaccessPermComb) ->
        EaccesPermsPerFile = lists:foldl(fun({Guid, Perm}, Acc) ->
            Acc#{Guid => [Perm | maps:get(Guid, Acc)]}
        end, ComplementaryPermsPerFile, EaccessPermComb),
        set_acls(Node, EaccesPermsPerFile, #{}, AceWho, AceFlags),
        ?assertMatch({error, ?EACCES}, Fun(SessId, RootPath))
    end, EaccesPermsCombs),

    % Granting only required perms should result in success
    RequiredPermsPerFile = lists:foldl(fun({Guid, Perm}, Acc) ->
        Acc#{Guid => [Perm | maps:get(Guid, Acc, [])]}
    end, #{}, AllRequiredPermsComb),
    set_acls(Node, RequiredPermsPerFile, #{}, AceWho, AceFlags),
    ?assertNotMatch({error, _}, Fun(SessId, RootPath));

run_acl_tests(Node, SessId, RootPath, Fun, ComplementaryPermsPerFile, AllRequiredPerms, AceWho, AceFlags, deny) ->
    AllPermsPerFile = maps:map(fun(_, _) -> ?ALL_PERMS end, ComplementaryPermsPerFile),

    % Denying only required perms and granting all others should result in eacces
    lists:foreach(fun({Guid, Perm}) ->
        set_acls(Node, AllPermsPerFile, #{Guid => [Perm]}, AceWho, AceFlags),
        ?assertMatch({error, ?EACCES}, Fun(SessId, RootPath))
    end, AllRequiredPerms),

    % Denying all perms but required ones should result in success
    set_acls(Node, #{}, ComplementaryPermsPerFile, AceWho, AceFlags),
    ?assertNotMatch({error, _}, Fun(SessId, RootPath)).


set_acls(Node, AllowedPermsPerFile, DeniedPermsPerFile, AceWho, AceFlags) ->
    maps:fold(fun(Guid, Perms, _) ->
        ?assertEqual(ok, lfm_proxy:set_acl(
            Node, ?ROOT_SESS_ID, {guid, Guid},
            [?ALLOW_ACE(AceWho, AceFlags, acl:mask_to_bitmask(Perms))])
        )
    end, ok, maps:without(maps:keys(DeniedPermsPerFile), AllowedPermsPerFile)),

    maps:fold(fun(Guid, Perms, _) ->
        ?assertEqual(ok, lfm_proxy:set_acl(
            Node, ?ROOT_SESS_ID, {guid, Guid},
            [
                ?DENY_ACE(AceWho, AceFlags, acl:mask_to_bitmask(Perms)),
                ?ALLOW_ACE(AceWho, AceFlags, acl:mask_to_bitmask(?ALL_PERMS))
            ]
        ))
    end, ok, DeniedPermsPerFile).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    initializer:teardown_storage(Config).


init_per_testcase(_Case, Config) ->
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec setup_env(node(), session:id(), file_meta:path(), DirDesc :: map()) ->
    [{file_id:file_guid(), [Perms :: binary()]}].
setup_env(Node, SessId, ParentDirPath, ParentDirDesc) ->
    maps:fold(
        fun
            (<<"file", _/binary>> = FileName, FilePerms, Acc) ->
                FilePath = <<ParentDirPath/binary, "/", FileName/binary>>,
                {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath, 8#777)),
                [{FileGuid, FilePerms} | Acc];
            (<<"dir", _/binary>> = DirName, DirDescWithPerms, Acc) ->
                DirPath = <<ParentDirPath/binary, "/", DirName/binary>>,
                {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, DirPath)),
                DirEnv = case maps:take(privs, DirDescWithPerms) of
                    error ->
                        setup_env(Node, SessId, DirPath, DirDescWithPerms);
                    {DirPerms, DirDesc} ->
                        [{DirGuid, DirPerms} | setup_env(Node, SessId, DirPath, DirDesc)]
                end,
                Acc ++ DirEnv
        end,
        [],
        ParentDirDesc
    ).


-spec complementary_perms(Perms :: [binary()]) -> ComplementaryPerms :: [binary()].
complementary_perms(Perms) ->
    ?ALL_PERMS -- lists:usort(lists:flatmap(
        fun
            (?read_object) -> [?read_object, ?list_container];
            (?list_container) -> [?read_object, ?list_container];
            (?write_object) -> [?write_object, ?add_object];
            (?add_object) -> [?write_object, ?add_object];
            (?append_data) -> [?append_data, ?add_subcontainer];
            (?add_subcontainer) -> [?append_data, ?add_subcontainer];
            (?read_metadata) -> [?read_metadata];
            (?write_metadata) -> [?write_metadata];
            (?execute) -> [?execute, ?traverse_container];
            (?traverse_container) -> [?execute, ?traverse_container];
            (?delete_object) -> [?delete_object, ?delete_subcontainer];
            (?delete_subcontainer) -> [?delete_object, ?delete_subcontainer];
            (?read_attributes) -> [?read_attributes];
            (?write_attributes) -> [?write_attributes];
            (?delete) -> [?delete];
            (?read_acl) -> [?read_acl];
            (?write_acl) -> [?write_acl];
            (?write_owner) -> [?write_owner]
        end,
        Perms
    )).


-spec combinations([term()]) -> [[term()]].
combinations([]) ->
    [[]];
combinations([Item | Items]) ->
    Combinations = combinations(Items),
    [[Item | Comb] || Comb <- Combinations] ++ Combinations.
