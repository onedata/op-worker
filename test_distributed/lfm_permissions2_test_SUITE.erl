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
    mkdir_test/1,
    ls_test/1,
    readdir_plus_test/1,
    get_child_attr_test/1,
    create_file_test/1,
    get_transfer_encoding_test/1,
    set_transfer_encoding_test/1,
    get_cdmi_completion_status_test/1,
    set_cdmi_completion_status_test/1,
    get_mimetype_test/1,
    set_mimetype_test/1,
    get_metadata_test/1,
    set_metadata_test/1,
    remove_metadata_test/1
]).

all() ->
    ?ALL([
        mkdir_test,
        ls_test,
        readdir_plus_test,
        get_child_attr_test,
        create_file_test,
        get_transfer_encoding_test,
        set_transfer_encoding_test,
        get_cdmi_completion_status_test,
        set_cdmi_completion_status_test,
        get_mimetype_test,
        set_mimetype_test,
        get_metadata_test,
        set_metadata_test,
        remove_metadata_test
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
-define(ALL_POSIX_PERMS, [read, write, exec]).


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
    user_outside_space = <<"user3">>,
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


mkdir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"dir1">> => #{privs => [?traverse_container, ?add_subcontainer]}
        },
        fn = fun(_, SessId, Path) ->
            lfm_proxy:mkdir(W, SessId, <<Path/binary, "/dir1/dir2">>)
        end
    }, Config).


ls_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"dir1">> => #{privs => [?list_container]}
        },
        fn = fun(_, SessId, Path) ->
            lfm_proxy:ls(W, SessId, {path, <<Path/binary, "/dir1/">>}, 0, 100)
        end
    }, Config).


readdir_plus_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"dir1">> => #{privs => [?traverse_container, ?list_container]}
        },
        fn = fun(_, SessId, Path) ->
            lfm_proxy:read_dir_plus(W, SessId, {path, <<Path/binary, "/dir1/">>}, 0, 100)
        end
    }, Config).


get_child_attr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"dir1">> => #{
                privs => [?traverse_container],
                <<"file1">> => #{}
            }
        },
        fn = fun(OwnerSessId, SessId, Path) ->
            {ok, ParentGuid} = lfm_proxy:resolve_guid(W, OwnerSessId, <<Path/binary, "/dir1/">>),
            lfm_proxy:get_child_attr(W, SessId, ParentGuid, <<"file1">>)
        end
    }, Config).


create_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"dir1">> => #{privs => [?traverse_container, ?add_object]}
        },
        fn = fun(_, SessId, Path) ->
            lfm_proxy:create(W, SessId, <<Path/binary, "/dir1/file1">>, 8#777)
        end
    }, Config).


get_transfer_encoding_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"file1">> => #{
                privs => [?read_attributes],
                hook => fun(OwnerSessId, Guid) ->
                    lfm_proxy:set_transfer_encoding(W, OwnerSessId, {guid, Guid}, <<"base64">>)
                end
            }
        },
        fn = fun(_, SessId, Path) ->
            lfm_proxy:get_transfer_encoding(W, SessId, {path, <<Path/binary, "/file1">>})
        end
    }, Config).


set_transfer_encoding_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"file1">> => #{privs => [?write_attributes]}
        },
        fn = fun(_, SessId, Path) ->
            lfm_proxy:set_transfer_encoding(W, SessId, {path, <<Path/binary, "/file1">>}, <<"base64">>)
        end
    }, Config).


get_cdmi_completion_status_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"file1">> => #{
                privs => [?read_attributes],
                hook => fun(OwnerSessId, Guid) ->
                    lfm_proxy:set_cdmi_completion_status(W, OwnerSessId, {guid, Guid}, <<"Completed">>)
                end
            }
        },
        fn = fun(_, SessId, Path) ->
            lfm_proxy:get_cdmi_completion_status(W, SessId, {path, <<Path/binary, "/file1">>})
        end
    }, Config).


set_cdmi_completion_status_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"file1">> => #{privs => [?write_attributes]}
        },
        fn = fun(_, SessId, Path) ->
            lfm_proxy:set_cdmi_completion_status(W, SessId, {path, <<Path/binary, "/file1">>}, <<"Completed">>)
        end
    }, Config).


get_mimetype_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"file1">> => #{
                privs => [?read_attributes],
                hook => fun(OwnerSessId, Guid) ->
                    lfm_proxy:set_mimetype(W, OwnerSessId, {guid, Guid}, <<"mimetype">>)
                end
            }
        },
        fn = fun(_, SessId, Path) ->
            lfm_proxy:get_mimetype(W, SessId, {path, <<Path/binary, "/file1">>})
        end
    }, Config).


set_mimetype_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"file1">> => #{privs => [?write_attributes]}
        },
        fn = fun(_, SessId, Path) ->
            lfm_proxy:set_mimetype(W, SessId, {path, <<Path/binary, "/file1">>}, <<"mimetype">>)
        end
    }, Config).


get_metadata_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"file1">> => #{
                privs => [?read_attributes],
                hook => fun(OwnerSessId, Guid) ->
                    lfm_proxy:set_metadata(W, OwnerSessId, {guid, Guid}, json, <<"VAL">>, [])
                end
            }
        },
        fn = fun(_, SessId, Path) ->
            lfm_proxy:get_metadata(W, SessId, {path, <<Path/binary, "/file1">>}, json, [], false)
        end
    }, Config).


set_metadata_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"file1">> => #{privs => [?write_attributes]}
        },
        fn = fun(_, SessId, Path) ->
            lfm_proxy:set_metadata(W, SessId, {path, <<Path/binary, "/file1">>}, json, <<"VAL">>, [])
        end
    }, Config).


remove_metadata_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    run_tests(W, #test_spec{
        root_dir = atom_to_binary(?FUNCTION_NAME, utf8),
        env = #{
            <<"file1">> => #{privs => [?write_attributes]}
        },
        fn = fun(_, SessId, Path) ->
            lfm_proxy:remove_metadata(W, SessId, {path, <<Path/binary, "/file1">>}, json)
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

    run_posix_tests(Node, RootDirPath, Spec, Config),
    run_acl_tests(Node, RootDirPath, Spec, Config).


%%%===================================================================
%%% POSIX TESTS MECHANISM
%%%===================================================================


run_posix_tests(Node, RootDirPath, #test_spec{
    owner = Owner,
    user = User,
    user_outside_space = OtherUser,
    env = EnvDesc,
    fn = Fun
}, Config) ->
    OwnerSessId = ?config({session_id, {Owner, ?GET_DOMAIN(Node)}}, Config),
    UserSessId = ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config),
    OtherUserSessId = ?config({session_id, {OtherUser, ?GET_DOMAIN(Node)}}, Config),

    lists:foreach(fun({SessId, Type, DirName}) ->
        DirPath = <<RootDirPath/binary, DirName/binary>>,
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, OwnerSessId, DirPath)),
        run_posix_tests(
            Node, OwnerSessId, SessId, DirPath, Fun, Type,
            setup_env(Node, OwnerSessId, DirPath, EnvDesc)
        )
    end, [
        {OwnerSessId, owner, <<"/owner_posix">>},
        {UserSessId, group, <<"/group_posix">>},
        {OtherUserSessId, other, <<"/other_posix">>}
    ]).


run_posix_tests(Node, OwnerSessId, SessId, RootPath, Fun, Type, RequiredPermsPerFile) ->
    RequiredPosixPermsPerFile = lists:map(fun({Guid, Perms}) ->
        {Guid, lists:usort(lists:flatmap(fun perm_to_posix_perms/1, Perms))}
    end, RequiredPermsPerFile),

    {ComplementaryPosixPermsPerFile, AllRequiredPosixPerms} = lists:foldl(
        fun({FileGuid, FileRequiredPerms}, {BasePermsPerFileAcc, RequiredPermsAcc}) ->
            {
                BasePermsPerFileAcc#{FileGuid => ?ALL_POSIX_PERMS -- FileRequiredPerms},
                [{FileGuid, Perm} || Perm <- FileRequiredPerms] ++ RequiredPermsAcc
            }
        end,
        {#{}, []},
        RequiredPosixPermsPerFile
    ),

    try
        run_posix_tests(
            Node, OwnerSessId, SessId, RootPath, Fun, ComplementaryPosixPermsPerFile,
            AllRequiredPosixPerms, Type
        )
    catch T:R ->
        RequiredPermsPerFileMap = lists:foldl(fun({Guid, RequiredPerms}, Acc) ->
            lfm_proxy:set_perms(Node, ?ROOT_SESS_ID, {guid, Guid}, 8#777),
            {ok, Path} = lfm_proxy:get_file_path(Node, OwnerSessId, Guid),
            Acc#{Path => RequiredPerms}
        end, #{}, RequiredPosixPermsPerFile),

        ct:pal(
            "POSIX TESTS FAILURE~n"
            "   Type: ~p~n"
            "   Root path: ~p~n"
            "   Required Perms: ~p~n"
            "   Stacktrace: ~p~n",
            [
                Type, RootPath,
                RequiredPermsPerFileMap,
                erlang:get_stacktrace()
            ]
        ),
        erlang:T(R)
    end.


run_posix_tests(Node, OwnerSessId, SessId, RootPath, Fun, ComplementaryPermsPerFile, AllRequiredPerms, owner) ->
    run_posix_tests_combinations(Node, OwnerSessId, SessId, RootPath, Fun, ComplementaryPermsPerFile, AllRequiredPerms, owner);

run_posix_tests(Node, OwnerSessId, SessId, RootPath, Fun, ComplementaryPermsPerFile, AllRequiredPerms, group) ->
    OperationRequiresOwner = lists:any(fun({_, Perm}) ->
        Perm =/= read andalso Perm =/= write andalso Perm =/= exec
    end, AllRequiredPerms),

    case OperationRequiresOwner of
        true ->
            AllModesPerFile = maps:map(fun(_, _) -> 8#777 end, ComplementaryPermsPerFile),
            set_modes(Node, AllModesPerFile),
            ?assertMatch({error, ?EACCES}, Fun(OwnerSessId, SessId, RootPath));
        false ->
            run_posix_tests_combinations(Node, OwnerSessId, SessId, RootPath, Fun, ComplementaryPermsPerFile, AllRequiredPerms, group)
    end;

% Users not belonging to space or unauthorized should be able to conduct any operation
run_posix_tests(Node, OwnerSessId, SessId, RootPath, Fun, ComplementaryPermsPerFile, _AllRequiredPerms, other) ->
    AllModesPerFile = maps:map(fun(_, _) -> 8#777 end, ComplementaryPermsPerFile),
    set_modes(Node, AllModesPerFile),
    ?assertMatch({error, _}, Fun(OwnerSessId, SessId, RootPath)),
    ?assertMatch({error, _}, Fun(OwnerSessId, ?GUEST_SESS_ID, RootPath)).


run_posix_tests_combinations(Node, OwnerSessId, SessId, RootPath, Fun, ComplementaryPermsPerFile, AllRequiredPerms, Type) ->
    AllRequiredModes = lists:map(fun({Guid, PosixPerm}) ->
        {Guid, posix_perm_to_mode(PosixPerm, Type)}
    end, AllRequiredPerms),
    ComplementaryModesPerFile = maps:map(fun(_, Perms) ->
        lists:foldl(fun(Perm, Acc) -> Acc bor posix_perm_to_mode(Perm, Type) end, 0, Perms)
    end, ComplementaryPermsPerFile),
    [AllRequiredModesComb | EaccesModesCombs] = combinations(AllRequiredModes),

    % Granting all modes without required ones should result in eacces
    lists:foreach(fun(EaccessModeComb) ->
        EaccesModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
            Acc#{Guid => Mode bor maps:get(Guid, Acc)}
        end, ComplementaryModesPerFile, EaccessModeComb),
        set_modes(Node, EaccesModesPerFile),
        ?assertMatch({error, ?EACCES}, Fun(OwnerSessId, SessId, RootPath))
    end, EaccesModesCombs),

    % Granting only required modes should result in success
    RequiredModesPerFile = lists:foldl(fun({Guid, Mode}, Acc) ->
        Acc#{Guid => Mode bor maps:get(Guid, Acc, 0)}
    end, #{}, AllRequiredModesComb),
    set_modes(Node, RequiredModesPerFile),
    ?assertNotMatch({error, _}, Fun(OwnerSessId, SessId, RootPath)).


set_modes(Node, ModePerFile) ->
    maps:fold(fun(Guid, Mode, _) ->
        ?assertEqual(ok, lfm_proxy:set_perms(
            Node, ?ROOT_SESS_ID, {guid, Guid}, Mode
        ))
    end, ok, ModePerFile).


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

    lists:foreach(fun({SessId, Type, DirName, AceWho, AceFlags}) ->
        DirPath = <<RootDirPath/binary, DirName/binary>>,
        ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, OwnerSessId, DirPath)),
        run_acl_tests(
            Node, OwnerSessId, SessId, DirPath, Fun, Type,
            setup_env(Node, OwnerSessId, DirPath, EnvDesc),
            AceWho, AceFlags
        )
    end, [
        {OwnerSessId, allow, <<"/owner_acl_allow">>, ?owner, ?no_flags_mask},
        {UserSessId, allow, <<"/user_acl_allow">>, User, ?no_flags_mask},
        {UserSessId, allow, <<"/user_group_acl_allow">>, UserGroup, ?identifier_group_mask},
        {UserSessId, allow, <<"/everyone_acl_allow">>, ?everyone, ?no_flags_mask},

        {OwnerSessId, deny, <<"/owner_acl_deny">>, ?owner, ?no_flags_mask},
        {UserSessId, deny, <<"/user_acl_deny">>, User, ?no_flags_mask},
        {UserSessId, deny, <<"/user_group_acl_deny">>, UserGroup, ?identifier_group_mask},
        {UserSessId, deny, <<"/everyone_acl_deny">>, ?everyone, ?no_flags_mask}
    ]).


run_acl_tests(Node, OwnerSessId, SessId, RootPath, Fun, Type, RequiredPermsPerFile, AceWho, AceFlags) ->
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
            Node, OwnerSessId, SessId, RootPath, Fun, ComplementaryPermsPerFile,
            AllRequiredPerms, AceWho, AceFlags, Type
        )
    catch T:R ->
        RequiredPermsPerFileMap = lists:foldl(fun({Guid, RequiredPerms}, Acc) ->
            lfm_proxy:set_perms(Node, ?ROOT_SESS_ID, {guid, Guid}, 8#777),
            {ok, Path} = lfm_proxy:get_file_path(Node, OwnerSessId, Guid),
            Acc#{Path => RequiredPerms}
        end, #{}, RequiredPermsPerFile),

        ct:pal(
            "ACL TESTS FAILURE~n"
            "   Type: ~p~n"
            "   Root path: ~p~n"
            "   Required Perms: ~p~n"
            "   Identifier: ~p~n"
            "   Is group identifier: ~p~n"
            "   Stacktrace: ~p~n",
            [
                Type, RootPath,
                RequiredPermsPerFileMap,
                AceWho, AceFlags == ?identifier_group_mask,
                erlang:get_stacktrace()
            ]
        ),
        erlang:T(R)
    end.


run_acl_tests(Node, OwnerSessId, SessId, RootPath, Fun, ComplementaryPermsPerFile, AllRequiredPerms, AceWho, AceFlags, allow) ->
    [AllRequiredPermsComb | EaccesPermsCombs] = combinations(AllRequiredPerms),

    % Granting all perms without required ones should result in eacces
    lists:foreach(fun(EaccessPermComb) ->
        EaccesPermsPerFile = lists:foldl(fun({Guid, Perm}, Acc) ->
            Acc#{Guid => [Perm | maps:get(Guid, Acc)]}
        end, ComplementaryPermsPerFile, EaccessPermComb),
        set_acls(Node, EaccesPermsPerFile, #{}, AceWho, AceFlags),
        ?assertMatch({error, ?EACCES}, Fun(OwnerSessId, SessId, RootPath))
    end, EaccesPermsCombs),

    % Granting only required perms should result in success
    RequiredPermsPerFile = lists:foldl(fun({Guid, Perm}, Acc) ->
        Acc#{Guid => [Perm | maps:get(Guid, Acc, [])]}
    end, #{}, AllRequiredPermsComb),
    set_acls(Node, RequiredPermsPerFile, #{}, AceWho, AceFlags),
    ?assertNotMatch({error, _}, Fun(OwnerSessId, SessId, RootPath));

run_acl_tests(Node, OwnerSessId, SessId, RootPath, Fun, ComplementaryPermsPerFile, AllRequiredPerms, AceWho, AceFlags, deny) ->
    AllPermsPerFile = maps:map(fun(_, _) -> ?ALL_PERMS end, ComplementaryPermsPerFile),

    % Denying only required perms and granting all others should result in eacces
    lists:foreach(fun({Guid, Perm}) ->
        set_acls(Node, AllPermsPerFile, #{Guid => [Perm]}, AceWho, AceFlags),
        ?assertMatch({error, ?EACCES}, Fun(OwnerSessId, SessId, RootPath))
    end, AllRequiredPerms),

    % Denying all perms but required ones should result in success
    set_acls(Node, #{}, ComplementaryPermsPerFile, AceWho, AceFlags),
    ?assertNotMatch({error, _}, Fun(OwnerSessId, SessId, RootPath)).


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
            (<<"file", _/binary>> = FileName, FileDesc, Acc) ->
                FilePath = <<ParentDirPath/binary, "/", FileName/binary>>,
                {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, FilePath, 8#777)),
                case maps:get(content, FileDesc, undefined) of
                    undefined ->
                        ok;
                    Content ->
                        {ok, FileHandle} = lfm_proxy:open(Node, SessId, {guid, FileGuid}, write),
                        ?assertMatch({ok, _}, lfm_proxy:write(Node, FileHandle, 0, Content)),
                        ?assertMatch(ok, lfm_proxy:fsync(Node, FileHandle)),
                        ?assertMatch(ok, lfm_proxy:close(Node, FileHandle))
                end,
                case maps:get(hook, FileDesc, undefined) of
                    undefined -> ok;
                    Fun -> Fun(SessId, FileGuid)
                end,
                case maps:get(perms, FileDesc, undefined) of
                    undefined -> Acc;
                    FilePerms -> [{FileGuid, FilePerms} | Acc]
                end;
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


-spec perm_to_posix_perms(Perm :: binary()) -> PosixPerms :: [atom()].
perm_to_posix_perms(?read_object) -> [read];
perm_to_posix_perms(?list_container) -> [read];
perm_to_posix_perms(?write_object) -> [write];
perm_to_posix_perms(?add_object) -> [write, exec];
perm_to_posix_perms(?append_data) -> [write];
perm_to_posix_perms(?add_subcontainer) -> [write];
perm_to_posix_perms(?read_metadata) -> [read];
perm_to_posix_perms(?write_metadata) -> [write];
perm_to_posix_perms(?execute) -> [exec];
perm_to_posix_perms(?traverse_container) -> [exec];
perm_to_posix_perms(?delete_object) -> [write, exec];
perm_to_posix_perms(?delete_subcontainer) -> [write, exec];
perm_to_posix_perms(?read_attributes) -> [];
perm_to_posix_perms(?write_attributes) -> [write];
perm_to_posix_perms(?delete) -> [owner_if_parent_sticky];
perm_to_posix_perms(?read_acl) -> [];
perm_to_posix_perms(?write_acl) -> [owner].


-spec posix_perm_to_mode(PosixPerm :: atom(), Type :: owner | group) ->
    non_neg_integer().
posix_perm_to_mode(read, owner) -> 8#4 bsl 6;
posix_perm_to_mode(write, owner) -> 8#2 bsl 6;
posix_perm_to_mode(exec, owner) -> 8#1 bsl 6;
posix_perm_to_mode(read, group) -> 8#4 bsl 3;
posix_perm_to_mode(write, group) -> 8#2 bsl 3;
posix_perm_to_mode(exec, group) -> 8#1 bsl 3;
posix_perm_to_mode(_, _) -> 8#0.


-spec combinations([term()]) -> [[term()]].
combinations([]) ->
    [[]];
combinations([Item | Items]) ->
    Combinations = combinations(Items),
    [[Item | Comb] || Comb <- Combinations] ++ Combinations.
