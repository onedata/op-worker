%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of lfm symlink API - creating and reading symlinks
%%% as well as resolving them against the file tree.
%%% @end
%%%-------------------------------------------------------------------
-module(file_lfm_symlinks_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("file/file_tree_test.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_and_read_symlink_test/1,
    create_hardlink_to_symlink_test/1,

    rubbish_path_test/1,
    non_existent_path_test/1,
    path_with_file_in_the_middle_test/1,
    user_root_absolute_path_test/1,
    space_absolute_path_test/1,
    relative_path_test/1,
    path_with_dots_test/1,
    symlink_to_itself_test/1,
    symlink_loop_test/1,
    symlink_hops_limit_test/1,
    symlink_chain_test/1,
    symlink_in_share_test/1
]).

% NOTE: the crud tests are run sequentially, as freezing the cluster clock
% (needed to observe timestamp changes) affects all the nodes at once.
groups() -> [
    {crud_tests, [], [
        create_and_read_symlink_test,
        create_hardlink_to_symlink_test
    ]},
    {resolution_tests, [parallel], [
        rubbish_path_test,
        non_existent_path_test,
        path_with_file_in_the_middle_test,
        user_root_absolute_path_test,
        space_absolute_path_test,
        relative_path_test,
        path_with_dots_test,
        symlink_to_itself_test,
        symlink_loop_test,
        symlink_hops_limit_test,
        symlink_chain_test,
        symlink_in_share_test
    ]}
].

all() -> [
    {group, crud_tests},
    {group, resolution_tests}
].


%%%====================================================================
%%% Crud test functions
%%%====================================================================


create_and_read_symlink_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    #object{guid = DirGuid} = file_tree_test_utils:create_and_sync_file_tree(
        user1, space_krk, #dir_spec{}
    ),
    SymlinkValue = <<"test_link">>,
    SymlinkValueSize = byte_size(SymlinkValue),
    ListingOpts = #{offset => 0, limit => 10, tune_for_large_continuous_listing => false},

    {ok, SymlinkAttrs} = ?assertMatch({ok, #file_attr{
        type = ?SYMLINK_TYPE,
        size = SymlinkValueSize,
        is_fully_replicated = undefined,
        parent_guid = DirGuid
    }}, lfm_proxy:make_symlink(
        Node, SessId, ?FILE_REF(DirGuid), str_utils:rand_hex(10), SymlinkValue
    )),
    ?assert(SymlinkAttrs#file_attr.atime > 0),
    ?assert(SymlinkAttrs#file_attr.mtime > 0),
    ?assert(SymlinkAttrs#file_attr.ctime > 0),
    ?assert(fslogic_file_id:is_symlink_uuid(file_id:guid_to_uuid(SymlinkAttrs#file_attr.guid))),

    SymlinkRef = ?FILE_REF(SymlinkAttrs#file_attr.guid),

    time_test_utils:simulate_seconds_passing(2),  % ensure the access time changes
    ?assertEqual({ok, SymlinkValue}, lfm_proxy:read_symlink(Node, SessId, SymlinkRef)),

    {ok, SymlinkAttrs2} = ?assertMatch({ok, #file_attr{
        type = ?SYMLINK_TYPE,
        size = SymlinkValueSize,
        is_fully_replicated = undefined,
        parent_guid = DirGuid
    }}, lfm_proxy:stat(Node, SessId, SymlinkRef)),
    ?assert(SymlinkAttrs2#file_attr.atime > SymlinkAttrs#file_attr.atime),
    ?assertMatch({ok, [SymlinkAttrs2], _}, lfm_proxy:get_children_attrs(
        Node, SessId, ?FILE_REF(DirGuid), ListingOpts
    )),

    ?assertEqual(ok, lfm_proxy:unlink(Node, SessId, SymlinkRef)),
    ?assertEqual({error, ?ENOENT}, lfm_proxy:read_symlink(Node, SessId, SymlinkRef)),
    ?assertMatch({ok, [], _}, lfm_proxy:get_children_attrs(
        Node, SessId, ?FILE_REF(DirGuid), ListingOpts
    )).


create_hardlink_to_symlink_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    #object{guid = DirGuid} = file_tree_test_utils:create_and_sync_file_tree(
        user1, space_krk, #dir_spec{}
    ),
    SymlinkValue = <<"test_link">>,

    {ok, #file_attr{guid = SymlinkGuid}} = ?assertMatch({ok, #file_attr{type = ?SYMLINK_TYPE}},
        lfm_proxy:make_symlink(Node, SessId, ?FILE_REF(DirGuid), str_utils:rand_hex(10), SymlinkValue)),

    % a hardlink to a symlink is a symlink itself and points to the very same target
    {ok, #file_attr{guid = HardlinkGuid}} = ?assertMatch(
        {ok, #file_attr{type = ?SYMLINK_TYPE}},
        lfm_proxy:make_link(Node, SessId, ?FILE_REF(SymlinkGuid), ?FILE_REF(DirGuid), str_utils:rand_hex(10))
    ),

    ?assertNotEqual(SymlinkGuid, HardlinkGuid),
    ?assertEqual({ok, SymlinkValue}, lfm_proxy:read_symlink(Node, SessId, ?FILE_REF(SymlinkGuid))),
    ?assertEqual({ok, SymlinkValue}, lfm_proxy:read_symlink(Node, SessId, ?FILE_REF(HardlinkGuid))).


%%%====================================================================
%%% Resolution test functions
%%%====================================================================


rubbish_path_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    #object{guid = SymlinkGuid} = file_tree_test_utils:create_and_sync_file_tree(
        user1, space_krk, #symlink_spec{symlink_value = <<"rubbish<>!@#xd">>}
    ),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(SymlinkGuid))).


non_existent_path_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    #object{guid = SymlinkGuid} = file_tree_test_utils:create_and_sync_file_tree(
        user1, space_krk, #symlink_spec{symlink_value = <<"a/b">>}
    ),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(SymlinkGuid))).


path_with_file_in_the_middle_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    DirName = str_utils:rand_hex(10),
    FileName = str_utils:rand_hex(10),

    [_, #object{guid = SymlinkGuid}] = file_tree_test_utils:create_and_sync_file_tree(
        user1, space_krk, [
            #dir_spec{name = DirName, children = [#file_spec{name = FileName}]},
            #symlink_spec{symlink_value = filename:join([DirName, FileName, "file2"])}
        ]
    ),

    ?assertMatch({error, ?ENOTDIR}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(SymlinkGuid))).


user_root_absolute_path_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    SpaceName = oct_background:get_space_name(space_krk),
    FileName = str_utils:rand_hex(10),

    [_, #object{guid = SymlinkGuid}] = file_tree_test_utils:create_and_sync_file_tree(
        user1, space_krk, [
            #file_spec{name = FileName},
            #symlink_spec{symlink_value = filename:join(["/", SpaceName, FileName])}
        ]
    ),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(SymlinkGuid))).


space_absolute_path_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(oct_background:get_space_id(space_krk)),
    DirName = str_utils:rand_hex(10),
    FileName = str_utils:rand_hex(10),

    [
        #object{children = [#object{guid = FileGuid}]},
        #object{guid = SymlinkGuid}
    ] = file_tree_test_utils:create_and_sync_file_tree(user1, space_krk, [
        #dir_spec{name = DirName, children = [#file_spec{name = FileName}]},
        #symlink_spec{symlink_value = filename:join([SpaceIdPrefix, DirName, FileName])}
    ]),

    ?assertMatch({ok, FileGuid}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(SymlinkGuid))).


relative_path_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    FileName = str_utils:rand_hex(10),

    #object{children = [
        #object{guid = FileGuid},
        #object{guid = SymlinkGuid}
    ]} = file_tree_test_utils:create_and_sync_file_tree(user1, space_krk, #dir_spec{children = [
        #file_spec{name = FileName},
        #symlink_spec{symlink_value = FileName}
    ]}),

    ?assertMatch({ok, FileGuid}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(SymlinkGuid))).


path_with_dots_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    Dir1Name = str_utils:rand_hex(10),
    FileName = str_utils:rand_hex(10),

    #object{children = [
        #object{guid = FileGuid},
        #object{guid = Symlink1Guid},
        #object{children = [
            #object{guid = Symlink2Guid},
            #object{guid = Symlink3Guid}
        ]}
    ]} = file_tree_test_utils:create_and_sync_file_tree(user1, space_krk, #dir_spec{
        name = Dir1Name,
        children = [
            #file_spec{name = FileName},
            #symlink_spec{symlink_value = filename:join([".", FileName])},
            #dir_spec{children = [
                #symlink_spec{symlink_value = filename:join(["..", FileName])},
                #symlink_spec{symlink_value = filename:join([".", "..", "..", ".", "..", "..", Dir1Name, FileName])}
            ]}
        ]
    }),

    ?assertMatch({ok, FileGuid}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(Symlink1Guid))),
    ?assertMatch({ok, FileGuid}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(Symlink2Guid))),
    ?assertMatch({ok, FileGuid}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(Symlink3Guid))).


symlink_to_itself_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    SymlinkName = str_utils:rand_hex(10),

    #object{guid = SymlinkGuid} = file_tree_test_utils:create_and_sync_file_tree(
        user1, space_krk, #symlink_spec{name = SymlinkName, symlink_value = SymlinkName}
    ),

    ?assertMatch({error, ?ELOOP}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(SymlinkGuid))).


symlink_loop_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    Symlink1Name = str_utils:rand_hex(10),
    Symlink2Name = str_utils:rand_hex(10),
    Symlink3Name = str_utils:rand_hex(10),

    [
        #object{guid = Symlink1Guid},
        #object{guid = Symlink2Guid},
        #object{guid = Symlink3Guid}
    ] = file_tree_test_utils:create_and_sync_file_tree(user1, space_krk, [
        #symlink_spec{name = Symlink1Name, symlink_value = Symlink2Name},
        #symlink_spec{name = Symlink2Name, symlink_value = Symlink3Name},
        #symlink_spec{name = Symlink3Name, symlink_value = Symlink1Name}
    ]),

    ?assertMatch({error, ?ELOOP}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(Symlink1Guid))),
    ?assertMatch({error, ?ELOOP}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(Symlink2Guid))),
    ?assertMatch({error, ?ELOOP}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(Symlink3Guid))).


symlink_hops_limit_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    FileName = str_utils:rand_hex(10),

    {_, SymlinkSpecs} = lists:foldl(fun(_, {PrevSymlinkName, Acc}) ->
        SymlinkName = str_utils:rand_hex(10),
        SymlinkSpec = #symlink_spec{name = SymlinkName, symlink_value = PrevSymlinkName},
        {SymlinkName, [SymlinkSpec | Acc]}
    end, {FileName, []}, lists:seq(1, 41)),

    [
        #object{guid = FileGuid} | Symlinks
    ] = file_tree_test_utils:create_and_sync_file_tree(user1, space_krk, [
        #file_spec{name = FileName} | lists:reverse(SymlinkSpecs)
    ]),

    lists:foreach(fun(#object{guid = SymlinkGuid}) ->
        ?assertMatch({ok, FileGuid}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(SymlinkGuid)))
    end, lists:droplast(Symlinks)),

    #object{guid = InvalidSymlink} = lists:last(Symlinks),
    ?assertMatch({error, ?ELOOP}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(InvalidSymlink))).


symlink_chain_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),

    Dir1Name = str_utils:rand_hex(10),
    FileName = str_utils:rand_hex(10),
    Dir2Name = str_utils:rand_hex(10),
    Symlink1Name = str_utils:rand_hex(10),

    [
        #object{guid = Dir1Guid, children = [
            #object{guid = FileGuid},
            #object{children = [#object{guid = Symlink1Guid}]}
        ]},
        #object{guid = Symlink2Guid}
    ] = file_tree_test_utils:create_and_sync_file_tree(user1, space_krk, [
        #dir_spec{name = Dir1Name, children = [
            #file_spec{name = FileName},
            #dir_spec{name = Dir2Name, children = [#symlink_spec{
                name = Symlink1Name,
                symlink_value = <<"./..">>
            }]}
        ]},
        #symlink_spec{symlink_value = filename:join([Dir1Name, Dir2Name, Symlink1Name, FileName])}
    ]),

    ?assertMatch({ok, Dir1Guid}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(Symlink1Guid))),
    ?assertMatch({ok, FileGuid}, lfm_proxy:resolve_symlink(Node, SessId, ?FILE_REF(Symlink2Guid))).


symlink_in_share_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),

    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(oct_background:get_space_id(space_krk)),
    File1Name = str_utils:rand_hex(10),
    DirName = str_utils:rand_hex(10),
    File2Name = str_utils:rand_hex(10),

    [
        #object{guid = _File1Guid},
        #object{shares = [DirShareId], children = [
            #object{guid = File2Guid},
            #object{guid = Symlink1Guid}, #object{guid = Symlink2Guid}, #object{guid = Symlink3Guid}
        ]}
    ] = file_tree_test_utils:create_and_sync_file_tree(user1, space_krk, [
        #file_spec{name = File1Name},
        #dir_spec{name = DirName, shares = [#share_spec{}], children = [
            #file_spec{name = File2Name},
            #symlink_spec{symlink_value = filename:join([SpaceIdPrefix, DirName, File2Name])},
            #symlink_spec{symlink_value = filename:join([SpaceIdPrefix, File1Name])},
            #symlink_spec{symlink_value = filename:join(["..", "..", "..", "..", File2Name])}
        ]}
    ]),

    File2ShareGuid = file_id:guid_to_share_guid(File2Guid, DirShareId),
    Symlink1ShareGuid = file_id:guid_to_share_guid(Symlink1Guid, DirShareId),
    Symlink2ShareGuid = file_id:guid_to_share_guid(Symlink2Guid, DirShareId),
    Symlink3ShareGuid = file_id:guid_to_share_guid(Symlink3Guid, DirShareId),

    % Space absolute path pointing to file in share
    ?assertMatch({ok, File2ShareGuid}, lfm_proxy:resolve_symlink(Node, ?GUEST_SESS_ID, ?FILE_REF(Symlink1ShareGuid))),

    % Space absolute path pointing to file outside share
    ?assertMatch({error, ?ENOENT}, lfm_proxy:resolve_symlink(Node, ?GUEST_SESS_ID, ?FILE_REF(Symlink2ShareGuid))),

    % Relative path (can't traverse outside of share)
    ?assertMatch({ok, File2ShareGuid}, lfm_proxy:resolve_symlink(Node, ?GUEST_SESS_ID, ?FILE_REF(Symlink3ShareGuid))).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    opt:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(create_and_read_symlink_test = Case, Config) ->
    time_test_utils:freeze_time(Config),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(create_and_read_symlink_test, Config) ->
    time_test_utils:unfreeze_time(Config);

end_per_testcase(_Case, _Config) ->
    ok.
