%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of token data access caveats.
%%% @end
%%%-------------------------------------------------------------------
-module(authz_data_access_caveats_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    list_user_root_dir_test/1,
    list_space_root_dir_test/1,
    list_directory_test/1,
    list_directory_with_offset_and_limit_test/1,
    list_directory_with_caveats_for_different_directory_test/1,
    list_directory_with_intersecting_caveats_test/1,
    list_shared_directory_test/1,
    list_ancestors_with_intersecting_caveats_test/1,
    list_previously_non_existent_file/1,

    allowed_ancestors_operations_test/1,
    data_access_caveats_cache_test/1,
    mv_test/1
]).

groups() -> [
    {ls_tests, [parallel], [
        list_user_root_dir_test,
        list_space_root_dir_test,
        list_directory_test,
        list_directory_with_offset_and_limit_test,
        list_directory_with_caveats_for_different_directory_test,
        list_directory_with_intersecting_caveats_test,
        list_shared_directory_test,
        list_ancestors_with_intersecting_caveats_test,
        list_previously_non_existent_file
    ]},
    {misc_tests, [parallel], [
        allowed_ancestors_operations_test,
        data_access_caveats_cache_test,
        mv_test
    ]}
].

all() -> [
    {group, ls_tests},
    {group, misc_tests}
].

-define(ATTEMPTS, 20).


%%%===================================================================
%%% ls tests
%%%===================================================================


-define(LS_SPACE, space_krk_par_p).
-define(LS_USER, user2).

-define(LS_FILE_TREE_SPEC, [
    #dir_spec{
        name = <<"ls_d1">>,
        shares = [#share_spec{}],
        children = [
            #file_spec{name = <<"f1">>},
            #file_spec{name = <<"f2">>},
            #file_spec{name = <<"f3">>},
            #file_spec{name = <<"f4">>},
            #file_spec{name = <<"f5">>}
        ]
    },
    #dir_spec{
        name = <<"ls_d2">>
    },
    #dir_spec{
        name = <<"ls_d3">>,
        children = [
            #dir_spec{
                name = <<"d1">>,
                children = [#file_spec{name = <<"f1">>}]
            },
            #file_spec{name = <<"f1">>},
            #file_spec{name = <<"f2">>}
        ]
    },
    #file_spec{name = <<"ls_f1">>}
]).

-define(LS_CPATH(__PATH), ls_build_cpath(?LS_SPACE, str_utils:to_binary(__PATH))).
-define(LS_GUID(__PATH), ls_get_guid(str_utils:to_binary(__PATH))).
-define(LS_OBJECT_ID(__PATH), ls_get_object_id(str_utils:to_binary(__PATH))).
-define(LS_ENTRY(__PATH), ls_get_entry(str_utils:to_binary(__PATH))).

-define(CV_PATH(__CANONICAL_PATHS), #cv_data_path{whitelist = __CANONICAL_PATHS}).
-define(CV_OBJECTID(__OBJECT_IDS), #cv_data_objectid{whitelist = __OBJECT_IDS}).


list_user_root_dir_test(_Config) ->
    UserRootDirGuid = fslogic_file_id:user_root_dir_guid(oct_background:get_user_id(?LS_USER)),

    Space1Name = oct_background:get_space_name(space1),
    Space1Guid = fslogic_file_id:spaceid_to_space_dir_guid(oct_background:get_space_id(space1)),

    SpaceKrkParPName = oct_background:get_space_name(space_krk_par_p),
    SpaceKrkParPId = oct_background:get_space_id(space_krk_par_p),
    SpaceKrkParPGuid = fslogic_file_id:spaceid_to_space_dir_guid(oct_background:get_space_id(space_krk_par_p)),

    % With no caveats listing user root dir should list all user spaces
    ?assertEqual(
        {ok, [{Space1Guid, Space1Name}, {SpaceKrkParPGuid, SpaceKrkParPName}]},
        ls_with_caveats(UserRootDirGuid, [])
    ),

    % But with caveats user root dir ls should show only spaces leading to allowed files
    ?assertEqual(
        {ok, [{SpaceKrkParPGuid, SpaceKrkParPName}]},
        ls_with_caveats(UserRootDirGuid, ?CV_PATH([filepath_utils:join([<<"/">>, SpaceKrkParPId])]))
    ),
    ?assertEqual(
        {ok, [{SpaceKrkParPGuid, SpaceKrkParPName}]},
        ls_with_caveats(UserRootDirGuid, ?CV_OBJECTID([?LS_OBJECT_ID("ls_d3/d1/f1")]))
    ).


list_space_root_dir_test(_Config) ->
    SpaceRootDirGuid = fslogic_file_id:spaceid_to_space_dir_guid(oct_background:get_space_id(?LS_SPACE)),

    % With no caveats listing space dir should list all space directories and files
    ?assertEqual(
        {ok, [?LS_ENTRY("ls_d1"), ?LS_ENTRY("ls_d2"), ?LS_ENTRY("ls_d3"), ?LS_ENTRY("ls_f1")]},
        ls_with_caveats(SpaceRootDirGuid, [])
    ),

    % But with caveats space ls should show only dirs leading to allowed files (even if they do not exist).
    ?assertEqual(
        {ok, [?LS_ENTRY("ls_d1"), ?LS_ENTRY("ls_d3")]},
        ls_with_caveats(SpaceRootDirGuid, ?CV_OBJECTID([?LS_OBJECT_ID("ls_d1/f1"), ?LS_OBJECT_ID("ls_d3/f2")]))
    ),
    ?assertEqual(
        {ok, [?LS_ENTRY("ls_d3"), ?LS_ENTRY("ls_f1")]},
        ls_with_caveats(SpaceRootDirGuid, ?CV_PATH([?LS_CPATH("ls_d3/non_existent_file"), ?LS_CPATH("ls_f1")]))
    ).


list_directory_test(_Config) ->
    % Whitelisting Dir should result in listing all it's files
    AllFileEntries = [
        ?LS_ENTRY("ls_d1/f1"), ?LS_ENTRY("ls_d1/f2"), ?LS_ENTRY("ls_d1/f3"),
        ?LS_ENTRY("ls_d1/f4"), ?LS_ENTRY("ls_d1/f5")
    ],
    ?assertEqual({ok, AllFileEntries}, ls_with_caveats(?LS_GUID("ls_d1"), ?CV_PATH([?LS_CPATH("ls_d1")]))),
    ?assertEqual({ok, AllFileEntries}, ls_with_caveats(?LS_GUID("ls_d1"), ?CV_OBJECTID([?LS_OBJECT_ID("ls_d1")]))),

    % Whitelisting concrete files should result in listing only them (the nonexistent ones will be omitted)
    ?assertEqual(
        {ok, [?LS_ENTRY("ls_d1/f1"), ?LS_ENTRY("ls_d1/f3")]},
        ls_with_caveats(?LS_GUID("ls_d1"), ?CV_PATH([
            ?LS_CPATH("ls_d1/f1"), ?LS_CPATH("ls_d1/f3"), ?LS_CPATH("ls_d1/non_existent_file")
        ]))
    ),
    ?assertEqual(
        {ok, [?LS_ENTRY("ls_d1/f2"), ?LS_ENTRY("ls_d1/f4")]},
        ls_with_caveats(?LS_GUID("ls_d1"), ?CV_OBJECTID([?LS_OBJECT_ID("ls_d1/f2"), ?LS_OBJECT_ID("ls_d1/f4")]))
    ),

    CaveatsWithFilesInDifferentDir = ?CV_PATH([?LS_CPATH("ls_d1/f1"), ?LS_CPATH("ls_d3/f1")]),
    ?assertEqual({ok, [?LS_ENTRY("ls_d1/f1")]}, ls_with_caveats(?LS_GUID("ls_d1"), CaveatsWithFilesInDifferentDir)),
    ?assertEqual({ok, [?LS_ENTRY("ls_d3/f1")]}, ls_with_caveats(?LS_GUID("ls_d3"), CaveatsWithFilesInDifferentDir)).


list_directory_with_offset_and_limit_test(_Config) ->
    Caveats = ?CV_PATH([
        ?LS_CPATH("ls_d1/f1"), ?LS_CPATH("ls_d1/f2"), ?LS_CPATH("ls_d1/dummy"),
        ?LS_CPATH("ls_d1/f4"), ?LS_CPATH("ls_d1/f5")
    ]),

    ?assertEqual(
        {ok, [?LS_ENTRY("ls_d1/f1"), ?LS_ENTRY("ls_d1/f2"), ?LS_ENTRY("ls_d1/f4")]},
        ls_with_caveats(?LS_GUID("ls_d1"), Caveats, 0, 3)
    ),
    ?assertEqual(
        {ok, [?LS_ENTRY("ls_d1/f4"), ?LS_ENTRY("ls_d1/f5")]},
        ls_with_caveats(?LS_GUID("ls_d1"), Caveats, 2, 3)
    ),
    ?assertEqual(
        {ok, [?LS_ENTRY("ls_d1/f4")]},
        ls_with_caveats(?LS_GUID("ls_d1"), Caveats, 2, 1)
    ).


list_directory_with_caveats_for_different_directory_test(_Config) ->
    ?assertEqual(
        {error, ?EACCES},
        ls_with_caveats(?LS_GUID("ls_d1"), ?CV_OBJECTID([?LS_OBJECT_ID("ls_d3")]))
    ).


list_directory_with_intersecting_caveats_test(_Config) ->
    % Using several caveats should result in listing only their intersection
    ?assertEqual(
        {ok, [?LS_ENTRY("ls_d1/f1"), ?LS_ENTRY("ls_d1/f4")]},
        ls_with_caveats(?LS_GUID("ls_d1"), [
            ?CV_PATH([?LS_CPATH("ls_d1/f1"), ?LS_CPATH("ls_d1/f2"), ?LS_CPATH("ls_d1/f4")]),
            ?CV_PATH([?LS_CPATH("ls_d1/f1"), ?LS_CPATH("ls_d1/f3"), ?LS_CPATH("ls_d1/f4")])
        ])
    ),
    ?assertEqual(
        {ok, [?LS_ENTRY("ls_d1/f1"), ?LS_ENTRY("ls_d1/f4")]},
        ls_with_caveats(?LS_GUID("ls_d1"), [
            ?CV_OBJECTID([?LS_OBJECT_ID("ls_d1/f1"), ?LS_OBJECT_ID("ls_d1/f2"), ?LS_OBJECT_ID("ls_d1/f4")]),
            ?CV_OBJECTID([?LS_OBJECT_ID("ls_d1/f1"), ?LS_OBJECT_ID("ls_d1/f3"), ?LS_OBJECT_ID("ls_d1/f4")])
        ])
    ),
    ?assertEqual(
        {ok, [?LS_ENTRY("ls_d1/f1"), ?LS_ENTRY("ls_d1/f4")]},
        ls_with_caveats(?LS_GUID("ls_d1"), [
            ?CV_PATH([?LS_CPATH("ls_d1/f1"), ?LS_CPATH("ls_d1/f2"), ?LS_CPATH("ls_d1/f4")]),
            ?CV_OBJECTID([?LS_OBJECT_ID("ls_d1/f1"), ?LS_OBJECT_ID("ls_d1/f3"), ?LS_OBJECT_ID("ls_d1/f4")])
        ])
    ),
    ?assertEqual(
        {ok, []},
        ls_with_caveats(?LS_GUID("ls_d1"), [
            ?CV_PATH([?LS_CPATH("ls_d1/f1"), ?LS_CPATH("ls_d1/f4")]),
            ?CV_OBJECTID([?LS_OBJECT_ID("ls_d1/f2"), ?LS_OBJECT_ID("ls_d1/f3")])
        ])
    ).


list_shared_directory_test(_Config) ->
    DirGuid = ?LS_GUID("ls_d1"),
    [DirShareId] = kv_utils:get([<<"ls_d1">>, shares], node_cache:get(ls_tests_file_tree)),
    DirShareGuid = file_id:guid_to_share_guid(DirGuid, DirShareId),

    % Token confinements have no effects on accessing files via shared guid -
    % all operations are automatically performed with ?GUEST perms
    ExpEntries = lists:map(fun(AbbrevPath) ->
        {Guid, Name} = ?LS_ENTRY(AbbrevPath),
        {file_id:guid_to_share_guid(Guid, DirShareId), Name}
    end, ["ls_d1/f1", "ls_d1/f2", "ls_d1/f3", "ls_d1/f4", "ls_d1/f5"]),

    ?assertEqual({ok, ExpEntries}, ls_with_caveats(DirShareGuid, ?CV_PATH([?LS_CPATH("ls_d1")]))),
    ?assertEqual({ok, ExpEntries}, ls_with_caveats(DirShareGuid, ?CV_PATH([
        ?LS_CPATH("ls_d1/f1"), ?LS_CPATH("ls_d1/f2"), ?LS_CPATH("ls_d1/f4")
    ]))),
    ?assertEqual({ok, ExpEntries}, ls_with_caveats(DirShareGuid, [
        ?CV_OBJECTID([?LS_OBJECT_ID("ls_d1/f1"), ?LS_OBJECT_ID("ls_d1/f3"), ?LS_OBJECT_ID("ls_d1/f4")]),
        ?CV_OBJECTID([?LS_OBJECT_ID("ls_d1/f1"), ?LS_OBJECT_ID("ls_d1/f4")]),
        ?CV_OBJECTID([?LS_OBJECT_ID("ls_d1/f4")])
    ])),
    ?assertEqual({ok, ExpEntries}, ls_with_caveats(DirShareGuid, ?CV_PATH([?LS_CPATH("non_existent_file")]))).


list_ancestors_with_intersecting_caveats_test(_Config) ->
    UserRootDirGuid = fslogic_file_id:user_root_dir_guid(oct_background:get_user_id(?LS_USER)),

    SpaceName = oct_background:get_space_name(?LS_SPACE),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(oct_background:get_space_id(?LS_SPACE)),

    Caveats = [
        ?CV_OBJECTID([?LS_OBJECT_ID("ls_d3/d1/f1")]),
        ?CV_PATH([?LS_CPATH("ls_d3/f2")])
    ],

    % Only ancestor directories common in all caveats SHOULD be listed
    ?assertEqual({ok, [{SpaceGuid, SpaceName}]}, ls_with_caveats(UserRootDirGuid, Caveats)),
    ?assertEqual({ok, [?LS_ENTRY("ls_d3")]}, ls_with_caveats(SpaceGuid, Caveats)),
    ?assertEqual({ok, []}, ls_with_caveats(?LS_GUID("ls_d3"), Caveats)).


list_previously_non_existent_file(_Config) ->
    Node = oct_background:get_random_provider_node(?RAND_ELEMENT([krakow, paris])),
    UserId = oct_background:get_user_id(?LS_USER),

    % List dir manually to use the same exact session with caveat for file not existing yet
    MainToken = get_main_token(),
    LsToken = tokens:confine(MainToken, ?CV_PATH([?LS_CPATH("ls_d2/f1")])),
    LsSessId = permissions_test_utils:create_session(Node, UserId, LsToken),

    LS = fun(Guid) -> lfm_proxy:get_children(Node, LsSessId, ?FILE_REF(Guid), 0, 100) end,

    ?assertEqual({ok, []}, LS(?LS_GUID("ls_d2"))),

    [#object{guid = FileGuid}, _] = onenv_file_test_utils:create_and_sync_file_tree(
        ?LS_USER, ?LS_GUID("ls_d2"), [
            #file_spec{name = <<"f1">>},
            #file_spec{name = <<"f2">>}
        ]
    ),

    ?assertEqual({ok, [{FileGuid, <<"f1">>}]}, LS(?LS_GUID("ls_d2"))).


%% @private
ls_setup() ->
    UserId = oct_background:get_user_id(?LS_USER),
    MainToken = provider_onenv_test_utils:create_oz_temp_access_token(UserId),
    store_main_token(MainToken),

    FileTreeObjects = onenv_file_test_utils:create_and_sync_file_tree(
        user1, ?LS_SPACE, ?LS_FILE_TREE_SPEC
    ),
    FileTreeDesc = ls_describe_file_tree(#{}, <<>>, FileTreeObjects),
    node_cache:put(ls_tests_file_tree, FileTreeDesc).


%% @private
store_main_token(MainToken) ->
    node_cache:put(ls_tests_main_token, MainToken).


%% @private
get_main_token() ->
    node_cache:get(ls_tests_main_token).


%% @private
ls_describe_file_tree(Desc, ParentPath, FileTreeObjects) when is_list(FileTreeObjects) ->
    lists:foldl(fun(FileObject, DescAcc) ->
        ls_describe_file_tree(DescAcc, ParentPath, FileObject)
    end, Desc, FileTreeObjects);

ls_describe_file_tree(Desc, ParentPath, #object{
    guid = Guid,
    name = Name,
    shares = Shares,
    children = Children
}) ->
    Path = filepath_utils:join([ParentPath, Name]),
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),

    NewDesc = Desc#{Path => #{
        name => Name,
        guid => Guid,
        object_id => ObjectId,
        shares => Shares
    }},

    lists:foldl(fun(Child, DescAcc) ->
        ls_describe_file_tree(DescAcc, Path, Child)
    end, NewDesc, utils:ensure_defined(Children, [])).


%% @private
ls_build_cpath(SpaceSelector, RelPath) ->
    filepath_utils:join([<<"/">>, oct_background:get_space_id(SpaceSelector), RelPath]).


%% @private
ls_get_name(Path) ->
    kv_utils:get([Path, name], node_cache:get(ls_tests_file_tree)).


%% @private
ls_get_guid(Path) ->
    kv_utils:get([Path, guid], node_cache:get(ls_tests_file_tree)).


%% @private
ls_get_object_id(Path) ->
    kv_utils:get([Path, object_id], node_cache:get(ls_tests_file_tree)).


%% @private
ls_get_entry(Path) ->
    {ls_get_guid(Path), ls_get_name(Path)}.


%% @private
ls_with_caveats(Guid, Caveats) ->
    ls_with_caveats(Guid, Caveats, 0, 100).


%% @private
ls_with_caveats(Guid, Caveats, Offset, Limit) ->
    Node = oct_background:get_random_provider_node(?RAND_ELEMENT([krakow, paris])),
    UserId = oct_background:get_user_id(?LS_USER),

    MainToken = get_main_token(),
    LsToken = tokens:confine(MainToken, Caveats),
    LsSessId = permissions_test_utils:create_session(Node, UserId, LsToken),

    lfm_proxy:get_children(Node, LsSessId, ?FILE_REF(Guid), Offset, Limit).


%%%===================================================================
%%% Misc tests
%%%===================================================================


allowed_ancestors_operations_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),

    UserId = oct_background:get_user_id(user1),
    UserSessionId = oct_background:get_user_session_id(user1, krakow),
    UserRootDirGuid = fslogic_file_id:user_root_dir_guid(UserId),

    SpaceName = oct_background:get_space_name(space_krk_par_p),
    SpaceRootDirGuid = fslogic_file_id:spaceid_to_space_dir_guid(oct_background:get_space_id(space_krk_par_p)),

    InaccessibleFileName = <<"inaccessible_file">>,

    ReversedAncestors = [{LastDirGuid, _} | _] = lists:foldl(fun(Num, [{DirGuid, _} | _] = Acc) ->
        ChildDirName = <<"dir", (integer_to_binary(Num))/binary>>,
        {ok, ChildDirGuid} = lfm_proxy:mkdir(Node, UserSessionId, DirGuid, ChildDirName, 8#777),
        {ok, _} = lfm_proxy:create(Node, UserSessionId, DirGuid, InaccessibleFileName, 8#777),
        [{ChildDirGuid, ChildDirName} | Acc]
    end, [{SpaceRootDirGuid, SpaceName}], lists:seq(1, 20)),

    Ancestors = lists:reverse(ReversedAncestors),

    DeepestDirName = <<"deepest_dir">>,
    {ok, DeepestDirGuid} = lfm_proxy:mkdir(Node, UserSessionId, LastDirGuid, DeepestDirName, 8#777),
    {ok, DeepestDirObjectId} = file_id:guid_to_objectid(DeepestDirGuid),

    Token = tokens:confine(
        provider_onenv_test_utils:create_oz_temp_access_token(UserId),
        ?CV_OBJECTID([DeepestDirObjectId])
    ),
    SessionIdWithCaveats = permissions_test_utils:create_session(Node, UserId, Token),

    lists:foldl(
        fun({{DirGuid, DirName}, Child}, {ParentPath, ParentGuid}) ->
            DirPath = case ParentPath of
                <<>> -> <<"/">>;
                <<"/">> -> <<"/", DirName/binary>>;
                _ -> <<ParentPath/binary, "/", DirName/binary>>
            end,

            % Most operations should be forbidden to perform on dirs/ancestors
            % leading to files allowed by caveats
            ?assertMatch(
                {error, ?EACCES},
                lfm_proxy:get_acl(Node, SessionIdWithCaveats, ?FILE_REF(DirGuid))
            ),
            ?assertMatch(
                {error, ?EACCES},
                lfm_proxy:create(Node, SessionIdWithCaveats, DirGuid, ?RAND_STR(), 8#777)
            ),

            % Below operations should succeed for every dir/ancestor leading
            % to file allowed by caveats
            ?assertMatch(
                {ok, [Child]},
                lfm_proxy:get_children(Node, SessionIdWithCaveats, ?FILE_REF(DirGuid), 0, 100)
            ),
            ?assertMatch(
                {ok, #file_attr{name = DirName, type = ?DIRECTORY_TYPE}},
                lfm_proxy:stat(Node, SessionIdWithCaveats, ?FILE_REF(DirGuid))
            ),
            ?assertMatch(
                {ok, DirGuid},
                lfm_proxy:resolve_guid(Node, SessionIdWithCaveats, DirPath)
            ),
            ?assertMatch(
                {ok, ParentGuid},
                lfm_proxy:get_parent(Node, SessionIdWithCaveats, ?FILE_REF(DirGuid))
            ),
            ?assertMatch(
                {ok, DirPath},
                lfm_proxy:get_file_path(Node, SessionIdWithCaveats, DirGuid)
            ),

            % Get child attr should also succeed but only for children that are
            % also allowed by caveats
            case ParentGuid of
                undefined ->
                    ok;
                _ ->
                    ?assertMatch(
                        {ok, #file_attr{name = DirName, type = ?DIRECTORY_TYPE}},
                        lfm_proxy:get_child_attr(Node, SessionIdWithCaveats, ParentGuid, DirName)
                    ),
                    ?assertMatch(
                        {error, ?ENOENT},
                        lfm_proxy:get_child_attr(Node, SessionIdWithCaveats, ParentGuid, InaccessibleFileName)
                    )
            end,

            {DirPath, DirGuid}
        end,
        {<<>>, undefined},
        lists:zip([{UserRootDirGuid, UserId} | Ancestors], Ancestors ++ [{DeepestDirGuid, DeepestDirName}])
    ),

    % Get acl should finally succeed for dir which is allowed by caveats
    ?assertMatch(
        {ok, []},
        lfm_proxy:get_acl(Node, SessionIdWithCaveats, ?FILE_REF(DeepestDirGuid))
    ),
    ?assertMatch(
        {ok, _},
        lfm_proxy:create(Node, SessionIdWithCaveats, DeepestDirGuid, ?RAND_STR(), 8#777)
    ).


data_access_caveats_cache_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),

    UserId = oct_background:get_user_id(user1),
    UserRootDirGuid = fslogic_file_id:user_root_dir_guid(UserId),

    SpaceRootDirGuid = fslogic_file_id:spaceid_to_space_dir_guid(oct_background:get_space_id(space_krk_par_p)),

    #object{
        guid = RootDirGuid,
        children = [
            #object{guid =  DirGuid, children = [
                #object{guid =  FileGuid}
            ]},
            #object{guid = OtherDirGuid}
        ]
    } = onenv_file_test_utils:create_and_sync_file_tree(user1, space_krk_par_p, #dir_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        children = [
            #dir_spec{name = <<"dir">>, children = [
                #file_spec{name = <<"file">>}
            ]},
            #dir_spec{}
        ]
    }),
    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    CheckCacheFun = fun(Rule) -> rpc:call(Node, permissions_cache, check_permission, Rule) end,

    Token = tokens:confine(provider_onenv_test_utils:create_oz_temp_access_token(UserId), [
        ?CV_OBJECTID([DirObjectId]),
        ?CV_OBJECTID([FileObjectId])
    ]),
    SessionId = permissions_test_utils:create_session(Node, UserId, Token),


    %% CHECK guid_constraint CACHE

    % before any call cache should be empty
    lists:foreach(fun(Guid) ->
        ?assertEqual({error, not_found}, CheckCacheFun([{guid_constraint, Token, Guid}]))
    end, [UserRootDirGuid, SpaceRootDirGuid, RootDirGuid, DirGuid, FileGuid]),

    % call on file should fill cache up to root dir with remaining guid constraints
    ?assertEqual({ok, []}, lfm_proxy:get_acl(Node, SessionId, ?FILE_REF(FileGuid))),

    lists:foreach(fun(Guid) ->
        ?assertEqual(
            {ok, {false, [[FileGuid], [DirGuid]]}},
            CheckCacheFun([{guid_constraint, Token, Guid}])
        )
    end, [UserRootDirGuid, SpaceRootDirGuid, RootDirGuid]),
    ?assertEqual(
        {ok, {false, [[FileGuid]]}},
        CheckCacheFun([{guid_constraint, Token, DirGuid}])
    ),
    ?assertEqual(
        {ok, true},
        CheckCacheFun([{guid_constraint, Token, FileGuid}])
    ),


    %% CHECK data_constraint CACHE

    % data_constraint cache is not filed recursively as guid_constraint one is
    % so only for file should it be filled
    lists:foreach(fun(Guid) ->
        ?assertEqual({error, not_found}, CheckCacheFun([{data_constraint, Token, Guid}]))
    end, [UserRootDirGuid, SpaceRootDirGuid, RootDirGuid, DirGuid]),

    ?assertEqual(
        {ok, equal_or_descendant},
        CheckCacheFun([{data_constraint, Token, FileGuid}])
    ),

    % calling on dir any function reserved only for equal_or_descendant should
    % cache {equal_or_descendant, ?EACCES} meaning that no such operation can be
    % performed but since ancestor checks were not performed it is not known whether
    % ancestor operations can be performed
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:get_acl(Node, SessionId, ?FILE_REF(DirGuid))
    ),
    ?assertEqual(
        {ok, {equal_or_descendant, ?EACCES}},
        CheckCacheFun([{data_constraint, Token, DirGuid}])
    ),

    % after calling operation possible to perform on ancestor cached value should
    % be changed to signal that file is ancestor and such operations can be performed
    ?assertMatch(
        {ok, [_]},
        lfm_proxy:get_children(Node, SessionId, ?FILE_REF(DirGuid), 0, 100)
    ),
    ?assertEqual(
        {ok, {ancestor, [<<"file">>]}},
        CheckCacheFun([{data_constraint, Token, DirGuid}])
    ),

    % Calling ancestor operation on unrelated to file in caveats dir all checks
    % will be performed and just ?EACESS will be cached meaning that no operation
    % on this dir can be performed
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:get_children(Node, SessionId, ?FILE_REF(OtherDirGuid), 0, 100)
    ),
    ?assertEqual(
        {ok, ?EACCES},
        CheckCacheFun([{data_constraint, Token, OtherDirGuid}])
    ).


mv_test(_Config) ->
    ParisNode = oct_background:get_random_provider_node(paris),

    SessionIdParis = oct_background:get_user_session_id(user1, paris),

    SpaceId = oct_background:get_space_id(space_krk_par_p),
    SpaceName = oct_background:get_space_name(space_krk_par_p),

    #object{name = RootDirName, children = [
        #object{name = DirName},
        #object{name = FileName, guid = FileGuid}
    ]} = onenv_file_test_utils:create_and_sync_file_tree(user1, space_krk_par_p, #dir_spec{
        name = str_utils:to_binary(?FUNCTION_NAME),
        children = [
            #dir_spec{},
            #file_spec{content = ?RAND_STR()}
        ]
    }),

    MvPath = filepath_utils:join([<<"/">>, SpaceName, RootDirName, DirName, FileName]),
    CanonicalMvPath = filepath_utils:join([<<"/">>, SpaceId, RootDirName, DirName, FileName]),

    ?assertMatch({ok, _}, lfm_proxy:stat(ParisNode, SessionIdParis, ?FILE_REF(FileGuid))),
    ?assertMatch({ok, _}, lfm_proxy:mv(ParisNode, SessionIdParis, ?FILE_REF(FileGuid), MvPath)),

    UserId = oct_background:get_user_id(user1),
    CheckNode = oct_background:get_random_provider_node(?RAND_ELEMENT([krakow, paris])),
    Token = tokens:confine(
        provider_onenv_test_utils:create_oz_temp_access_token(UserId),
        ?CV_PATH([CanonicalMvPath])
    ),
    SessionIdWithCaveat = permissions_test_utils:create_session(CheckNode, UserId, Token),
    ?assertMatch({ok, _}, lfm_proxy:stat(CheckNode, SessionIdWithCaveat, {path, MvPath}), ?ATTEMPTS).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op",
        posthook = fun(NewConfig) ->
            % clean space
            lists:foreach(fun(SpaceSelector) ->
                {ok, FileEntries} = onenv_file_test_utils:ls(user1, SpaceSelector, 0, 10000),

                lists_utils:pforeach(fun({Guid, _}) ->
                    onenv_file_test_utils:rm_and_sync_file(user1, Guid)
                end, FileEntries)
            end, [space1, space_krk_par_p]),

            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(ls_tests, Config) ->
    NewConfig = lfm_proxy:init(Config, false),
    ls_setup(),
    NewConfig;
init_per_group(misc_tests, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(ls_tests, Config) ->
    lfm_proxy:teardown(Config);
end_per_group(misc_tests, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
