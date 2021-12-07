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
%%% TODO VFS-8738 rewrite permissions test to onenv
%%% @end
%%%-------------------------------------------------------------------
-module(permissions_test_base).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("permissions_test.hrl").
-include("proto/common/handshake_messages.hrl").
-include("storage_files_test_SUITE.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

-export([
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    data_access_caveats_test/1,
    data_access_caveats_ancestors_test/1,
    data_access_caveats_ancestors_test2/1,
    data_access_caveats_cache_test/1,

    mkdir_test/1,
    get_children_test/1,
    get_children_attrs_test/1,
    get_children_details_test/1,
    get_child_attr_test/1,
    mv_dir_test/1,
    rm_dir_test/1,

    create_file_test/1,
    open_for_read_test/1,
    open_for_write_test/1,
    open_for_rdwr_test/1,
    create_and_open_test/1,
    truncate_test/1,
    mv_file_test/1,
    rm_file_test/1,

    get_parent_test/1,
    get_file_path_test/1,
    get_file_guid_test/1,
    get_file_attr_test/1,
    get_file_details_test/1,
    get_file_distribution_test/1,

    set_perms_test/1,
    check_read_perms_test/1,
    check_write_perms_test/1,
    check_rdwr_perms_test/1,

    create_share_test/1,
    remove_share_test/1,
    share_perms_test/1,

    get_acl_test/1,
    set_acl_test/1,
    remove_acl_test/1,

    get_transfer_encoding_test/1,
    set_transfer_encoding_test/1,
    get_cdmi_completion_status_test/1,
    set_cdmi_completion_status_test/1,
    get_mimetype_test/1,
    set_mimetype_test/1,

    get_metadata_test/1,
    set_metadata_test/1,
    remove_metadata_test/1,
    get_xattr_test/1,
    list_xattr_test/1,
    set_xattr_test/1,
    remove_xattr_test/1,

    add_qos_entry_test/1,
    get_qos_entry_test/1,
    remove_qos_entry_test/1,
    get_effective_file_qos_test/1,
    check_qos_fulfillment_test/1,

    permission_cache_test/1,
    multi_provider_permission_cache_test/1,
    expired_session_test/1
]).
% Export for use in rpc
-export([check_perms/3]).

-define(rpcCache(W, Function, Args), rpc:call(W, permissions_cache, Function, Args)).

-define(SCENARIO_NAME, atom_to_binary(?FUNCTION_NAME, utf8)).

-define(ATTEMPTS, 35).

% TODO VFS-7563 add tests concerning datasets

%%%===================================================================
%%% Test functions
%%%===================================================================


data_access_caveats_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    FileOwner = <<"user1">>,
    FileOwnerUserSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(W)}}, Config),

    UserId = <<"user2">>,
    UserRootDir = fslogic_uuid:user_root_dir_guid(UserId),
    Space1RootDir = fslogic_uuid:spaceid_to_space_dir_guid(<<"space1">>),
    Space3RootDir = fslogic_uuid:spaceid_to_space_dir_guid(<<"space3">>),

    ScenarioName = ?SCENARIO_NAME,
    DirName = <<ScenarioName/binary, "1">>,
    DirPath = <<"/space1/", DirName/binary>>,
    {ok, DirGuid} = lfm_proxy:mkdir(W, FileOwnerUserSessId, DirPath),
    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    {ok, ShareId} = lfm_proxy:create_share(W, FileOwnerUserSessId, ?FILE_REF(DirGuid), <<"share">>),
    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),

    DirName2 = <<ScenarioName/binary, "2">>,
    DirPath2 = <<"/space1/", DirName2/binary>>,
    {ok, _DirGuid2} = lfm_proxy:mkdir(W, FileOwnerUserSessId, DirPath2),

    [
        {Path1, ObjectId1, F1, ShareF1},
        {Path2, ObjectId2, F2, ShareF2},
        {Path3, ObjectId3, F3, ShareF3},
        {Path4, ObjectId4, F4, ShareF4},
        {Path5, ObjectId5, F5, ShareF5}
    ] = lists:map(fun(Num) ->
        FileName = <<"file", ($0 + Num)>>,
        FilePath = <<DirPath/binary, "/", FileName/binary>>,
        {ok, FileGuid} = lfm_proxy:create(W, FileOwnerUserSessId, FilePath, 8#777),
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
        ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
        {FilePath, FileObjectId, {FileGuid, FileName}, {ShareFileGuid, FileName}}
    end, lists:seq(1, 5)),

    MainToken = initializer:create_access_token(UserId),

    LsWithConfinedToken = fun(Guid, Caveats) ->
        LsToken = tokens:confine(MainToken, Caveats),
        LsSessId = permissions_test_utils:create_session(W, UserId, LsToken),
        lfm_proxy:get_children(W, LsSessId, ?FILE_REF(Guid), 0, 100)
    end,


    % Whitelisting Dir should result in listing all it's files
    ?assertMatch(
        {ok, [F1, F2, F3, F4, F5]},
        LsWithConfinedToken(DirGuid, #cv_data_path{whitelist = [DirPath]})
    ),
    ?assertMatch(
        {ok, [F1, F2, F3, F4, F5]},
        LsWithConfinedToken(DirGuid, #cv_data_objectid{whitelist = [DirObjectId]})
    ),

    % Nevertheless token confinements have no effects on accessing files via shared guid -
    % all operations are automatically performed with ?GUEST perms
    ?assertMatch(
        {ok, [ShareF1, ShareF2, ShareF3, ShareF4, ShareF5]},
        LsWithConfinedToken(ShareDirGuid, #cv_data_path{whitelist = [DirPath]})
    ),

    % Whitelisting concrete files should result in listing only them
    ?assertMatch(
        {ok, [F1, F3, F5]},
        LsWithConfinedToken(DirGuid, #cv_data_path{whitelist = [Path1, Path3, Path5]})
    ),
    ?assertMatch(
        {ok, [F1, F3, F5]},
        LsWithConfinedToken(DirGuid, #cv_data_objectid{whitelist = [ObjectId1, ObjectId3, ObjectId5]})
    ),

    % Nevertheless token confinements have no effects on accessing files via shared guid -
    % all operations are automatically performed with ?GUEST perms
    ?assertMatch(
        {ok, [ShareF1, ShareF2, ShareF3, ShareF4, ShareF5]},
        LsWithConfinedToken(ShareDirGuid, #cv_data_path{whitelist = [Path1, Path3, Path5]})
    ),

    % Using several caveats should result in listing only their intersection
    ?assertMatch(
        {ok, [F1, F5]},
        LsWithConfinedToken(DirGuid, [
            #cv_data_path{whitelist = [Path1, Path3, Path4, Path5]},
            #cv_data_path{whitelist = [Path1, Path2, Path5]},
            #cv_data_path{whitelist = [Path1, Path5]}
        ])
    ),
    ?assertMatch(
        {ok, [F1, F5]},
        LsWithConfinedToken(DirGuid, [
            #cv_data_objectid{whitelist = [ObjectId1, ObjectId3, ObjectId4, ObjectId5]},
            #cv_data_objectid{whitelist = [ObjectId1, ObjectId2, ObjectId5]},
            #cv_data_objectid{whitelist = [ObjectId1, ObjectId5]}
        ])
    ),
    ?assertMatch(
        {ok, [F1, F5]},
        LsWithConfinedToken(DirGuid, [
            #cv_data_path{whitelist = [Path1, Path3, Path4, Path5]},
            #cv_data_objectid{whitelist = [ObjectId1, ObjectId2, ObjectId5]},
            #cv_data_path{whitelist = [Path1, Path5]}
        ])
    ),
    ?assertMatch(
        {ok, []},
        LsWithConfinedToken(DirGuid, [
            #cv_data_objectid{whitelist = [ObjectId3, ObjectId4]},
            #cv_data_path{whitelist = [Path1, Path5]}
        ])
    ),

    % Nevertheless token confinements have no effects on accessing files via shared guid -
    % all operations are automatically performed with ?GUEST perms
    ?assertMatch(
        {ok, [ShareF1, ShareF2, ShareF3, ShareF4, ShareF5]},
        LsWithConfinedToken(ShareDirGuid, [
            #cv_data_objectid{whitelist = [ObjectId1, ObjectId3, ObjectId4, ObjectId5]},
            #cv_data_objectid{whitelist = [ObjectId1, ObjectId2, ObjectId5]},
            #cv_data_objectid{whitelist = [ObjectId1, ObjectId5]}
        ])
    ),

    % Children of dir being listed that don't exist should be omitted from results
    ?assertMatch(
        {ok, [F1, F5]},
        LsWithConfinedToken(DirGuid, [
            #cv_data_path{whitelist = [Path1, <<DirPath/binary, "/i_do_not_exist">>, Path5]}
        ])
    ),

    % Using caveat for different directory should result in {error, eacces}
    ?assertMatch(
        {error, ?EACCES},
        LsWithConfinedToken(DirGuid, #cv_data_path{whitelist = [<<"/space1/qwe">>]})
    ),

    % Nevertheless token confinements have no effects on accessing files via shared guid -
    % all operations are automatically performed with ?GUEST perms
    ?assertMatch(
        {ok, [ShareF1, ShareF2, ShareF3, ShareF4, ShareF5]},
        LsWithConfinedToken(ShareDirGuid, #cv_data_path{whitelist = [<<"/space1/qwe">>]})
    ),

    % With no caveats listing user root dir should list all user spaces
    ?assertMatch(
        {ok, [{Space1RootDir, <<"space1">>}, {Space3RootDir, <<"space3">>}]},
        LsWithConfinedToken(UserRootDir, [])
    ),
    % But with caveats user root dir ls should show only spaces leading to allowed files
    ?assertMatch(
        {ok, [{Space1RootDir, <<"space1">>}]},
        LsWithConfinedToken(UserRootDir, #cv_data_path{whitelist = [DirPath]})
    ),

    % With no caveats listing space dir should list all space directories
    SessId12 = permissions_test_utils:create_session(W, UserId, MainToken),
    ?assertMatch(
        {ok, [_ | _]},
        lfm_proxy:get_children(W, SessId12, ?FILE_REF(Space1RootDir), 0, 100)
    ),
    % And all operations on it and it's children should be allowed
    ?assertMatch(
        {ok, _},
        lfm_proxy:get_acl(W, SessId12, ?FILE_REF(Space1RootDir))
    ),
    ?assertMatch(
        {ok, _},
        lfm_proxy:get_acl(W, SessId12, ?FILE_REF(DirGuid))
    ),
    % But with caveats space ls should show only dirs leading to allowed files.
    Token13 = tokens:confine(MainToken, #cv_data_path{whitelist = [Path1]}),
    SessId13 = permissions_test_utils:create_session(W, UserId, Token13),
    ?assertMatch(
        {ok, [{DirGuid, DirName}]},
        lfm_proxy:get_children(W, SessId13, ?FILE_REF(Space1RootDir), 0, 100)
    ),
    % On such dirs (ancestor) it should be possible to perform only certain
    % operations like ls, stat, resolve_guid, get_parent and resolve_path.
    ?assertMatch(
        {ok, [F1]},
        lfm_proxy:get_children(W, SessId13, ?FILE_REF(DirGuid), 0, 100)
    ),
    ?assertMatch(
        {ok, #file_attr{name = DirName, type = ?DIRECTORY_TYPE}},
        lfm_proxy:stat(W, SessId13, ?FILE_REF(DirGuid))
    ),
    ?assertMatch(
        {ok, DirGuid},
        lfm_proxy:resolve_guid(W, SessId13, DirPath)
    ),
    ?assertMatch(
        {ok, Space1RootDir},
        lfm_proxy:get_parent(W, SessId13, ?FILE_REF(DirGuid))
    ),
    ?assertMatch(
        {ok, DirPath},
        lfm_proxy:get_file_path(W, SessId13, DirGuid)
    ),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:get_acl(W, SessId13, ?FILE_REF(DirGuid))
    ),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:create(W, SessId13, DirGuid, <<"file1">>, 8#777)
    ),

    % Test listing with caveats and options (offset, limit)
    Token14 = tokens:confine(MainToken, #cv_data_path{whitelist = [
        Path1, Path2, <<DirPath/binary, "/i_do_not_exist">>, Path4, Path5
    ]}),
    SessId14 = permissions_test_utils:create_session(W, UserId, Token14),
    ?assertMatch(
        {ok, [F1, F2, F4]},
        lfm_proxy:get_children(W, SessId14, ?FILE_REF(DirGuid), 0, 3)
    ),
    ?assertMatch(
        {ok, [F4, F5]},
        lfm_proxy:get_children(W, SessId14, ?FILE_REF(DirGuid), 2, 3)
    ),
    ?assertMatch(
        {ok, [F4]},
        lfm_proxy:get_children(W, SessId14, ?FILE_REF(DirGuid), 2, 1)
    ).


data_access_caveats_ancestors_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    UserId = <<"user3">>,
    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(W)}}, Config),
    UserRootDir = fslogic_uuid:user_root_dir_guid(UserId),

    SpaceName = <<"space2">>,
    SpaceRootDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceName),

    InaccessibleFileName = <<"inaccessible_file">>,

    Dirs0 = [{LastDirGuid, _} | _] = lists:foldl(fun(Num, [{DirGuid, _} | _] = Acc) ->
        SubDirName = <<"dir", (integer_to_binary(Num))/binary>>,
        {ok, SubDirGuid} = lfm_proxy:mkdir(W, UserSessId, DirGuid, SubDirName, 8#777),
        {ok, _} = lfm_proxy:create(W, UserSessId, DirGuid, InaccessibleFileName, 8#777),
        [{SubDirGuid, SubDirName} | Acc]
    end, [{SpaceRootDirGuid, SpaceName}], lists:seq(1, 20)),
    Dirs1 = lists:reverse(Dirs0),

    FileInDeepestDirName = <<"file">>,
    {ok, FileInDeepestDirGuid} = lfm_proxy:create(W, UserSessId, LastDirGuid, FileInDeepestDirName, 8#777),
    {ok, FileInDeepestDirObjectId} = file_id:guid_to_objectid(FileInDeepestDirGuid),

    Token = initializer:create_access_token(UserId, [
        #cv_data_objectid{whitelist = [FileInDeepestDirObjectId]}
    ]),
    SessId = permissions_test_utils:create_session(W, UserId, Token),

    lists:foldl(
        fun({{DirGuid, DirName}, Child}, {ParentPath, ParentGuid}) ->
            DirPath = case ParentPath of
                <<>> ->
                    <<"/">>;
                <<"/">> ->
                    <<"/", DirName/binary>>;
                _ ->
                    <<ParentPath/binary, "/", DirName/binary>>
            end,

            % Most operations should be forbidden to perform on dirs/ancestors
            % leading to files allowed by caveats
            ?assertMatch(
                {error, ?EACCES},
                lfm_proxy:get_acl(W, SessId, ?FILE_REF(DirGuid))
            ),

            % Below operations should succeed for every dir/ancestor leading
            % to file allowed by caveats
            ?assertMatch(
                {ok, [Child]},
                lfm_proxy:get_children(W, SessId, ?FILE_REF(DirGuid), 0, 100)
            ),
            ?assertMatch(
                {ok, #file_attr{name = DirName, type = ?DIRECTORY_TYPE}},
                lfm_proxy:stat(W, SessId, ?FILE_REF(DirGuid))
            ),
            ?assertMatch(
                {ok, DirGuid},
                lfm_proxy:resolve_guid(W, SessId, DirPath)
            ),
            ?assertMatch(
                {ok, ParentGuid},
                lfm_proxy:get_parent(W, SessId, ?FILE_REF(DirGuid))
            ),
            ?assertMatch(
                {ok, DirPath},
                lfm_proxy:get_file_path(W, SessId, DirGuid)
            ),

            % Get child attr should also succeed but only for children that are
            % also allowed by caveats
            case ParentGuid of
                undefined ->
                    ok;
                _ ->
                    ?assertMatch(
                        {ok, #file_attr{name = DirName, type = ?DIRECTORY_TYPE}},
                        lfm_proxy:get_child_attr(W, SessId, ParentGuid, DirName)
                    ),
                    ?assertMatch(
                        {error, ?ENOENT},
                        lfm_proxy:get_child_attr(W, SessId, ParentGuid, InaccessibleFileName)
                    )
            end,

            {DirPath, DirGuid}
        end,
        {<<>>, undefined},
        lists:zip([{UserRootDir, UserId} | Dirs1], Dirs1 ++ [{FileInDeepestDirGuid, FileInDeepestDirName}])
    ),

    % Get acl should finally succeed for file which is allowed by caveats
    ?assertMatch(
        {ok, []},
        lfm_proxy:get_acl(W, SessId, ?FILE_REF(FileInDeepestDirGuid))
    ).


data_access_caveats_ancestors_test2(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    UserId = <<"user3">>,
    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(W)}}, Config),
    UserRootDir = fslogic_uuid:user_root_dir_guid(UserId),

    SpaceName = <<"space2">>,
    SpaceRootDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceName),

    RootDirName = ?SCENARIO_NAME,
    {ok, RootDirGuid} = lfm_proxy:mkdir(W, UserSessId, SpaceRootDirGuid, RootDirName, 8#777),

    CentralDirName = <<"central">>,
    {ok, _} = lfm_proxy:mkdir(W, UserSessId, RootDirGuid, CentralDirName, 8#777),

    [
        {RightDirGuid, RightDirName, RightFileObjectId, RightFile},
        {LeftDirGuid, LeftDirName, LeftFileObjectId, LeftFile}
    ] = lists:map(fun(DirName) ->
        {ok, DirGuid} = lfm_proxy:mkdir(W, UserSessId, RootDirGuid, DirName, 8#777),
        [{FileObjectId, File} | _] = lists:map(fun(Num) ->
            FileName = <<"file", ($0 + Num)>>,
            {ok, FileGuid} = lfm_proxy:create(W, UserSessId, DirGuid, FileName, 8#777),
            {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
            {FileObjectId, {FileGuid, FileName}}
        end, lists:seq(1, 5)),
        {DirGuid, DirName, FileObjectId, File}
    end, [<<"right">>, <<"left">>]),

    MainToken = initializer:create_access_token(UserId),

    % All dirs leading to files allowed by caveat should be listed in ls
    Token1 = tokens:confine(MainToken, #cv_data_objectid{
        whitelist = [LeftFileObjectId, RightFileObjectId]
    }),
    SessId1 = permissions_test_utils:create_session(W, UserId, Token1),
    ?assertMatch(
        {ok, [{SpaceRootDirGuid, SpaceName}]},
        lfm_proxy:get_children(W, SessId1, ?FILE_REF(UserRootDir), 0, 100)
    ),
    ?assertMatch(
        {ok, [{RootDirGuid, RootDirName}]},
        lfm_proxy:get_children(W, SessId1, ?FILE_REF(SpaceRootDirGuid), 0, 100)
    ),
    ?assertMatch(
        {ok, [{LeftDirGuid, LeftDirName}, {RightDirGuid, RightDirName}]},
        lfm_proxy:get_children(W, SessId1, ?FILE_REF(RootDirGuid), 0, 100)
    ),
    ?assertMatch(
        {ok, [LeftFile]},
        lfm_proxy:get_children(W, SessId1, ?FILE_REF(LeftDirGuid), 0, 100)
    ),
    ?assertMatch(
        {ok, [RightFile]},
        lfm_proxy:get_children(W, SessId1, ?FILE_REF(RightDirGuid), 0, 100)
    ),

    % When caveats have empty intersection then ls should return []
    Token2 = tokens:confine(MainToken, [
        #cv_data_objectid{whitelist = [LeftFileObjectId]},
        #cv_data_objectid{whitelist = [RightFileObjectId]}
    ]),
    SessId2 = permissions_test_utils:create_session(W, UserId, Token2),
    ?assertMatch(
        {ok, [{SpaceRootDirGuid, SpaceName}]},
        lfm_proxy:get_children(W, SessId2, ?FILE_REF(UserRootDir), 0, 100)
    ),
    ?assertMatch(
        {ok, [{RootDirGuid, RootDirName}]},
        lfm_proxy:get_children(W, SessId2, ?FILE_REF(SpaceRootDirGuid), 0, 100)
    ),
    ?assertMatch(
        {ok, []},
        lfm_proxy:get_children(W, SessId2, ?FILE_REF(RootDirGuid), 0, 100)
    ).


data_access_caveats_cache_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    UserId = <<"user3">>,
    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(W)}}, Config),
    UserRootDir = fslogic_uuid:user_root_dir_guid(UserId),

    SpaceName = <<"space2">>,
    SpaceRootDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceName),

    RootDirName = ?SCENARIO_NAME,
    {ok, RootDirGuid} = lfm_proxy:mkdir(W, UserSessId, SpaceRootDirGuid, RootDirName, 8#777),

    {ok, DirGuid} = lfm_proxy:mkdir(W, UserSessId, RootDirGuid, <<"dir">>, 8#777),
    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    {ok, FileGuid} = lfm_proxy:create(W, UserSessId, DirGuid, <<"file">>, 8#777),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    Token = initializer:create_access_token(UserId, [
        #cv_data_objectid{whitelist = [DirObjectId]},
        #cv_data_objectid{whitelist = [FileObjectId]}
    ]),
    SessId = permissions_test_utils:create_session(W, UserId, Token),


    %% CHECK guid_constraint CACHE

    % before any call cache should be empty
    lists:foreach(fun(Guid) ->
        ?assertEqual(
            calculate,
            ?rpcCache(W, check_permission, [{guid_constraint, Token, Guid}])
        )
    end, [UserRootDir, SpaceRootDirGuid, RootDirGuid, DirGuid, FileGuid]),

    % call on file should fill cache up to root dir with remaining guid constraints
    ?assertMatch(
        {ok, []},
        lfm_proxy:get_acl(W, SessId, ?FILE_REF(FileGuid))
    ),
    lists:foreach(fun(Guid) ->
        ?assertEqual(
            {ok, {false, [[FileGuid], [DirGuid]]}},
            ?rpcCache(W, check_permission, [{guid_constraint, Token, Guid}])
        )
    end, [UserRootDir, SpaceRootDirGuid, RootDirGuid]),
    ?assertEqual(
        {ok, {false, [[FileGuid]]}},
        ?rpcCache(W, check_permission, [{guid_constraint, Token, DirGuid}])
    ),
    ?assertEqual(
        {ok, true},
        ?rpcCache(W, check_permission, [{guid_constraint, Token, FileGuid}])
    ),


    %% CHECK data_constraint CACHE

    % data_constraint cache is not filed recursively as guid_constraint one is
    % so only for file should it be filled
    lists:foreach(fun(Guid) ->
        ?assertEqual(
            calculate,
            ?rpcCache(W, check_permission, [{data_constraint, Token, Guid}])
        )
    end, [UserRootDir, SpaceRootDirGuid, RootDirGuid, DirGuid]),

    ?assertEqual(
        {ok, equal_or_descendant},
        ?rpcCache(W, check_permission, [{data_constraint, Token, FileGuid}])
    ),

    % calling on dir any function reserved only for equal_or_descendant should
    % cache {equal_or_descendant, ?EACCES} meaning that no such operation can be
    % performed but since ancestor checks were not performed it is not known whether
    % ancestor operations can be performed
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:get_acl(W, SessId, ?FILE_REF(DirGuid))
    ),
    ?assertEqual(
        {ok, {equal_or_descendant, ?EACCES}},
        ?rpcCache(W, check_permission, [{data_constraint, Token, DirGuid}])
    ),

    % after calling operation possible to perform on ancestor cached value should
    % be changed to signal that file is ancestor and such operations can be performed
    ?assertMatch(
        {ok, [_]},
        lfm_proxy:get_children(W, SessId, ?FILE_REF(DirGuid), 0, 100)
    ),
    ?assertEqual(
        {ok, {ancestor, [<<"file">>]}},
        ?rpcCache(W, check_permission, [{data_constraint, Token, DirGuid}])
    ),

    % Calling ancestor operation on unrelated to file in caveats dir all checks
    % will be performed and just ?EACESS will be cached meaning that no operation
    % on this dir can be performed
    {ok, OtherDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        W, UserSessId, RootDirGuid, <<"other_dir">>, 8#777
    )),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:get_children(W, SessId, ?FILE_REF(OtherDirGuid), 0, 100)
    ),
    ?assertEqual(
        {ok, ?EACCES},
        ?rpcCache(W, check_permission, [{data_constraint, Token, OtherDirGuid}])
    ).


mkdir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_subcontainer]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            case lfm_proxy:mkdir(W, SessId, ParentDirGuid, <<"dir2">>, 8#777) of
                {ok, DirGuid} ->
                    permissions_test_utils:ensure_dir_created_on_storage(W, DirGuid);
                {error, _} = Error ->
                    Error
            end
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_change_ownership, <<TestCaseRootDirPath/binary, "/dir1/dir2">>}
        end
    }, Config).


get_children_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?list_container]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(lfm_proxy:get_children(W, SessId, DirKey, 0, 100))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }, Config).


get_children_attrs_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?list_container]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(lfm_proxy:get_children_attrs(W, SessId, DirKey, #{offset => 0, size => 100}))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }, Config).


get_children_details_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?list_container]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(lfm_proxy:get_children_details(W, SessId, DirKey, #{offset => 0, size => 100}))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }, Config).


get_child_attr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container],
            children = [#file{name = <<"file1">>}]
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            extract_ok(lfm_proxy:get_child_attr(W, SessId, ParentDirGuid, <<"file1">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1/file1">>}
        end
    }, Config).


mv_dir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [
            #dir{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_subcontainer],
                children = [
                    #dir{
                        name = <<"dir11">>,
                        perms = [?delete]
                    }
                ]
            },
            #dir{
                name = <<"dir2">>,
                perms = [?traverse_container, ?add_subcontainer]
            }
        ],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            SrcDirPath = <<TestCaseRootDirPath/binary, "/dir1/dir11">>,
            SrcDirKey = maps:get(SrcDirPath, ExtraData),
            DstDirPath = <<TestCaseRootDirPath/binary, "/dir2">>,
            DstDirKey = maps:get(DstDirPath, ExtraData),
            extract_ok(lfm_proxy:mv(W, SessId, SrcDirKey, DstDirKey, <<"dir21">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir2/dir21">>}
        end
    }, Config).


rm_dir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [
            #dir{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_subcontainer],
                children = [
                    #dir{
                        name = <<"dir2">>,
                        perms = [?delete, ?list_container]
                    }
                ]
            }
        ],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1/dir2">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(lfm_proxy:unlink(W, SessId, DirKey))
        end
    }, Config).


create_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            case lfm_proxy:create(W, SessId, ParentDirGuid, <<"file1">>, 8#777) of
                {ok, FileGuid} ->
                    permissions_test_utils:ensure_file_created_on_storage(W, FileGuid);
                {error, _} = Error ->
                    Error
            end
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_change_ownership, <<TestCaseRootDirPath/binary, "/dir1/file1">>}
        end
    }, Config).


open_for_read_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object],
            on_create = fun(FileOwnerSessId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(W, FileOwnerSessId, Guid),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:open(W, SessId, FileKey, read))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


open_for_write_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_object],
            on_create = fun(FileOwnerSessId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(W, FileOwnerSessId, Guid),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:open(W, SessId, FileKey, write))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


open_for_rdwr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object, ?write_object],
            on_create = fun(FileOwnerSessId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(W, FileOwnerSessId, Guid),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:open(W, SessId, FileKey, rdwr))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


create_and_open_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_object],
            on_create = fun(FileOwnerSessId, Guid) ->
                % Create dummy file to ensure that directory is created on storage.
                % Otherwise it may be not possible during tests without necessary perms.
                create_dummy_file(W, FileOwnerSessId, Guid),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ?FILE_REF(ParentDirGuid) = maps:get(ParentDirPath, ExtraData),
            extract_ok(lfm_proxy:create_and_open(W, SessId, ParentDirGuid, <<"file1">>, 8#777))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_change_ownership, <<TestCaseRootDirPath/binary, "/dir1/file1">>}
        end
    }, Config).


truncate_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:truncate(W, SessId, FileKey, 0))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


mv_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [
            #dir{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_object],
                children = [
                    #file{
                        name = <<"file11">>,
                        perms = [?delete]
                    }
                ]
            },
            #dir{
                name = <<"dir2">>,
                perms = [?traverse_container, ?add_object]
            }
        ],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            SrcFilePath = <<TestCaseRootDirPath/binary, "/dir1/file11">>,
            SrcFileKey = maps:get(SrcFilePath, ExtraData),
            DstDirPath = <<TestCaseRootDirPath/binary, "/dir2">>,
            DstDirKey = maps:get(DstDirPath, ExtraData),
            extract_ok(lfm_proxy:mv(W, SessId, SrcFileKey, DstDirKey, <<"file21">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir2/file21">>}
        end
    }, Config).


rm_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [
            #dir{
                name = <<"dir1">>,
                perms = [?traverse_container, ?delete_object],
                children = [
                    #file{
                        name = <<"file1">>,
                        perms = [?delete]
                    }
                ]
            }
        ],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/dir1/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:unlink(W, SessId, FileKey))
        end
    }, Config).


get_parent_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_parent(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_file_path_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = false, % TODO VFS-6057
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            ?FILE_REF(FileGuid) = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_file_path(W, SessId, FileGuid))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_file_guid_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        operation = fun(SessId, TestCaseRootDirPath, _ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            extract_ok(lfm_proxy:resolve_guid(W, SessId, FilePath))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_file_attr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:stat(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_file_details_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_details(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_file_distribution_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_file_distribution(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    FileOwner = <<"user1">>,

    FileOwnerUserSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(W)}}, Config),
    GroupUserSessId = ?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config),
    OtherUserSessId = ?config({session_id, {<<"user3">>, ?GET_DOMAIN(W)}}, Config),
    SpaceOwnerSessId = ?config({session_id, {<<"owner">>, ?GET_DOMAIN(W)}}, Config),

    DirPath = <<"/space1/dir1">>,
    {ok, DirGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:mkdir(W, FileOwnerUserSessId, DirPath)
    ),

    FilePath = <<"/space1/dir1/file1">>,
    {ok, FileGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:create(W, FileOwnerUserSessId, FilePath, 8#777)
    ),
    {ok, ShareId} = ?assertMatch({ok, _}, lfm_proxy:create_share(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), <<"share">>)),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    % Open file to ensure it's creation on storage
    {ok, Handle} = lfm_proxy:open(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), write),
    ok = lfm_proxy:close(W, Handle),

    AssertProperStorageAttrsFun = fun(ExpMode) ->
        permissions_test_utils:assert_user_is_file_owner_on_storage(
            W, ?SPACE_ID, FilePath, FileOwnerUserSessId, #{mode => ?FILE_MODE(ExpMode)}
        )
    end,

    AssertProperStorageAttrsFun(8#777),

    %% POSIX

    % file owner can always change file perms if he has access to it
    permissions_test_utils:set_modes(W, #{DirGuid => 8#677, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), 8#000)
    ),
    permissions_test_utils:set_modes(W, #{DirGuid => 8#100, FileGuid => 8#000}),
    ?assertMatch(ok, lfm_proxy:set_perms(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), 8#000)),
    AssertProperStorageAttrsFun(8#000),

    % but not if that access is via shared guid
    permissions_test_utils:set_modes(W, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EPERM},
        lfm_proxy:set_perms(W, FileOwnerUserSessId, ?FILE_REF(ShareFileGuid), 8#000)
    ),
    AssertProperStorageAttrsFun(8#777),

    % other users from space can't change perms no matter what
    permissions_test_utils:set_modes(W, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, GroupUserSessId, ?FILE_REF(FileGuid), 8#000)
    ),
    AssertProperStorageAttrsFun(8#777),

    % with exception being space owner who can always change perms no matter what
    permissions_test_utils:set_modes(W, #{DirGuid => 8#000, FileGuid => 8#000}),
    ?assertMatch(ok, lfm_proxy:set_perms(W, SpaceOwnerSessId, ?FILE_REF(FileGuid), 8#555)),
    AssertProperStorageAttrsFun(8#555),

    % but even space owner cannot perform write operation on space dir
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(<<"space1">>),
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(W, SpaceOwnerSessId, ?FILE_REF(SpaceGuid), 8#000)),
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(W, SpaceOwnerSessId, ?FILE_REF(SpaceGuid), 8#555)),
    ?assertMatch({error, ?EPERM}, lfm_proxy:set_perms(W, SpaceOwnerSessId, ?FILE_REF(SpaceGuid), 8#777)),

    % users outside of space shouldn't even see the file
    permissions_test_utils:set_modes(W, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?ENOENT},
        lfm_proxy:set_perms(W, OtherUserSessId, ?FILE_REF(FileGuid), 8#000)
    ),
    AssertProperStorageAttrsFun(8#777),

    %% ACL

    % file owner can always change file perms if he has access to it
    permissions_test_utils:set_acls(W, #{
        DirGuid => ?ALL_DIR_PERMS -- [?traverse_container],
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), 8#000)
    ),

    permissions_test_utils:set_acls(W, #{
        DirGuid => [?traverse_container],
        FileGuid => []
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(ok, lfm_proxy:set_perms(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), 8#000)),

    % but not if that access is via shared guid
    ?assertMatch(
        {error, ?EPERM},
        lfm_proxy:set_perms(W, FileOwnerUserSessId, ?FILE_REF(ShareFileGuid), 8#000)
    ),

    % file owner cannot change acl after his access was denied by said acl
    permissions_test_utils:set_acls(W, #{}, #{
        FileGuid => ?ALL_FILE_PERMS
    }, ?everyone, ?no_flags_mask),

    PermsBitmask = permissions_test_utils:perms_to_bitmask(?ALL_FILE_PERMS),

    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_acl(W, FileOwnerUserSessId, ?FILE_REF(FileGuid), [
            ?ALLOW_ACE(?owner, ?no_flags_mask, PermsBitmask)
        ])
    ),

    % but space owner always can change acl for any file
    ?assertMatch(
        ok,
        lfm_proxy:set_acl(W, SpaceOwnerSessId, ?FILE_REF(FileGuid), [
            ?ALLOW_ACE(?owner, ?no_flags_mask, PermsBitmask)
        ])
    ),

    % other users from space can't change perms no matter what
    permissions_test_utils:set_acls(W, #{
        DirGuid => ?ALL_DIR_PERMS,
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, GroupUserSessId, ?FILE_REF(FileGuid), 8#000)
    ),

    % users outside of space shouldn't even see the file
    permissions_test_utils:set_acls(W, #{
        DirGuid => ?ALL_DIR_PERMS,
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?ENOENT},
        lfm_proxy:set_perms(W, OtherUserSessId, ?FILE_REF(FileGuid), 8#000)
    ).


check_read_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:check_perms(W, SessId, FileKey, read))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


check_write_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:check_perms(W, SessId, FileKey, write))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


check_rdwr_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object, ?write_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:check_perms(W, SessId, FileKey, rdwr))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


create_share_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{name = <<"dir1">>}],
        posix_requires_space_privs = [?SPACE_MANAGE_SHARES],
        acl_requires_space_privs = [?SPACE_MANAGE_SHARES],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            extract_ok(lfm_proxy:create_share(W, SessId, DirKey, <<"create_share">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }, Config).


remove_share_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            on_create = fun(FileOwnerSessId, Guid) ->
                {ok, ShareId} = ?assertMatch({ok, _}, lfm_proxy:create_share(
                    W, FileOwnerSessId, ?FILE_REF(Guid), <<"share_to_remove">>
                )),
                ShareId
            end
        }],
        posix_requires_space_privs = [?SPACE_MANAGE_SHARES],
        acl_requires_space_privs = [?SPACE_MANAGE_SHARES],
        available_in_readonly_mode = false,
        available_in_share_mode = inapplicable,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ShareId = maps:get(DirPath, ExtraData),
            extract_ok(lfm_proxy:remove_share(W, SessId, ShareId))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/dir1">>}
        end
    }, Config).


share_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    FileOwner = <<"user1">>,

    FileOwnerUserSessId = ?config({session_id, {FileOwner, ?GET_DOMAIN(W)}}, Config),
    GroupUserSessId = ?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config),

    ScenarioDirName = ?SCENARIO_NAME,
    ScenarioDirPath = <<"/space1/", ScenarioDirName/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, FileOwnerUserSessId, ScenarioDirPath, 8#700)),

    MiddleDirPath = <<ScenarioDirPath/binary, "/dir2">>,
    {ok, MiddleDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, FileOwnerUserSessId, MiddleDirPath, 8#777)),

    BottomDirPath = <<MiddleDirPath/binary, "/dir3">>,
    {ok, BottomDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, FileOwnerUserSessId, BottomDirPath), 8#777),

    FilePath = <<BottomDirPath/binary, "/file1">>,
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, FileOwnerUserSessId, FilePath, 8#777)),

    {ok, ShareId} = ?assertMatch({ok, _}, lfm_proxy:create_share(W, FileOwnerUserSessId, ?FILE_REF(MiddleDirGuid), <<"share">>)),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    % Accessing file in normal mode by space user should result in eacces (dir1 perms -> 8#700)
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:stat(W, GroupUserSessId, ?FILE_REF(FileGuid))
    ),
    % But accessing it in share mode should succeed as perms should be checked only up to
    % share root (dir1/dir2 -> 8#777) and not space root
    ?assertMatch(
        {ok, #file_attr{guid = ShareFileGuid}},
        lfm_proxy:stat(W, GroupUserSessId, ?FILE_REF(ShareFileGuid))
    ),

    % Changing BottomDir mode to 8#770 should forbid access to file in share mode
    ?assertEqual(ok, lfm_proxy:set_perms(W, ?ROOT_SESS_ID, ?FILE_REF(BottomDirGuid), 8#770)),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:stat(W, GroupUserSessId, ?FILE_REF(ShareFileGuid))
    ).


get_acl_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_acl]
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_acl(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_acl_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_acl]
        }],
        posix_requires_space_privs = {file_owner, [?SPACE_WRITE_DATA]},
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:set_acl(W, SessId, FileKey, [
                ?ALLOW_ACE(
                    ?group,
                    ?no_flags_mask,
                    permissions_test_utils:perms_to_bitmask(?ALL_FILE_PERMS)
                )
            ]))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


remove_acl_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_acl]
        }],
        posix_requires_space_privs = {file_owner, [?SPACE_WRITE_DATA]},
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:remove_acl(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_transfer_encoding_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(FileOwnerSessId, Guid) ->
                lfm_proxy:set_transfer_encoding(W, FileOwnerSessId, ?FILE_REF(Guid), <<"base64">>),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_transfer_encoding(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_transfer_encoding_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:set_transfer_encoding(W, SessId, FileKey, <<"base64">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_cdmi_completion_status_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(FileOwnerSessId, Guid) ->
                lfm_proxy:set_cdmi_completion_status(W, FileOwnerSessId, ?FILE_REF(Guid), <<"Completed">>),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_cdmi_completion_status(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_cdmi_completion_status_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:set_cdmi_completion_status(W, SessId, FileKey, <<"Completed">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_mimetype_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(FileOwnerSessId, Guid) ->
                lfm_proxy:set_mimetype(W, FileOwnerSessId, ?FILE_REF(Guid), <<"mimetype">>),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_mimetype(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_mimetype_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:set_mimetype(W, SessId, FileKey, <<"mimetype">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_metadata_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(FileOwnerSessId, Guid) ->
                lfm_proxy:set_metadata(W, FileOwnerSessId, ?FILE_REF(Guid), json, <<"VAL">>, []),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_metadata(W, SessId, FileKey, json, [], false))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_metadata_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:set_metadata(W, SessId, FileKey, json, <<"VAL">>, []))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


remove_metadata_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata],
            on_create = fun(FileOwnerSessId, Guid) ->
                lfm_proxy:set_metadata(W, FileOwnerSessId, ?FILE_REF(Guid), json, <<"VAL">>, []),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:remove_metadata(W, SessId, FileKey, json))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_xattr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(FileOwnerSessId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(W, FileOwnerSessId, ?FILE_REF(Guid), Xattr),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_xattr(W, SessId, FileKey, <<"myxattr">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


list_xattr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            on_create = fun(FileOwnerSessId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(W, FileOwnerSessId, ?FILE_REF(Guid), Xattr),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:list_xattr(W, SessId, FileKey, false, false))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


set_xattr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:set_xattr(W, SessId, FileKey, #xattr{
                name = <<"myxattr">>, value = <<"VAL">>
            }))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


remove_xattr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata],
            on_create = fun(FileOwnerSessId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(W, FileOwnerSessId, ?FILE_REF(Guid), Xattr),
                ?FILE_REF(Guid)
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:remove_xattr(W, SessId, FileKey, <<"myxattr">>))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


add_qos_entry_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:add_qos_entry(W, SessId, FileKey, <<"country=FR">>, 1))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_qos_entry_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            on_create = fun(FileOwnerSessId, Guid) ->
                {ok, QosEntryId} = lfm_proxy:add_qos_entry(
                    W, FileOwnerSessId, ?FILE_REF(Guid), <<"country=FR">>, 1
                ),
                QosEntryId
            end 
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_qos_entry(W, SessId, QosEntryId))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


remove_qos_entry_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            on_create = fun(FileOwnerSessId, Guid) ->
                {ok, QosEntryId} = lfm_proxy:add_qos_entry(
                    W, FileOwnerSessId, ?FILE_REF(Guid), <<"country=FR">>, 1
                ),
                QosEntryId
            end
        }],
        available_in_readonly_mode = false,
        available_in_share_mode = inapplicable,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:remove_qos_entry(W, SessId, QosEntryId))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


get_effective_file_qos_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            on_create = fun(FileOwnerSessId, Guid) ->
                {ok, _QosEntryId} = lfm_proxy:add_qos_entry(
                    W, FileOwnerSessId, ?FILE_REF(Guid), <<"country=FR">>, 1
                ),
                ?FILE_REF(Guid)
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:get_effective_file_qos(W, SessId, FileKey))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


check_qos_fulfillment_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    permissions_test_runner:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir_name = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            on_create = fun(FileOwnerSessId, Guid) ->
                {ok, QosEntryId} = lfm_proxy:add_qos_entry(
                    W, FileOwnerSessId, ?FILE_REF(Guid), <<"country=FR">>, 1
                ),
                QosEntryId
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        operation = fun(SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            extract_ok(lfm_proxy:check_qos_status(W, SessId, QosEntryId))
        end,
        final_ownership_check = fun(TestCaseRootDirPath) ->
            {should_preserve_ownership, <<TestCaseRootDirPath/binary, "/file1">>}
        end
    }, Config).


permission_cache_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    PermissionCacheStatusUuid = <<"status">>,

    case ?rpcCache(W, get, [PermissionCacheStatusUuid]) of
        {ok, #document{value = #permissions_cache{value = {permissions_cache_helper2, _}}}} ->
            ?assertEqual(ok, ?rpcCache(W, invalidate, [])),
            ?assertMatch({ok, #document{value = #permissions_cache{value = {permissions_cache_helper, permissions_cache_helper2}}}},
                ?rpcCache(W, get, [PermissionCacheStatusUuid]), 3);
        _ ->
            ok
    end,

    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p1])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p2])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p3])),

    ?assertMatch({ok, _}, ?rpcCache(W, cache_permission, [p1, ok])),
    ?assertEqual({ok, ok}, ?rpcCache(W, check_permission, [p1])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p2])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p3])),

    ?assertEqual(ok, ?rpcCache(W, invalidate, [])),
    ?assertMatch({ok, #document{value = #permissions_cache{value = {permissions_cache_helper2, _}}}},
        ?rpcCache(W, get, [PermissionCacheStatusUuid])),
    ?assertMatch({ok, _}, ?rpcCache(W, cache_permission, [p2, ok])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p1])),
    ?assertEqual({ok, ok}, ?rpcCache(W, check_permission, [p2])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p3])),

    ?assertMatch({ok, #document{value = #permissions_cache{value = {permissions_cache_helper2, permissions_cache_helper}}}},
        ?rpcCache(W, get, [PermissionCacheStatusUuid]), 2),
    ?assertEqual(ok, ?rpcCache(W, invalidate, [])),
    ?assertMatch({ok, #document{value = #permissions_cache{value = {permissions_cache_helper, _}}}},
        ?rpcCache(W, get, [PermissionCacheStatusUuid]), 2),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p1])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p2])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p3])),

    for(50, fun() -> ?assertEqual(ok, ?rpcCache(W, invalidate, [])) end),
    CheckFun = fun() ->
        case ?rpcCache(W, get, [PermissionCacheStatusUuid]) of
            {ok, #document{value = #permissions_cache{value = {permissions_cache_helper, permissions_cache_helper2}}}} ->
                ok;
            {ok, #document{value = #permissions_cache{value = {permissions_cache_helper2, permissions_cache_helper}}}} ->
                ok;
            Other ->
                Other
        end
    end,
    ?assertMatch(ok, CheckFun(), 10),
    ?assertMatch({ok, _}, ?rpcCache(W, cache_permission, [p1, xyz])),
    ?assertMatch({ok, _}, ?rpcCache(W, cache_permission, [p3, ok])),
    ?assertEqual({ok, xyz}, ?rpcCache(W, check_permission, [p1])),
    ?assertEqual(calculate, ?rpcCache(W, check_permission, [p2])),
    ?assertEqual({ok, ok}, ?rpcCache(W, check_permission, [p3])).


multi_provider_permission_cache_test(Config) ->
    [P2, P1W2, P1W1] = ?config(op_worker_nodes, Config),
    Nodes = [P1W2, P1W1, P2],

    User = <<"user1">>,

    Path = <<"/space1/multi_provider_permission_cache_test">>,
    P1W2SessId = ?config({session_id, {User, ?GET_DOMAIN(P1W2)}}, Config),

    {Guid, AllPerms} = case rand:uniform(2) of
        1 ->
            {_, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(P1W2, P1W2SessId, Path, 8#777)),
            {FileGuid, ?ALL_FILE_PERMS};
        2 ->
            {_, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(P1W2, P1W2SessId, Path, 8#777)),
            {DirGuid, ?ALL_DIR_PERMS}
    end,

    % Set random posix permissions for file/dir and assert they are properly propagated to other
    % nodes/providers (that includes permissions cache - obsolete entries should be overridden)
    lists:foreach(fun(_IterationNum) ->
        PosixPerms = lists_utils:random_sublist(?ALL_POSIX_PERMS),
        Mode = lists:foldl(fun(Perm, Acc) ->
            Acc bor permissions_test_utils:posix_perm_to_mode(Perm, owner)
        end, 0, PosixPerms),
        permissions_test_utils:set_modes(P1W2, #{Guid => Mode}),

        {AllowedPerms, DeniedPerms} = lists:foldl(fun(Perm, {AllowedPermsAcc, DeniedPermsAcc}) ->
            case permissions_test_utils:perm_to_posix_perms(Perm) -- [owner, owner_if_parent_sticky | PosixPerms] of
                [] -> {[Perm | AllowedPermsAcc], DeniedPermsAcc};
                _ -> {AllowedPermsAcc, [Perm | DeniedPermsAcc]}
            end
        end, {[], []}, AllPerms),

        run_multi_provider_perm_test(
            Nodes, User, Guid, PosixPerms, DeniedPerms,
            {error, ?EACCES}, <<"denied posix perm">>, Config
        ),
        run_multi_provider_perm_test(
            Nodes, User, Guid, PosixPerms, AllowedPerms,
            ok, <<"allowed posix perm">>, Config
        )
    end, lists:seq(1, 5)),

    % Set random acl permissions for file/dir and assert they are properly propagated to other
    % nodes/providers (that includes permissions cache - obsolete entries should be overridden)
    lists:foreach(fun(_IterationNum) ->
        SetPerms = lists_utils:random_sublist(AllPerms),
        permissions_test_utils:set_acls(P1W2, #{Guid => SetPerms}, #{}, ?everyone, ?no_flags_mask),

        run_multi_provider_perm_test(
            Nodes, User, Guid, SetPerms, permissions_test_utils:complementary_perms(P1W2, Guid, SetPerms),
            {error, ?EACCES}, <<"denied acl perm">>, Config
        ),
        run_multi_provider_perm_test(
            Nodes, User, Guid, SetPerms, SetPerms,
            ok, <<"allowed acl perm">>, Config
        )
    end, lists:seq(1, 10)).


run_multi_provider_perm_test(Nodes, User, Guid, PermsSet, TestedPerms, ExpResult, Scenario, Config) ->
    lists:foreach(fun(TestedPerm) ->
        lists:foreach(fun(Node) ->
            try
                ?assertMatch(
                    ExpResult,
                    check_perms(Node, User, Guid, [TestedPerm], Config),
                    ?ATTEMPTS
                )
            catch _:Reason ->
                ct:pal(
                    "PERMISSIONS TESTS FAILURE~n"
                    "   Scenario: multi_provider_permission_cache_test ~p~n"
                    "   Node: ~p~n"
                    "   Perms set: ~p~n"
                    "   Tested perm: ~p~n"
                    "   Reason: ~p~n",
                    [
                        Scenario, Node, PermsSet, TestedPerm, Reason
                    ]
                ),
                erlang:error(perms_test_failed)
            end
        end, Nodes)
    end, TestedPerms).


expired_session_test(Config) ->
    % Setup
    [W | _] = ?config(op_worker_nodes, Config),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W)}}, Config),
    {_, GUID} = ?assertMatch(
        {ok, _},
        lfm_proxy:create(W, SessId1, <<"/space1/es_file">>, 8#770)
    ),

    ok = rpc:call(W, session, delete, [SessId1]),

    % Verification
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:open(W, SessId1, ?FILE_REF(GUID), write)
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = initializer:setup_storage(NewConfig),
        NewConfig2 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig1, "env_desc.json"),
            [{spaces_owners, [<<"owner">>]} | NewConfig1]
        ),
        initializer:mock_auth_manager(NewConfig2),
        NewConfig2
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, ?MODULE]} | Config].


end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(multi_provider_permission_cache_test, Config) ->
    ct:timetrap({minutes, 15}),
    init_per_testcase(default, Config);

init_per_testcase(mv_dir_test, Config) ->
    ct:timetrap({minutes, 5}),
    init_per_testcase(default, Config);

init_per_testcase(mv_file_test, Config) ->
    ct:timetrap({minutes, 5}),
    init_per_testcase(default, Config);

init_per_testcase(_Case, Config) ->
    initializer:mock_share_logic(Config),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
check_perms(Node, User, Guid, Perms, Config) ->
    SessId = ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config),
    UserCtx = rpc:call(Node, user_ctx, new, [SessId]),

    rpc:call(Node, ?MODULE, check_perms, [
        UserCtx, file_ctx:new_by_guid(Guid),
        [?OPERATIONS(permissions_test_utils:perms_to_bitmask(Perms))]
    ]).


%% @private
check_perms(UserCtx, FileCtx, Perms) ->
    try
        fslogic_authz:ensure_authorized(UserCtx, FileCtx, Perms),
        ok
    catch _Type:Reason ->
        {error, Reason}
    end.


%% @private
-spec for(pos_integer(), term()) -> term().
for(1, F) ->
    F();
for(N, F) ->
    F(),
    for(N - 1, F).


%% @private
-spec fill_file_with_dummy_data(node(), session:id(), file_id:file_guid()) -> ok.
fill_file_with_dummy_data(Node, SessId, Guid) ->
    {ok, FileHandle} = ?assertMatch(
        {ok, _},
        lfm_proxy:open(Node, SessId, ?FILE_REF(Guid), write)
    ),
    ?assertMatch({ok, 4}, lfm_proxy:write(Node, FileHandle, 0, <<"DATA">>)),
    ?assertMatch(ok, lfm_proxy:fsync(Node, FileHandle)),
    ?assertMatch(ok, lfm_proxy:close(Node, FileHandle)).


%% @private
-spec create_dummy_file(node(), session:id(), file_id:file_guid()) -> ok.
create_dummy_file(Node, SessId, DirGuid) ->
    RandomFileName = <<"DUMMY_FILE_", (integer_to_binary(rand:uniform(1024)))/binary>>,
    {ok, {_Guid, FileHandle}} =
        lfm_proxy:create_and_open(Node, SessId, DirGuid, RandomFileName, ?DEFAULT_FILE_PERMS),
    ?assertMatch(ok, lfm_proxy:close(Node, FileHandle)).


%% @private
-spec extract_ok
    (ok | {ok, term()} | {ok, term(), term()} | {ok, term(), term(), term()}) -> ok;
    ({error, term()}) -> {error, term()}.
extract_ok(ok) -> ok;
extract_ok({ok, _}) -> ok;
extract_ok({ok, _, _}) -> ok;
extract_ok({ok, _, _, _}) -> ok;
extract_ok({error, _} = Error) -> Error.
