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
-module(lfm_permissions_test_SUITE).
-author("Bartosz Walkowicz").

-include("lfm_permissions_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    data_access_caveats_test/1,
    data_access_caveats_ancestors_test/1,
    data_access_caveats_ancestors_test2/1,
    data_access_caveats_cache_test/1,

    mkdir_test/1,
    ls_test/1,
    readdir_plus_test/1,
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
    expired_session_test/1
]).

all() ->
    ?ALL([
        data_access_caveats_test,
        data_access_caveats_ancestors_test,
        data_access_caveats_ancestors_test2,
        data_access_caveats_cache_test,

        mkdir_test,
        ls_test,
        readdir_plus_test,
        get_child_attr_test,
        mv_dir_test,
        rm_dir_test,

        create_file_test,
        open_for_read_test,
        open_for_write_test,
        open_for_rdwr_test,
        create_and_open_test,
        truncate_test,
        mv_file_test,
        rm_file_test,

        get_parent_test,
        get_file_path_test,
        get_file_guid_test,
        get_file_attr_test,
        get_file_distribution_test,

        set_perms_test,
        check_read_perms_test,
        check_write_perms_test,
        check_rdwr_perms_test,

        create_share_test,
        remove_share_test,
        share_perms_test,

        get_acl_test,
        set_acl_test,
        remove_acl_test,

        get_transfer_encoding_test,
        set_transfer_encoding_test,
        get_cdmi_completion_status_test,
        set_cdmi_completion_status_test,
        get_mimetype_test,
        set_mimetype_test,

        get_metadata_test,
        set_metadata_test,
        remove_metadata_test,
        get_xattr_test,
        list_xattr_test,
        set_xattr_test,
        remove_xattr_test,

        add_qos_entry_test,
        get_qos_entry_test,
        remove_qos_entry_test,
        get_effective_file_qos_test,
        check_qos_fulfillment_test,

        permission_cache_test,
        expired_session_test
    ]).


-define(rpcCache(W, Function, Args), rpc:call(W, permissions_cache, Function, Args)).

-define(SCENARIO_NAME, atom_to_binary(?FUNCTION_NAME, utf8)).


%%%===================================================================
%%% Test functions
%%%===================================================================


data_access_caveats_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    Owner = <<"user1">>,
    OwnerUserSessId = ?config({session_id, {Owner, ?GET_DOMAIN(W)}}, Config),

    UserId = <<"user2">>,
    UserRootDir = fslogic_uuid:user_root_dir_guid(UserId),
    Space1RootDir = fslogic_uuid:spaceid_to_space_dir_guid(<<"space1">>),
    Space3RootDir = fslogic_uuid:spaceid_to_space_dir_guid(<<"space3">>),

    ScenarioName = ?SCENARIO_NAME,
    DirName = <<ScenarioName/binary, "1">>,
    DirPath = <<"/space1/", DirName/binary>>,
    {ok, DirGuid} = lfm_proxy:mkdir(W, OwnerUserSessId, DirPath),
    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    {ok, ShareId} = lfm_proxy:create_share(W, OwnerUserSessId, {guid, DirGuid}, <<"share">>),
    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),

    DirName2 = <<ScenarioName/binary, "2">>,
    DirPath2 = <<"/space1/", DirName2/binary>>,
    {ok, _DirGuid2} = lfm_proxy:mkdir(W, OwnerUserSessId, DirPath2),

    [
        {Path1, ObjectId1, F1, ShareF1},
        {Path2, ObjectId2, F2, ShareF2},
        {Path3, ObjectId3, F3, ShareF3},
        {Path4, ObjectId4, F4, ShareF4},
        {Path5, ObjectId5, F5, ShareF5}
    ] = lists:map(fun(Num) ->
        FileName = <<"file", ($0 + Num)>>,
        FilePath = <<DirPath/binary, "/", FileName/binary>>,
        {ok, FileGuid} = lfm_proxy:create(W, OwnerUserSessId, FilePath, 8#777),
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
        ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
        {FilePath, FileObjectId, {FileGuid, FileName}, {ShareFileGuid, FileName}}
    end, lists:seq(1, 5)),

    MainToken = initializer:create_access_token(UserId),

    LsWithConfinedToken = fun(Guid, Caveats) ->
        LsToken = tokens:confine(MainToken, Caveats),
        LsSessId = lfm_permissions_test_utils:create_session(W, UserId, LsToken),
        lfm_proxy:ls(W, LsSessId, {guid, Guid}, 0, 100)
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
    SessId12 = lfm_permissions_test_utils:create_session(W, UserId, MainToken),
    ?assertMatch(
        {ok, [_ | _]},
        lfm_proxy:ls(W, SessId12, {guid, Space1RootDir}, 0, 100)
    ),
    % And all operations on it and it's children should be allowed
    ?assertMatch(
        {ok, _},
        lfm_proxy:get_acl(W, SessId12, {guid, Space1RootDir})
    ),
    ?assertMatch(
        {ok, _},
        lfm_proxy:get_acl(W, SessId12, {guid, DirGuid})
    ),
    % But with caveats space ls should show only dirs leading to allowed files.
    Token13 = tokens:confine(MainToken, #cv_data_path{whitelist = [Path1]}),
    SessId13 = lfm_permissions_test_utils:create_session(W, UserId, Token13),
    ?assertMatch(
        {ok, [{DirGuid, DirName}]},
        lfm_proxy:ls(W, SessId13, {guid, Space1RootDir}, 0, 100)
    ),
    % On such dirs (ancestor) it should be possible to perform only certain
    % operations like ls, stat, resolve_guid, get_parent and resolve_path.
    ?assertMatch(
        {ok, [F1]},
        lfm_proxy:ls(W, SessId13, {guid, DirGuid}, 0, 100)
    ),
    ?assertMatch(
        {ok, #file_attr{name = DirName, type = ?DIRECTORY_TYPE}},
        lfm_proxy:stat(W, SessId13, {guid, DirGuid})
    ),
    ?assertMatch(
        {ok, DirGuid},
        lfm_proxy:resolve_guid(W, SessId13, DirPath)
    ),
    ?assertMatch(
        {ok, Space1RootDir},
        lfm_proxy:get_parent(W, SessId13, {guid, DirGuid})
    ),
    ?assertMatch(
        {ok, DirPath},
        lfm_proxy:get_file_path(W, SessId13, DirGuid)
    ),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:get_acl(W, SessId13, {guid, DirGuid})
    ),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:create(W, SessId13, DirGuid, <<"file1">>, 8#777)
    ),

    % Test listing with caveats and options (offset, limit)
    Token14 = tokens:confine(MainToken, #cv_data_path{whitelist = [
        Path1, Path2, <<DirPath/binary, "/i_do_not_exist">>, Path4, Path5
    ]}),
    SessId14 = lfm_permissions_test_utils:create_session(W, UserId, Token14),
    ?assertMatch(
        {ok, [F1, F2, F4]},
        lfm_proxy:ls(W, SessId14, {guid, DirGuid}, 0, 3)
    ),
    ?assertMatch(
        {ok, [F4, F5]},
        lfm_proxy:ls(W, SessId14, {guid, DirGuid}, 2, 3)
    ),
    ?assertMatch(
        {ok, [F4]},
        lfm_proxy:ls(W, SessId14, {guid, DirGuid}, 2, 1)
    ).


data_access_caveats_ancestors_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    UserId = <<"user3">>,
    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(W)}}, Config),
    UserRootDir = fslogic_uuid:user_root_dir_guid(UserId),

    SpaceName = <<"space2">>,
    SpaceRootDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceName),

    Dirs0 = [{LastDirGuid, _} | _] = lists:foldl(fun(Num, [{DirGuid, _} | _] = Acc) ->
        SubDirName = <<"dir", (integer_to_binary(Num))/binary>>,
        {ok, SubDirGuid} = lfm_proxy:mkdir(W, UserSessId, DirGuid, SubDirName, 8#777),
        {ok, _} = lfm_proxy:create(W, UserSessId, DirGuid, <<"file", ($0 + Num)>>, 8#777),
        [{SubDirGuid, SubDirName} | Acc]
    end, [{SpaceRootDirGuid, SpaceName}], lists:seq(1, 20)),
    Dirs1 = lists:reverse(Dirs0),

    FileInDeepestDirName = <<"file">>,
    {ok, FileInDeepestDirGuid} = lfm_proxy:create(W, UserSessId, LastDirGuid, FileInDeepestDirName, 8#777),
    {ok, FileInDeepestDirObjectId} = file_id:guid_to_objectid(FileInDeepestDirGuid),

    Token = initializer:create_access_token(UserId, [
        #cv_data_objectid{whitelist = [FileInDeepestDirObjectId]}
    ]),
    SessId = lfm_permissions_test_utils:create_session(W, UserId, Token),

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
                lfm_proxy:get_acl(W, SessId, {guid, DirGuid})
            ),

            % Below operations should succeed for every dir/ancestor leading
            % to file allowed by caveats
            ?assertMatch(
                {ok, [Child]},
                lfm_proxy:ls(W, SessId, {guid, DirGuid}, 0, 100)
            ),
            ?assertMatch(
                {ok, #file_attr{name = DirName, type = ?DIRECTORY_TYPE}},
                lfm_proxy:stat(W, SessId, {guid, DirGuid})
            ),
            ?assertMatch(
                {ok, DirGuid},
                lfm_proxy:resolve_guid(W, SessId, DirPath)
            ),
            ?assertMatch(
                {ok, ParentGuid},
                lfm_proxy:get_parent(W, SessId, {guid, DirGuid})
            ),
            ?assertMatch(
                {ok, DirPath},
                lfm_proxy:get_file_path(W, SessId, DirGuid)
            ),

            {DirPath, DirGuid}
        end,
        {<<>>, undefined},
        lists:zip([{UserRootDir, UserId} | Dirs1], Dirs1 ++ [{FileInDeepestDirGuid, FileInDeepestDirName}])
    ),

    % Get acl should finally succeed for file which is allowed by caveats
    ?assertMatch(
        {ok, []},
        lfm_proxy:get_acl(W, SessId, {guid, FileInDeepestDirGuid})
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
    SessId1 = lfm_permissions_test_utils:create_session(W, UserId, Token1),
    ?assertMatch(
        {ok, [{SpaceRootDirGuid, SpaceName}]},
        lfm_proxy:ls(W, SessId1, {guid, UserRootDir}, 0, 100)
    ),
    ?assertMatch(
        {ok, [{RootDirGuid, RootDirName}]},
        lfm_proxy:ls(W, SessId1, {guid, SpaceRootDirGuid}, 0, 100)
    ),
    ?assertMatch(
        {ok, [{LeftDirGuid, LeftDirName}, {RightDirGuid, RightDirName}]},
        lfm_proxy:ls(W, SessId1, {guid, RootDirGuid}, 0, 100)
    ),
    ?assertMatch(
        {ok, [LeftFile]},
        lfm_proxy:ls(W, SessId1, {guid, LeftDirGuid}, 0, 100)
    ),
    ?assertMatch(
        {ok, [RightFile]},
        lfm_proxy:ls(W, SessId1, {guid, RightDirGuid}, 0, 100)
    ),

    % When caveats have empty intersection then ls should return []
    Token2 = tokens:confine(MainToken, [
        #cv_data_objectid{whitelist = [LeftFileObjectId]},
        #cv_data_objectid{whitelist = [RightFileObjectId]}
    ]),
    SessId2 = lfm_permissions_test_utils:create_session(W, UserId, Token2),
    ?assertMatch(
        {ok, [{SpaceRootDirGuid, SpaceName}]},
        lfm_proxy:ls(W, SessId2, {guid, UserRootDir}, 0, 100)
    ),
    ?assertMatch(
        {ok, [{RootDirGuid, RootDirName}]},
        lfm_proxy:ls(W, SessId2, {guid, SpaceRootDirGuid}, 0, 100)
    ),
    ?assertMatch(
        {ok, []},
        lfm_proxy:ls(W, SessId2, {guid, RootDirGuid}, 0, 100)
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
    SessId = lfm_permissions_test_utils:create_session(W, UserId, Token),


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
        lfm_proxy:get_acl(W, SessId, {guid, FileGuid})
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
        {ok, subpath},
        ?rpcCache(W, check_permission, [{data_constraint, Token, FileGuid}])
    ),

    % calling on dir any function reserved only for subpath should cache
    % {subpath, ?EACCES} meaning that no such operation can be performed
    % but since ancestor checks were not performed it is not known whether
    % ancestor operations can be performed
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:get_acl(W, SessId, {guid, DirGuid})
    ),
    ?assertEqual(
        {ok, {subpath, ?EACCES}},
        ?rpcCache(W, check_permission, [{data_constraint, Token, DirGuid}])
    ),

    % after calling operation possible to perform on ancestor cached value should
    % be changed to signal that file is ancestor and such operations can be performed
    ?assertMatch(
        {ok, [_]},
        lfm_proxy:ls(W, SessId, {guid, DirGuid}, 0, 100)
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
        lfm_proxy:ls(W, SessId, {guid, OtherDirGuid}, 0, 100)
    ),
    ?assertEqual(
        {ok, ?EACCES},
        ?rpcCache(W, check_permission, [{data_constraint, Token, OtherDirGuid}])
    ).


mkdir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_subcontainer]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            {guid, ParentDirGuid} = maps:get(ParentDirPath, ExtraData),
            lfm_proxy:mkdir(W, SessId, ParentDirGuid, <<"dir2">>, 8#777)
        end
    }, Config).


ls_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?list_container]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            lfm_proxy:ls(W, SessId, DirKey, 0, 100)
        end
    }, Config).


readdir_plus_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?list_container]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            lfm_proxy:read_dir_plus(W, SessId, DirKey, 0, 100)
        end
    }, Config).


get_child_attr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container],
            children = [#file{name = <<"file1">>}]
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            {guid, ParentDirGuid} = maps:get(ParentDirPath, ExtraData),
            lfm_proxy:get_child_attr(W, SessId, ParentDirGuid, <<"file1">>)
        end
    }, Config).


mv_dir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
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
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            SrcDirPath = <<TestCaseRootDirPath/binary, "/dir1/dir11">>,
            SrcDirKey = maps:get(SrcDirPath, ExtraData),
            DstDirPath = <<TestCaseRootDirPath/binary, "/dir2">>,
            DstDirKey = maps:get(DstDirPath, ExtraData),
            lfm_proxy:mv(W, SessId, SrcDirKey, DstDirKey, <<"dir21">>)
        end
    }, Config).


rm_dir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
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
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1/dir2">>,
            DirKey = maps:get(DirPath, ExtraData),
            lfm_proxy:unlink(W, SessId, DirKey)
        end
    }, Config).


create_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            {guid, ParentDirGuid} = maps:get(ParentDirPath, ExtraData),
            lfm_proxy:create(W, SessId, ParentDirGuid, <<"file1">>, 8#777)
        end
    }, Config).


open_for_read_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object],
            on_create = fun(OwnerSessId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(W, OwnerSessId, Guid),
                {guid, Guid}
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:open(W, SessId, FileKey, read)
        end
    }, Config).


open_for_write_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_object],
            on_create = fun(OwnerSessId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(W, OwnerSessId, Guid),
                {guid, Guid}
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:open(W, SessId, FileKey, write)
        end
    }, Config).


open_for_rdwr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object, ?write_object],
            on_create = fun(OwnerSessId, Guid) ->
                % write to file to force its creation on storage. Otherwise it
                % may be not possible during tests without necessary perms.
                fill_file_with_dummy_data(W, OwnerSessId, Guid),
                {guid, Guid}
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:open(W, SessId, FileKey, rdwr)
        end
    }, Config).


create_and_open_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            perms = [?traverse_container, ?add_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            ParentDirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            {guid, ParentDirGuid} = maps:get(ParentDirPath, ExtraData),
            lfm_proxy:create_and_open(W, SessId, ParentDirGuid, <<"file1">>, 8#777)
        end
    }, Config).


truncate_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:truncate(W, SessId, FileKey, 0)
        end
    }, Config).


mv_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
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
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            SrcFilePath = <<TestCaseRootDirPath/binary, "/dir1/file11">>,
            SrcFileKey = maps:get(SrcFilePath, ExtraData),
            DstDirPath = <<TestCaseRootDirPath/binary, "/dir2">>,
            DstDirKey = maps:get(DstDirPath, ExtraData),
            lfm_proxy:mv(W, SessId, SrcFileKey, DstDirKey, <<"file21">>)
        end
    }, Config).


rm_file_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
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
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/dir1/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:unlink(W, SessId, FileKey)
        end
    }, Config).


get_parent_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:get_parent(W, SessId, FileKey)
        end
    }, Config).


get_file_path_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = false, % TODO VFS-6057
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            {guid, FileGuid} = maps:get(FilePath, ExtraData),
            lfm_proxy:get_file_path(W, SessId, FileGuid)
        end
    }, Config).


get_file_guid_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, _ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            lfm_proxy:resolve_guid(W, SessId, FilePath)
        end
    }, Config).


get_file_attr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{name = <<"file1">>}],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:stat(W, SessId, FileKey)
        end
    }, Config).


get_file_distribution_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:get_file_distribution(W, SessId, FileKey)
        end
    }, Config).


set_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Owner = <<"user1">>,

    OwnerUserSessId = ?config({session_id, {Owner, ?GET_DOMAIN(W)}}, Config),
    GroupUserSessId = ?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config),
    OtherUserSessId = ?config({session_id, {<<"user3">>, ?GET_DOMAIN(W)}}, Config),

    DirPath = <<"/space1/dir1">>,
    {ok, DirGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:mkdir(W, OwnerUserSessId, DirPath)
    ),

    FilePath = <<"/space1/dir1/file1">>,
    {ok, FileGuid} = ?assertMatch(
        {ok, _},
        lfm_proxy:create(W, OwnerUserSessId, FilePath, 8#777)
    ),
    {ok, ShareId} = ?assertMatch({ok, _}, lfm_proxy:create_share(W, OwnerUserSessId, {guid, FileGuid}, <<"share">>)),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    %% POSIX

    % owner can always change file perms if he has access to it
    lfm_permissions_test_utils:set_modes(W, #{DirGuid => 8#677, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, OwnerUserSessId, {guid, FileGuid}, 8#000)
    ),
    lfm_permissions_test_utils:set_modes(W, #{DirGuid => 8#100, FileGuid => 8#000}),
    ?assertMatch(ok, lfm_proxy:set_perms(W, OwnerUserSessId, {guid, FileGuid}, 8#000)),

    % but not if that access is via shared guid
    lfm_permissions_test_utils:set_modes(W, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, OwnerUserSessId, {guid, ShareFileGuid}, 8#000)
    ),

    % other users from space can't change perms no matter what
    lfm_permissions_test_utils:set_modes(W, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, GroupUserSessId, {guid, FileGuid}, 8#000)
    ),

    % users outside of space shouldn't even see the file
    lfm_permissions_test_utils:set_modes(W, #{DirGuid => 8#777, FileGuid => 8#777}),
    ?assertMatch(
        {error, ?ENOENT},
        lfm_proxy:set_perms(W, OtherUserSessId, {guid, FileGuid}, 8#000)
    ),

    %% ACL

    % owner can always change file perms if he has access to it
    lfm_permissions_test_utils:set_acls(W, #{
        DirGuid => ?ALL_DIR_PERMS -- [?traverse_container],
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, OwnerUserSessId, {guid, FileGuid}, 8#000)
    ),

    lfm_permissions_test_utils:set_acls(W, #{
        DirGuid => [?traverse_container],
        FileGuid => []
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(ok, lfm_proxy:set_perms(W, OwnerUserSessId, {guid, FileGuid}, 8#000)),

    % but not if that access is via shared guid
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, OwnerUserSessId, {guid, ShareFileGuid}, 8#000)
    ),

    % other users from space can't change perms no matter what
    lfm_permissions_test_utils:set_acls(W, #{
        DirGuid => ?ALL_DIR_PERMS,
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:set_perms(W, GroupUserSessId, {guid, FileGuid}, 8#000)
    ),

    % users outside of space shouldn't even see the file
    lfm_permissions_test_utils:set_acls(W, #{
        DirGuid => ?ALL_DIR_PERMS,
        FileGuid => ?ALL_FILE_PERMS
    }, #{}, ?everyone, ?no_flags_mask),
    ?assertMatch(
        {error, ?ENOENT},
        lfm_proxy:set_perms(W, OtherUserSessId, {guid, FileGuid}, 8#000)
    ).


check_read_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:check_perms(W, SessId, FileKey, read)
        end
    }, Config).


check_write_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_object]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:check_perms(W, SessId, FileKey, write)
        end
    }, Config).


check_rdwr_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_object, ?write_object]
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA, ?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:check_perms(W, SessId, FileKey, rdwr)
        end
    }, Config).


create_share_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#dir{name = <<"dir1">>}],
        posix_requires_space_privs = [?SPACE_MANAGE_SHARES],
        acl_requires_space_privs = [?SPACE_MANAGE_SHARES],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            DirKey = maps:get(DirPath, ExtraData),
            lfm_proxy:create_share(W, SessId, DirKey, <<"create_share">>)
        end
    }, Config).


remove_share_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#dir{
            name = <<"dir1">>,
            on_create = fun(OwnerSessId, Guid) ->
                {ok, ShareId} = ?assertMatch({ok, _}, lfm_proxy:create_share(
                    W, OwnerSessId, {guid, Guid}, <<"share_to_remove">>
                )),
                ShareId
            end
        }],
        posix_requires_space_privs = [?SPACE_MANAGE_SHARES],
        acl_requires_space_privs = [?SPACE_MANAGE_SHARES],
        available_in_readonly_mode = false,
        available_in_share_mode = inapplicable,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            DirPath = <<TestCaseRootDirPath/binary, "/dir1">>,
            ShareId = maps:get(DirPath, ExtraData),
            lfm_proxy:remove_share(W, SessId, ShareId)
        end
    }, Config).


share_perms_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),
    Owner = <<"user1">>,

    OwnerUserSessId = ?config({session_id, {Owner, ?GET_DOMAIN(W)}}, Config),
    GroupUserSessId = ?config({session_id, {<<"user2">>, ?GET_DOMAIN(W)}}, Config),

    ScenarioDirName = ?SCENARIO_NAME,
    ScenarioDirPath = <<"/space1/", ScenarioDirName/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W, OwnerUserSessId, ScenarioDirPath, 8#700)),

    MiddleDirPath = <<ScenarioDirPath/binary, "/dir2">>,
    {ok, MiddleDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, OwnerUserSessId, MiddleDirPath, 8#777)),

    BottomDirPath = <<MiddleDirPath/binary, "/dir3">>,
    {ok, BottomDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W, OwnerUserSessId, BottomDirPath), 8#777),

    FilePath = <<BottomDirPath/binary, "/file1">>,
    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W, OwnerUserSessId, FilePath, 8#777)),

    {ok, ShareId} = ?assertMatch({ok, _}, lfm_proxy:create_share(W, OwnerUserSessId, {guid, MiddleDirGuid}, <<"share">>)),
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    % Accessing file in normal mode by space user should result in eacces (dir1 perms -> 8#700)
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:stat(W, GroupUserSessId, {guid, FileGuid})
    ),
    % But accessing it in share mode should succeed as perms should be checked only up to
    % share root (dir1/dir2 -> 8#777) and not space root
    ?assertMatch(
        {ok, #file_attr{guid = ShareFileGuid}},
        lfm_proxy:stat(W, GroupUserSessId, {guid, ShareFileGuid})
    ),

    % Changing BottomDir mode to 8#770 should forbid access to file in share mode
    ?assertEqual(ok, lfm_proxy:set_perms(W, ?ROOT_SESS_ID, {guid, BottomDirGuid}, 8#770)),
    ?assertMatch(
        {error, ?EACCES},
        lfm_proxy:stat(W, GroupUserSessId, {guid, ShareFileGuid})
    ).


get_acl_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_acl]
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:get_acl(W, SessId, FileKey)
        end
    }, Config).


set_acl_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_acl]
        }],
        posix_requires_space_privs = owner,
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:set_acl(W, SessId, FileKey, [
                ?ALLOW_ACE(
                    ?group,
                    ?no_flags_mask,
                    lfm_permissions_test_utils:perms_to_bitmask(?ALL_FILE_PERMS)
                )
            ])
        end
    }, Config).


remove_acl_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_acl]
        }],
        posix_requires_space_privs = owner,
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:remove_acl(W, SessId, FileKey)
        end
    }, Config).


get_transfer_encoding_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(OwnerSessId, Guid) ->
                lfm_proxy:set_transfer_encoding(W, OwnerSessId, {guid, Guid}, <<"base64">>),
                {guid, Guid}
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:get_transfer_encoding(W, SessId, FileKey)
        end
    }, Config).


set_transfer_encoding_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:set_transfer_encoding(W, SessId, FileKey, <<"base64">>)
        end
    }, Config).


get_cdmi_completion_status_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(OwnerSessId, Guid) ->
                lfm_proxy:set_cdmi_completion_status(W, OwnerSessId, {guid, Guid}, <<"Completed">>),
                {guid, Guid}
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:get_cdmi_completion_status(W, SessId, FileKey)
        end
    }, Config).


set_cdmi_completion_status_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:set_cdmi_completion_status(W, SessId, FileKey, <<"Completed">>)
        end
    }, Config).


get_mimetype_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_attributes],
            on_create = fun(OwnerSessId, Guid) ->
                lfm_proxy:set_mimetype(W, OwnerSessId, {guid, Guid}, <<"mimetype">>),
                {guid, Guid}
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:get_mimetype(W, SessId, FileKey)
        end
    }, Config).


set_mimetype_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_attributes]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:set_mimetype(W, SessId, FileKey, <<"mimetype">>)
        end
    }, Config).


get_metadata_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                lfm_proxy:set_metadata(W, OwnerSessId, {guid, Guid}, json, <<"VAL">>, []),
                {guid, Guid}
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:get_metadata(W, SessId, FileKey, json, [], false)
        end
    }, Config).


set_metadata_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:set_metadata(W, SessId, FileKey, json, <<"VAL">>, [])
        end
    }, Config).


remove_metadata_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                lfm_proxy:set_metadata(W, OwnerSessId, {guid, Guid}, json, <<"VAL">>, []),
                {guid, Guid}
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:remove_metadata(W, SessId, FileKey, json)
        end
    }, Config).


get_xattr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(W, OwnerSessId, {guid, Guid}, Xattr),
                {guid, Guid}
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:get_xattr(W, SessId, FileKey, <<"myxattr">>)
        end
    }, Config).


list_xattr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            on_create = fun(OwnerSessId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(W, OwnerSessId, {guid, Guid}, Xattr),
                {guid, Guid}
            end
        }],
        available_in_readonly_mode = true,
        available_in_share_mode = true,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:list_xattr(W, SessId, FileKey, false, false)
        end
    }, Config).


set_xattr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:set_xattr(W, SessId, FileKey, #xattr{name = <<"myxattr">>, value = <<"VAL">>})
        end
    }, Config).


remove_xattr_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                Xattr = #xattr{name = <<"myxattr">>, value = <<"VAL">>},
                lfm_proxy:set_xattr(W, OwnerSessId, {guid, Guid}, Xattr),
                {guid, Guid}
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:remove_xattr(W, SessId, FileKey, <<"myxattr">>)
        end
    }, Config).


add_qos_entry_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata]
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = false,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:add_qos_entry(W, SessId, FileKey, <<"country=FR">>, 1)
        end
    }, Config).


get_qos_entry_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                {ok, QosEntryId} = lfm_proxy:add_qos_entry(
                    W, OwnerSessId, {guid, Guid}, <<"country=FR">>, 1
                ),
                QosEntryId
            end 
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            lfm_proxy:get_qos_entry(W, SessId, QosEntryId)
        end
    }, Config).


remove_qos_entry_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?write_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                {ok, QosEntryId} = lfm_proxy:add_qos_entry(
                    W, OwnerSessId, {guid, Guid}, <<"country=FR">>, 1
                ),
                QosEntryId
            end
        }],
        posix_requires_space_privs = [?SPACE_WRITE_DATA],
        acl_requires_space_privs = [?SPACE_WRITE_DATA],
        available_in_readonly_mode = false,
        available_in_share_mode = inapplicable,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            lfm_proxy:remove_qos_entry(W, SessId, QosEntryId)
        end
    }, Config).


get_effective_file_qos_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                {ok, _QosEntryId} = lfm_proxy:add_qos_entry(
                    W, OwnerSessId, {guid, Guid}, <<"country=FR">>, 1
                ),
                {guid, Guid}
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            FileKey = maps:get(FilePath, ExtraData),
            lfm_proxy:get_effective_file_qos(W, SessId, FileKey)
        end
    }, Config).


check_qos_fulfillment_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    lfm_permissions_test_scenarios:run_scenarios(#perms_test_spec{
        test_node = W,
        root_dir = ?SCENARIO_NAME,
        files = [#file{
            name = <<"file1">>,
            perms = [?read_metadata],
            on_create = fun(OwnerSessId, Guid) ->
                {ok, QosEntryId} = lfm_proxy:add_qos_entry(
                    W, OwnerSessId, {guid, Guid}, <<"country=FR">>, 1
                ),
                QosEntryId
            end
        }],
        posix_requires_space_privs = [?SPACE_READ_DATA],
        acl_requires_space_privs = [?SPACE_READ_DATA],
        available_in_readonly_mode = true,
        available_in_share_mode = inapplicable,
        operation = fun(_OwnerSessId, SessId, TestCaseRootDirPath, ExtraData) ->
            FilePath = <<TestCaseRootDirPath/binary, "/file1">>,
            QosEntryId = maps:get(FilePath, ExtraData),
            lfm_proxy:check_qos_fulfilled(W, SessId, QosEntryId)
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
        lfm_proxy:open(W, SessId1, {guid, GUID}, write)
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        )
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(_Case, Config) ->
    initializer:mock_share_logic(Config),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec for(pos_integer(), term()) -> term().
for(1, F) ->
    F();
for(N, F) ->
    F(),
    for(N - 1, F).


-spec fill_file_with_dummy_data(node(), session:id(), file_id:file_guid()) -> ok.
fill_file_with_dummy_data(Node, SessId, Guid) ->
    {ok, FileHandle} = ?assertMatch(
        {ok, _},
        lfm_proxy:open(Node, SessId, {guid, Guid}, write)
    ),
    ?assertMatch({ok, 4}, lfm_proxy:write(Node, FileHandle, 0, <<"DATA">>)),
    ?assertMatch(ok, lfm_proxy:fsync(Node, FileHandle)),
    ?assertMatch(ok, lfm_proxy:close(Node, FileHandle)).
