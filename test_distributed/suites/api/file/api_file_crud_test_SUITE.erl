%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file crud API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(api_file_crud_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_file_test_utils.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").

-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_file_test/1,
    create_file_at_path_test/1,
    
    get_file_instance_test/1,
    get_shared_file_instance_test/1,
    get_file_instance_on_provider_not_supporting_space_test/1,

    update_file_instance_test/1,
    update_file_instance_on_provider_not_supporting_space_test/1,

    delete_file_instance_test/1,
    delete_file_instance_at_path_test/1,
    delete_file_instance_on_provider_not_supporting_space_test/1
]).

groups() -> [
    {parallel, [parallel], [
        create_file_test,
        create_file_at_path_test,
        
        get_file_instance_on_provider_not_supporting_space_test,

        update_file_instance_test,
        update_file_instance_on_provider_not_supporting_space_test,

        delete_file_instance_test,
        delete_file_instance_at_path_test,
        delete_file_instance_on_provider_not_supporting_space_test
    ]},
    {sequential, [sequential], [
        % Cannot be run in parallel with other tests, as they modify space
        % content which results in mtime change.
        get_file_instance_test,
        % Cannot be executed in parallel with get_file_instance_test as
        % both tests check space dir shares and expect different results.
        get_shared_file_instance_test
    ]}
].

all() -> [
    {group, parallel},
    {group, sequential}
].


-define(ATTEMPTS, 30).


%%%===================================================================
%%% Create file instance test functions
%%%===================================================================

create_file_test(_Config) ->
    MemRef = api_test_memory:init(),
    api_test_memory:set(MemRef, path_to_parent, <<>>),
    create_file_test_base(id, MemRef).


create_file_at_path_test(_Config) ->
    MemRef = api_test_memory:init(),
    PathTokens = [N1, N2, N3] = lists_utils:generate(fun(_) -> ?RANDOM_FILE_NAME() end, 3),
    onenv_file_test_utils:create_and_sync_file_tree(user2, space_krk_par, #dir_spec{
        name = N1,
        children = [
            #dir_spec{
                name = N2,
                children = [#dir_spec{name = N3}]
            }
        ]
    }),
    api_test_memory:set(MemRef, path_to_parent, filename:join(PathTokens)),
    create_file_test_base(path, MemRef).


create_file_test_base(CreationType, MemRef) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],
    
    SpaceId = oct_background:get_space_id(space_krk_par),
    SpaceObjectId = file_id:check_guid_to_objectid(fslogic_file_id:spaceid_to_space_dir_guid(SpaceId)),
    
    ClientSpec = #client_spec{
        correct = [
            user2,  % space owner - doesn't need any perms
            user3,  % space member - should succeed as creating files doesn't require any perms
            user4   % space member - should succeed as creating files doesn't require any perms
        ],
        unauthorized = [nobody],
        forbidden_not_in_space = [user1]
    },
    
    RequiredBase = case CreationType of
        id -> [<<"name">>];
        path -> []
    end,
    
    % put dummy value, so nothing fails when retrieving it even if it doesn't make sense
    api_test_memory:set(MemRef, hardlink_target, dummy_id),
    ScenarioSpecBase =
        #scenario_spec{
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpec,
            prepare_args_fun = build_create_file_rest_args_fun(SpaceObjectId, CreationType, MemRef),
            validate_result_fun = build_create_file_validate_rest_call_fun(MemRef),
            data_spec = DataSpecBase = #data_spec{
                required = RequiredBase,
                optional = [<<"type">>, <<"mode">>],
                correct_values = CorrectValuesBase = #{
                    % name provided only to test that it is required field, actual name is generated in prepare_args_fun
                    % in order to avoid conflicts
                    <<"name">> => [dummy_name],
                    <<"mode">> => [?DEFAULT_DIR_MODE, ?DEFAULT_FILE_MODE]
                },
                bad_values = [
                    {<<"mode">>, <<"some_binary">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
                    {<<"mode">>, -1, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"mode">>, 0, 8#1777)},
                    {<<"mode">>, <<"2000">>, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"mode">>, 0, 8#1777)}
                    
                ]
            }
        },
    ?assert(onenv_api_test_runner:run_tests([
        ScenarioSpecBase#scenario_spec{
            name = "Create non-link file using rest",
            data_spec = DataSpecBase#data_spec{
                correct_values = CorrectValuesBase#{
                    <<"type">> => ["REG", "DIR"]
                }
            }
        },
        ScenarioSpecBase#scenario_spec{
            name = "Create hardlink using rest",
            setup_fun = fun() ->
                #object{guid = Guid} = onenv_file_test_utils:create_and_sync_file_tree(
                    user2, space_krk_par, #file_spec{}),
                api_test_memory:set(MemRef, hardlink_target, file_id:check_guid_to_objectid(Guid))
            end,
            data_spec = DataSpecBase#data_spec{
                required = RequiredBase ++ [<<"target_file_id">>],
                discriminator = [<<"type">>],
                correct_values = CorrectValuesBase#{
                    <<"type">> => ["LNK"],
                    <<"target_file_id">> => [dummy_id] % NOTE: substituted in prepare_args_fun
                }
            }
        },
        ScenarioSpecBase#scenario_spec{
            name = "Create symlink using rest",
            data_spec = DataSpecBase#data_spec{
                required = RequiredBase ++ [<<"target_file_path">>],
                discriminator = [<<"type">>],
                correct_values = CorrectValuesBase#{
                    <<"type">> => ["SYMLNK"],
                    <<"target_file_path">> => [<<"dummy_symlink_path">>]
                }
            }
        }
    ])).


%% @private
build_create_file_rest_args_fun(ParentId, CreationType, MemRef) ->
    fun(#api_test_ctx{data = Data0}) ->
        Data1 = utils:ensure_defined(Data0, #{}),
        {Id, Data2} = api_test_utils:maybe_substitute_bad_id(ParentId, Data1),
        Name = ?RANDOM_FILE_NAME(),
        api_test_memory:set(MemRef, name, Name),
        % replace name to unique here to avoid conflicts between tests
        Data3 = maps_utils:update_existing_key(Data2, <<"name">>, Name),
        Data4 = maps_utils:update_existing_key(
            Data3, <<"target_file_id">>, api_test_memory:get(MemRef, hardlink_target)),
    
        {Method, RestPath, Data5} = case CreationType of
            id ->
                {post, <<"data/", Id/binary, "/children">>, Data4};
            path ->
                PathToDirectParent = api_test_memory:get(MemRef, path_to_parent),
                Path = <<"data/", Id/binary, "/path/", PathToDirectParent/binary, "/", Name/binary>>,
                {put, Path, maps:without([<<"name">>], Data4)}
        end,
        
        #rest_args{
            method = Method,
            path = http_utils:append_url_parameters(RestPath, Data5)
        }
    end.


%% @private
build_create_file_validate_rest_call_fun(MemRef) ->
    fun
        (#api_test_ctx{client = ?USER(UserId), node = Node, data = Data}, {ok, ?HTTP_201_CREATED, _, Response}) ->
            #{<<"fileId">> := FileObjectId} = ?assertMatch(#{<<"fileId">> := _}, Response),
            ProviderId = opw_test_rpc:get_provider_id(Node),
            SessId = oct_background:get_user_session_id(UserId, ProviderId),
            StatFun = fun(AttrList) ->
                lfm_proxy:stat(Node, SessId, ?FILE_REF(file_id:check_objectid_to_guid(FileObjectId)), AttrList)
            end,
            PathToDirectParent = api_test_memory:get(MemRef, path_to_parent),
            ExpectedName = api_test_memory:get(MemRef, name),
            ExpectedPath = filename:join([<<"/">>, ?SPACE_KRK_PAR, PathToDirectParent, ExpectedName]),
    
            BaseAttrList = [?attr_name, ?attr_type, ?attr_path],
            case maps:get(<<"type">>, Data, "REG") of
                "REG" ->
                    ?assertMatch({ok, #file_attr{
                        name = ExpectedName,
                        type = ?REGULAR_FILE_TYPE,
                        path = ExpectedPath
                    }}, StatFun(BaseAttrList));
                "DIR" ->
                    ?assertMatch({ok, #file_attr{
                        name = ExpectedName,
                        type = ?DIRECTORY_TYPE,
                        path = ExpectedPath
                    }}, StatFun(BaseAttrList));
                "LNK" ->
                    ?assertMatch({ok, #file_attr{
                        name = ExpectedName,
                        type = ?REGULAR_FILE_TYPE, % hardlinks are indistinguishable from regular files
                        path = ExpectedPath,
                        hardlink_count = 2
                    }},
                        StatFun(BaseAttrList ++ [?attr_hardlink_count]));
                "SYMLNK" ->
                    SymlinkValue = maps:get(<<"target_file_path">>, Data),
                    ?assertMatch({ok, #file_attr{
                        name = ExpectedName,
                        type = ?SYMLINK_TYPE,
                        path = ExpectedPath,
                        symlink_value = SymlinkValue
                    }},
                        StatFun(BaseAttrList ++ [?attr_symlink_value]))
            end;
        (_ApiTestCtx, _) ->
            ok
    end.

%%%===================================================================
%%% Get file instance test functions
%%%===================================================================

get_file_instance_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],

    {FileType, _FilePath, FileGuid, #file_attr{
        guid = FileGuid
    } = FileAttrs} = api_test_utils:create_file_in_space_krk_par_with_additional_metadata(
        <<"/", ?SPACE_KRK_PAR/binary>>, false, ?RANDOM_FILE_NAME()
    ),
    ExpJsonFileDetails = fun(Node) -> file_attrs_to_gs_json(Node, undefined, FileAttrs) end,

    SpaceId = oct_background:get_space_id(space_krk_par),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    SpaceDetails = get_space_dir_attrs(paris, SpaceGuid, ?SPACE_KRK_PAR),
    ExpJsonSpaceDetails = fun(Node) -> file_attrs_to_gs_json(Node, undefined, SpaceDetails) end,

    ClientSpec = #client_spec{
        correct = [
            user2,  % space owner - doesn't need any perms
            user3,  % files owner
            user4   % space member - should succeed as getting attrs doesn't require any perms
            % TODO VFS-6766 revoke ?SPACE_VIEW priv and see that list of shares is empty
        ],
        unauthorized = [nobody],
        forbidden_not_in_space = [user1]
    },

    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = str_utils:format("Get instance for ~ts using gs private api", [FileType]),
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpec,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(FileGuid, private),
            validate_result_fun = build_get_instance_validate_gs_call_fun(ExpJsonFileDetails),
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, undefined, undefined
            )
        },
        #scenario_spec{
            name = str_utils:format("Get instance for ~ts using gs public api", [FileType]),
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(FileGuid, public),
            validate_result_fun = fun(#api_test_ctx{client = Client}, Result) ->
                case Client of
                    ?NOBODY -> ?assertEqual(?ERROR_UNAUTHORIZED, Result);
                    _ -> ?assertEqual(?ERROR_FORBIDDEN, Result)
                end
            end
        },
        #scenario_spec{
            name = str_utils:format("Get instance for ?SPACE_KRK_PAR using gs private api"),
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpec,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(SpaceGuid, private),
            validate_result_fun = build_get_instance_validate_gs_call_fun(ExpJsonSpaceDetails)
        }
    ])).


get_shared_file_instance_test(_Config) ->
    [P1] = oct_background:get_provider_nodes(krakow),
    [P2] = oct_background:get_provider_nodes(paris),
    Providers = [P1, P2],

    SpaceOwnerSessId = oct_background:get_user_session_id(user2, krakow),

    {FileType, _FilePath, FileGuid, #file_attr{
            guid = FileGuid,
            shares = OriginalShares
    } = OriginalFileAttrs} = api_test_utils:create_file_in_space_krk_par_with_additional_metadata(
        <<"/", ?SPACE_KRK_PAR/binary>>, false, ?RANDOM_FILE_NAME()
    ),

    FileShareId1 = api_test_utils:share_file_and_sync_file_attrs(P1, SpaceOwnerSessId, Providers, FileGuid),
    FileShareId2 = api_test_utils:share_file_and_sync_file_attrs(P1, SpaceOwnerSessId, Providers, FileGuid),

    FileDetailsWithShares = OriginalFileAttrs#file_attr{shares = [FileShareId2, FileShareId1 | OriginalShares]},

    SpaceId = oct_background:get_space_id(space_krk_par),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    SpaceShareId = api_test_utils:share_file_and_sync_file_attrs(P1, SpaceOwnerSessId, Providers, SpaceGuid),
    ShareSpaceGuid = file_id:guid_to_share_guid(SpaceGuid, SpaceShareId),

    ShareSpaceDetails = get_space_dir_attrs(paris, SpaceGuid, ?SPACE_KRK_PAR),
    ExpJsonShareSpaceDetails = fun(Node) -> file_attrs_to_gs_json(Node, SpaceShareId, ShareSpaceDetails) end,

    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, SpaceShareId),
    ExpJsonShareFileDetails = fun(Node) -> file_attrs_to_gs_json(Node, SpaceShareId, FileDetailsWithShares) end,
    
    ShareRootFileGuid = file_id:guid_to_share_guid(FileGuid, FileShareId1),
    ExpJsonShareRootFileDetails = fun(Node) -> file_attrs_to_gs_json(Node, FileShareId1, FileDetailsWithShares) end,

    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = str_utils:format("Get instance for directly shared ~ts using gs public api", [FileType]),
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(ShareRootFileGuid, public),
            validate_result_fun = build_get_instance_validate_gs_call_fun(ExpJsonShareRootFileDetails),
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, FileShareId1, undefined
            )
        },
        #scenario_spec{
            name = str_utils:format("Get instance for directly shared ~ts using gs private api", [FileType]),
            type = gs_with_shared_guid_and_aspect_private,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(ShareRootFileGuid, private),
            validate_result_fun = fun(_, Result) -> ?assertEqual(?ERROR_UNAUTHORIZED, Result) end
        },
        #scenario_spec{
            name = str_utils:format("Get instance for indirectly shared ~ts using gs public api", [FileType]),
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(ShareFileGuid, public),
            validate_result_fun = build_get_instance_validate_gs_call_fun(ExpJsonShareFileDetails)
        },
        #scenario_spec{
            name = str_utils:format("Get instance for shared ?SPACE_KRK_PAR using gs public api"),
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(ShareSpaceGuid, public),
            validate_result_fun = build_get_instance_validate_gs_call_fun(ExpJsonShareSpaceDetails)
        }
    ])).


get_file_instance_on_provider_not_supporting_space_test(_Config) ->
    P2Id = oct_background:get_provider_id(paris),
    [P2Node] = oct_background:get_provider_nodes(paris),

    SpaceId = oct_background:get_space_id(space_krk),
    {FileType, _FilePath, FileGuid, _ShareId} = api_test_utils:create_shared_file_in_space_krk(),

    ValidateGsCallResultFun = fun(_, Result) ->
        ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(SpaceId, P2Id), Result)
    end,

    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = str_utils:format("Get instance for ~ts on provider not supporting user using gs api", [
                FileType
            ]),
            type = gs,
            target_nodes = [P2Node],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(FileGuid, private),
            validate_result_fun = ValidateGsCallResultFun,
            data_spec = undefined
        }
    ])).


%% @private
-spec file_attrs_to_gs_json(node(), undefined | od_share:id(), #file_attr{}) -> map().
file_attrs_to_gs_json(Node, ShareId, #file_attr{guid = FileGuid} = FileAttr) ->
    ProviderId = opw_test_rpc:get_provider_id(Node),
    CurrentJson = api_test_utils:file_attr_to_json(ShareId, gs, ProviderId, FileAttr),
    DeprecatedFileAttr = api_test_utils:replace_attrs_with_deprecated(CurrentJson),
    JsonFileAttr = maps:with(
        [onedata_file:attr_name_to_json(A) || A <- ?DEPRECATED_ALL_FILE_ATTRS] ++
        [onedata_file:attr_name_to_json(deprecated, A) || A <- ?DEPRECATED_ALL_FILE_ATTRS],
    maps:merge(CurrentJson, DeprecatedFileAttr)),
    JsonFileAttr#{
        <<"gri">> => gri:serialize(#gri{
            type = op_file,
            id = file_id:guid_to_share_guid(FileGuid, ShareId),
            aspect = instance,
            scope = case ShareId of
                undefined -> private;
                _ -> public
            end
        }),
        <<"revision">> => 1
    }.


%% @private
-spec build_get_instance_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_instance_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data}) ->
        {GriId, _} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data),

        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = instance, scope = Scope}
        }
    end.


%% @private
-spec build_get_instance_validate_gs_call_fun(fun((node()) -> ExpJsonDetails :: map())) ->
    onenv_api_test_runner:validate_call_result_fun().
build_get_instance_validate_gs_call_fun(ExpJsonDetailsFun) ->
    fun(#api_test_ctx{node = Node}, Result) ->
        {ok, ResultMap} = ?assertMatch({ok, _}, Result),
        % do not compare size in attrs, as stats may not be fully calculated yet (which is normal and expected)
        %% @TODO VFS-11380 analyze why ctime is updated here and whether it should be
        ?assertEqual(
            maps:without([<<"size">>, <<"ctime">>], ExpJsonDetailsFun(Node)),
            maps:without([<<"size">>, <<"ctime">>], ResultMap))
    end.


%% @private
-spec get_space_dir_attrs(oct_background:entity_selector(), file_id:file_guid(), od_space:name()) -> #file_attr{}.
get_space_dir_attrs(ProviderSelector, SpaceDirGuid, SpaceName) ->
    {ok, SpaceAttrs} = ?assertMatch(
        {ok, _},
        file_test_utils:get_attrs(oct_background:get_random_provider_node(ProviderSelector), SpaceDirGuid),
        ?ATTEMPTS
    ),
    SpaceAttrs#file_attr{
        name = SpaceName,
        parent_guid = undefined,
        active_permissions_type = posix,
        eff_protection_flags = ?no_flags_mask,
        eff_qos_inheritance_path = ?none_inheritance_path,
        eff_dataset_inheritance_path = ?none_inheritance_path,
        has_custom_metadata = false
    }.


%%%===================================================================
%%% Update file instance test functions
%%%===================================================================


update_file_instance_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),

    {FileType, _FilePath, FileGuid, ShareId} = api_test_utils:create_and_sync_shared_file_in_space_krk_par(
        8#707
    ),
    ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    GetMode = fun(Node) ->
        {ok, #file_attr{mode = Mode}} = file_test_utils:get_attrs(Node, FileGuid),
        Mode
    end,

    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = str_utils:format("Update ~ts instance using gs private api", [FileType]),
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK_PAR,

            prepare_args_fun = build_update_file_instance_test_prepare_gs_args_fun(FileGuid, private),
            validate_result_fun = fun(_, Result) ->
                ?assertEqual(ok, Result)
            end,
            verify_fun = fun
                (expected_failure, #api_test_ctx{node = TestNode}) ->
                    ?assertMatch(8#707, GetMode(TestNode), ?ATTEMPTS),
                    true;
                (expected_success, #api_test_ctx{data = Data}) ->
                    PosixPerms = maps:get(<<"posixPermissions">>, Data, <<"0707">>),
                    Mode = binary_to_integer(PosixPerms, 8),
                    lists:foreach(fun(Node) -> ?assertMatch(Mode, GetMode(Node), ?ATTEMPTS) end, Providers),
                    true
            end,

            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                FileGuid, ShareId, update_file_instance_test_data_spec()
            )
        },
        #scenario_spec{
            name = str_utils:format("Update ~ts instance using gs public api", [FileType]),
            type = gs_not_supported,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_update_file_instance_test_prepare_gs_args_fun(ShareGuid, public),
            validate_result_fun = fun(_TestCaseCtx, Result) ->
                ?assertEqual(?ERROR_NOT_SUPPORTED, Result)
            end,
            data_spec = update_file_instance_test_data_spec()
        }
    ])).


update_file_instance_on_provider_not_supporting_space_test(_Config) ->
    P2Id = oct_background:get_provider_id(paris),
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    SpaceId = oct_background:get_space_id(space_krk),
    {FileType, _FilePath, FileGuid, _ShareId} = api_test_utils:create_shared_file_in_space_krk(),

    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = str_utils:format("Update ~ts instance on provider not supporting user using gs api", [
                FileType
            ]),
            type = gs,
            target_nodes = [P2Node],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK,
            verify_fun = fun(_, _) ->
                ?assertMatch(
                    {ok, #file_attr{mode = 8#777}},
                    file_test_utils:get_attrs(P1Node, FileGuid),
                    ?ATTEMPTS
                ),
                true
            end,
            prepare_args_fun = build_update_file_instance_test_prepare_gs_args_fun(FileGuid, private),
            validate_result_fun = fun(_, Result) ->
                ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(SpaceId, P2Id), Result)
            end,
            data_spec = update_file_instance_test_data_spec()
        }
    ])).


%% @private
-spec update_file_instance_test_data_spec() -> onenv_api_test_runner:data_spec().
update_file_instance_test_data_spec() ->
    #data_spec{
        required = [<<"posixPermissions">>],
        correct_values = #{<<"posixPermissions">> => [
            <<"0000">>, <<"0111">>, <<"0544">>, <<"0707">>
        ]},
        bad_values = [
            {<<"posixPermissions">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"posixPermissions">>)},
            {<<"posixPermissions">>, <<"integer">>, ?ERROR_BAD_VALUE_INTEGER(<<"posixPermissions">>)},
            {<<"posixPermissions">>, <<"0888">>, ?ERROR_BAD_VALUE_INTEGER(<<"posixPermissions">>)},
            {<<"posixPermissions">>, <<"888">>, ?ERROR_BAD_VALUE_INTEGER(<<"posixPermissions">>)},
            {<<"posixPermissions">>, <<"77777">>,
                ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"posixPermissions">>, 0, 8#777)}
        ]
    }.


%% @private
-spec build_update_file_instance_test_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_update_file_instance_test_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),

        #gs_args{
            operation = update,
            gri = #gri{type = op_file, id = GriId, aspect = instance, scope = Scope},
            data = Data1
        }
    end.


%%%===================================================================
%%% Delete file instance test functions
%%%===================================================================


delete_file_instance_test(Config) ->
    [P1] = oct_background:get_provider_nodes(krakow),
    [P2] = oct_background:get_provider_nodes(paris),
    Providers = [P1, P2],

    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),
    SpaceOwnerSessId = oct_background:get_user_session_id(user2, krakow),

    TopDirPath = filename:join(["/", ?SPACE_KRK_PAR, ?RANDOM_FILE_NAME()]),
    {ok, TopDirGuid} = lfm_proxy:mkdir(P1, UserSessIdP1, TopDirPath, 8#704),
    TopDirShareId = api_test_utils:share_file_and_sync_file_attrs(P1, SpaceOwnerSessId, Providers, TopDirGuid),
    TopDirShareGuid = file_id:guid_to_share_guid(TopDirGuid, TopDirShareId),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),

    MemRef = api_test_memory:init(),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [
                    user2,  % space owner - doesn't need any perms
                    user3
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1],
                forbidden_in_space = [{user4, ?ERROR_POSIX(?EACCES)}]  % forbidden by file perms
            },

            setup_fun = build_delete_instance_setup_fun(MemRef, TopDirPath, FileType),
            verify_fun = build_delete_instance_verify_fun(MemRef, Config),

            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Delete ~ts instance using rest api", [FileType]),
                    type = rest,
                    prepare_args_fun = build_delete_instance_test_prepare_rest_args_fun({mem_ref, MemRef}),
                    validate_result_fun = fun(_, {ok, RespCode, _RespHeaders, _RespBody}) ->
                        ?assertEqual(?HTTP_204_NO_CONTENT, RespCode)
                    end
                },
                #scenario_template{
                    name = str_utils:format("Delete ~ts instance using gs private api", [FileType]),
                    type = gs,
                    prepare_args_fun = build_delete_instance_test_prepare_gs_args_fun({mem_ref, MemRef}, private),
                    validate_result_fun = fun(_, Result) ->
                        ?assertEqual(ok, Result)
                    end
                }
            ],

            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                TopDirGuid, TopDirShareId, undefined
            )
        },
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Delete shared ~ts instance using rest api", [FileType]),
                    type = rest_not_supported,
                    prepare_args_fun = build_delete_instance_test_prepare_rest_args_fun({guid, TopDirShareGuid}),
                    validate_result_fun = fun(_TestCaseCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
                        ?assertEqual(errors:to_http_code(?ERROR_NOT_SUPPORTED), RespCode),
                        ?assertEqual(?REST_ERROR(?ERROR_NOT_SUPPORTED), RespBody)
                    end
                }
                #scenario_template{
                    name = str_utils:format("Delete shared ~ts instance using gs public api", [FileType]),
                    type = gs_not_supported,
                    prepare_args_fun = build_delete_instance_test_prepare_gs_args_fun({guid, TopDirShareGuid}, public),
                    validate_result_fun = fun(_TestCaseCtx, Result) ->
                        ?assertEqual(?ERROR_NOT_SUPPORTED, Result)
                    end
                }
            ]
        }
    ])).


delete_file_instance_at_path_test(Config) ->
    [P1] = oct_background:get_provider_nodes(krakow),
    [P2] = oct_background:get_provider_nodes(paris),
    Providers = [P1, P2],

    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),
    SpaceOwnerSessId = oct_background:get_user_session_id(user2, krakow),

    TopDirName = ?RANDOM_FILE_NAME(),
    TopDirPath = filename:join(["/", ?SPACE_KRK_PAR, TopDirName]),
    {ok, TopDirGuid} = lfm_proxy:mkdir(P1, UserSessIdP1, TopDirPath, 8#704),
    TopDirShareId = api_test_utils:share_file_and_sync_file_attrs(P1, SpaceOwnerSessId, Providers, TopDirGuid),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),

    DataSpec = #data_spec{
        optional = [<<"path">>],
        correct_values = #{
            <<"path">> => [
                filename_only_relative_to_parent_dir_placeholder,
                directory_and_filename_relative_to_space_root_dir_placeholder,
                directory_and_filename_relative_to_space_id_placeholder
            ]
        },
        bad_values = [
            {<<"path">>, <<"/a/b/\0null\0/">>, ?ERROR_BAD_VALUE_FILE_PATH},
            {<<"path">>, nonexistent_path, ?ERROR_POSIX(?ENOENT)}
        ]
    },

    MemRef = api_test_memory:init(),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [
                    user2,  % space owner - doesn't need any perms
                    user3
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1],
                forbidden_in_space = [{user4, ?ERROR_POSIX(?EACCES)}]  % forbidden by file perms
            },

            setup_fun = build_delete_instance_setup_fun(MemRef, TopDirPath, FileType),
            verify_fun = build_delete_instance_verify_fun(MemRef, Config),

            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Delete ~ts at path instance using rest api", [FileType]),
                    type = rest,
                    prepare_args_fun = build_delete_instance_at_path_test_prepare_rest_args_fun(
                        MemRef, TopDirGuid, TopDirName),
                    validate_result_fun = fun(_, {ok, RespCode, _RespHeaders, _RespBody}) ->
                        ?assertEqual(?HTTP_204_NO_CONTENT, RespCode)
                    end
                }
            ],

            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                TopDirGuid, TopDirShareId, DataSpec
            )
        }
    ])).


%% @private
-spec build_delete_instance_at_path_test_prepare_rest_args_fun(
    api_test_memory:mem_ref(), file_id:file_guid(), file_meta:name()
) ->
    onenv_api_test_runner:prepare_args_fun().
build_delete_instance_at_path_test_prepare_rest_args_fun(MemRef, TopDirGuid, TopDirName) ->
    fun(#api_test_ctx{data = Data}) ->
        SpaceId = oct_background:get_space_id(space_krk_par),

        RootFileName = api_test_memory:get(MemRef, file_name),
        RootFileGuid = api_test_memory:get(MemRef, file_guid),

        {ParentGuidOrSpaceId, Path} = case maps:get(<<"path">>, Data, undefined) of
            filename_only_relative_to_parent_dir_placeholder ->
                {TopDirGuid, RootFileName};
            directory_and_filename_relative_to_space_root_dir_placeholder ->
                SpaceRootDirGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
                {SpaceRootDirGuid, filepath_utils:join([TopDirName, RootFileName])};
            directory_and_filename_relative_to_space_id_placeholder ->
                {space_id, filepath_utils:join([TopDirName, RootFileName])};
            nonexistent_path ->
                {space_id, ?RANDOM_FILE_NAME()};
            undefined ->
                {RootFileGuid, <<"">>};
            CustomPath ->
                {space_id, CustomPath}
        end,

        ParentId = case ParentGuidOrSpaceId of
            space_id ->
                SpaceId;
            Guid ->
                {ok, ObjectId} = file_id:guid_to_objectid(Guid),
                ObjectId
        end,

        DataWithoutPath = maps:remove(<<"path">>, Data),

        {Id, _} = api_test_utils:maybe_substitute_bad_id(ParentId, DataWithoutPath),
        RestPath = filepath_utils:join([<<"data">>, Id, <<"path">>, Path]),

        #rest_args{
            method = delete,
            path = RestPath
        }
    end.


delete_file_instance_on_provider_not_supporting_space_test(_Config) ->
    P2Id = oct_background:get_provider_id(paris),
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),

    SpaceId = oct_background:get_space_id(space_krk),
    {FileType, _FilePath, FileGuid, _ShareId} = api_test_utils:create_shared_file_in_space_krk(),

    ?assert(onenv_api_test_runner:run_tests([
        #scenario_spec{
            name = str_utils:format("Delete ~ts instance on provider not supporting user using gs api", [
                FileType
            ]),
            type = gs,
            target_nodes = [P2Node],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_KRK,

            prepare_args_fun = build_delete_instance_test_prepare_gs_args_fun({guid, FileGuid}, private),
            validate_result_fun = fun(_, Result) ->
                ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(SpaceId, P2Id), Result)
            end,
            verify_fun = fun(_, _) ->
                ?assertMatch({ok, _}, file_test_utils:get_attrs(P1Node, FileGuid), ?ATTEMPTS),
                true
            end
        }
    ])).


%% @private
-spec build_delete_instance_setup_fun(
    api_test_memory:mem_ref(),
    file_meta:path(),
    api_test_utils:file_type()
) ->
    onenv_api_test_runner:setup_fun().
build_delete_instance_setup_fun(MemRef, TopDirPath, FileType) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),
    SpaceOwnerSessIdP1 = oct_background:get_user_session_id(user2, krakow),

    fun() ->
        RootFileName = ?RANDOM_FILE_NAME(),
        Path = filename:join([TopDirPath, RootFileName]),
        {ok, RootFileGuid} = lfm_test_utils:create_file(FileType, P1Node, UserSessIdP1, Path, 8#704),
        RootFileShares = case api_test_utils:randomly_create_share(P1Node, SpaceOwnerSessIdP1, RootFileGuid) of
            undefined -> [];
            ShareId -> [ShareId]
        end,
        ?assertMatch(
            {ok, #file_attr{shares = RootFileShares}},
            file_test_utils:get_attrs(P2Node, RootFileGuid),
            ?ATTEMPTS
        ),
        api_test_memory:set(MemRef, file_name, RootFileName),
        api_test_memory:set(MemRef, file_guid, RootFileGuid),

        AllFiles = case FileType of
            <<"dir">> ->
                SubFiles = lists_utils:pmap(fun(Num) ->
                    {_, _, FileGuid, _} = api_test_utils:create_file_in_space_krk_par_with_additional_metadata(
                        Path, false, <<"file_or_dir_", Num>>
                    ),
                    FileGuid
                end, [$0, $1, $2, $3, $4]),

                [RootFileGuid | SubFiles];
            _ ->
                [RootFileGuid]
        end,
        api_test_memory:set(MemRef, all_files, AllFiles),

        AllShares = lists:foldl(fun(FileGuid, SharesAcc) ->
            {ok, #file_attr{shares = Shares}} = file_test_utils:get_attrs(P2Node, FileGuid),
            Shares ++ SharesAcc
        end, [], AllFiles),
        api_test_memory:set(MemRef, all_shares, lists:usort(AllShares))
    end.


%% @private
-spec build_delete_instance_verify_fun(
    api_test_memory:mem_ref(),
    onenv_api_test_runner:ct_config()
) ->
    onenv_api_test_runner:setup_fun().
build_delete_instance_verify_fun(MemRef, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    RandomNode = lists_utils:random_element(Nodes),

    fun(Expectation, _) ->
        ExpResult = case Expectation of
            expected_failure -> ok;
            expected_success -> {error, ?ENOENT}
        end,

        lists:foreach(fun(ShareId) ->
            ?assertMatch({ok, _}, get_share(RandomNode, ShareId), ?ATTEMPTS)
        end, api_test_memory:get(MemRef, all_shares)),

        lists:foreach(fun(Guid) ->
            lists:foreach(fun(Node) ->
                ?assertMatch(ExpResult, ?extract_ok(file_test_utils:get_attrs(Node, Guid)), ?ATTEMPTS)
            end, Nodes)
        end, api_test_memory:get(MemRef, all_files))
    end.


%% @private
-spec get_share(node(), od_share:id()) -> {ok, od_share:doc()} | errors:error().
get_share(Node, ShareId) ->
    rpc:call(Node, share_logic, get, [?ROOT_SESS_ID, ShareId]).


%% @private
-spec build_delete_instance_test_prepare_rest_args_fun(
    {guid, file_id:file_guid()} | {mem_ref, api_test_memory:mem_ref()}
) ->
    onenv_api_test_runner:prepare_args_fun().
build_delete_instance_test_prepare_rest_args_fun(MemRefOrGuid) ->
    fun(#api_test_ctx{data = Data}) ->
        BareGuid = ensure_guid(MemRefOrGuid),
        {ok, ObjectId} = file_id:guid_to_objectid(BareGuid),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(ObjectId, Data),

        #rest_args{
            method = delete,
            path = <<"data/", Id/binary>>
        }
    end.


%% @private
-spec build_delete_instance_test_prepare_gs_args_fun(
    {guid, file_id:file_guid()} | {mem_ref, api_test_memory:mem_ref()},
    gri:scope()
) ->
    onenv_api_test_runner:prepare_args_fun().
build_delete_instance_test_prepare_gs_args_fun(MemRefOrGuid, Scope) ->
    fun(#api_test_ctx{data = Data}) ->
        BareGuid = ensure_guid(MemRefOrGuid),
        {Id, _} = api_test_utils:maybe_substitute_bad_id(BareGuid, Data),

        #gs_args{
            operation = delete,
            gri = #gri{type = op_file, id = Id, aspect = instance, scope = Scope}
        }
    end.


%% @private
-spec ensure_guid({guid, file_id:file_guid()} | {mem_ref, api_test_memory:mem_ref()}) ->
    file_if:file_guid().
ensure_guid({guid, Guid}) -> Guid;
ensure_guid({mem_ref, MemRef}) -> api_test_memory:get(MemRef, file_guid).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "api_tests",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
