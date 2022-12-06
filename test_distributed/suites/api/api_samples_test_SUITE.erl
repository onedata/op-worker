%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning GraphSync API for fetching
%%% API samples for different resources.
%%% @end
%%%-------------------------------------------------------------------
-module(api_samples_test_SUITE).
-author("Lukasz Opiola").

-include("modules/fslogic/fslogic_common.hrl").
-include("api_test_runner.hrl").
-include("onenv_test_utils.hrl").
-include("api_file_test_utils.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/api_samples/common.hrl").
-include_lib("ctool/include/test/rest_api_samples_test_utils.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").


-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    public_file_api_samples_test/1,
    private_file_api_samples_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        public_file_api_samples_test,
        private_file_api_samples_test
    ]}
].

all() -> [
    {group, all_tests}
].

-define(RAND_NODE(), oct_background:get_random_provider_node(krakow)).
-define(USER_2_SESS(), oct_background:get_user_session_id(user2, krakow)).

-define(DUMMY_XROOTD_SERVER_DOMAIN, <<"xrootd.example.com">>).

-define(EXAMPLE_METADATA_SPEC, #metadata_spec{
    json = #{<<"some_json_key">> => <<"some_json_value">>},
    rdf = <<"<some_rdf>value</some_rdf>">>,
    xattrs = #{<<"xattr_key">> => <<"xattr_value">>}
}).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% Test functions
%%%===================================================================


public_file_api_samples_test(_Config) ->
    #object{
        guid = TopDirGuid,
        type = ?DIRECTORY_TYPE,
        name = TopDirName,
        shares = [TopDirShareId],
        children = [
            #object{
                guid = FileGuid,
                type = ?REGULAR_FILE_TYPE,
                shares = [FileShareId]
            },
            #object{
                guid = NestedDirGuid,
                type = ?DIRECTORY_TYPE,
                children = [
                    #object{
                        guid = NestedFileGuid,
                        type = ?REGULAR_FILE_TYPE
                    }
                ]
            }
        ]
    } = onenv_file_test_utils:create_and_sync_file_tree(user2, space_krk_par,
        #dir_spec{
            metadata = ?EXAMPLE_METADATA_SPEC,
            shares = [#share_spec{}],
            children = [
                #file_spec{
                    name = <<"file">>,
                    content = ?RAND_STR(),
                    metadata = ?EXAMPLE_METADATA_SPEC,
                    shares = [#share_spec{}]
                },
                #dir_spec{
                    name = <<"nested_dir">>,
                    metadata = ?EXAMPLE_METADATA_SPEC,
                    children = [
                        #file_spec{
                            name = <<"nested_file">>,
                            content = ?RAND_STR(),
                            metadata = ?EXAMPLE_METADATA_SPEC
                        }
                    ]
                }
            ]
        }
    ),

    public_file_api_samples_test_base(?REGULAR_FILE_TYPE, FileGuid, FileShareId, <<"/file">>),
    public_file_api_samples_test_base(?REGULAR_FILE_TYPE, FileGuid, TopDirShareId, filename:join([<<"/">>, TopDirName, <<"file">>])),
    public_file_api_samples_test_base(?REGULAR_FILE_TYPE, NestedFileGuid, TopDirShareId, filename:join([<<"/">>, TopDirName, <<"nested_dir">>, <<"nested_file">>])),
    public_file_api_samples_test_base(?DIRECTORY_TYPE, TopDirGuid, TopDirShareId, <<"/", TopDirName/binary>>),
    public_file_api_samples_test_base(?DIRECTORY_TYPE, NestedDirGuid, TopDirShareId, filename:join([<<"/">>, TopDirName, <<"nested_dir">>])).


public_file_api_samples_test_base(FileType, FileGuid, ShareId, FilePathInShare) ->
    public_file_api_samples_test_base(FileType, FileGuid, ShareId, FilePathInShare, xrootd_enabled),
    public_file_api_samples_test_base(FileType, FileGuid, ShareId, FilePathInShare, xrootd_disabled).

public_file_api_samples_test_base(FileType, FileGuid, ShareId, FilePathInShare, XrootdStatus) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],

    OpenDataXrootdServerDomain = case XrootdStatus of
        xrootd_enabled -> ?DUMMY_XROOTD_SERVER_DOMAIN;
        xrootd_disabled -> undefined
    end,
    ozw_test_rpc:set_env(open_data_xrootd_server_domain, OpenDataXrootdServerDomain),

    ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    SpaceId = oct_background:get_space_id(space_krk_par),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [nobody, user1, user2, user3, user4]
            },
            scenario_templates = [
                #scenario_template{
                    name = <<"Get public file API samples using gs api">>,
                    type = gs,
                    prepare_args_fun = fun(#api_test_ctx{node = Node, data = Data0}) ->
                        % clear cached values for the xrootd config
                        rpc:call(Node, node_cache, clear, [{service_configuration, onezone}]),
                        {TestedId, _} = api_test_utils:maybe_substitute_bad_id(ShareGuid, Data0),
                        #gs_args{
                            operation = get,
                            gri = #gri{type = op_file, id = TestedId, aspect = api_samples, scope = public}
                        }
                    end,
                    validate_result_fun = fun(_, {ok, Result}) ->
                        ExpectedApiRoot = str_utils:format_bin("https://~s/api/v3/onezone", [ozw_test_rpc:get_domain()]),
                        rest_api_samples_test_utils:verify_structure(
                            Result, ExpectedApiRoot, exp_operation_list(public, FileType)
                        ),
                        rest_api_samples_test_utils:test_samples(
                            Result, get_user_access_token(), cacert_opts(), ShareGuid, fun build_sample_test_spec/1
                        ),
                        case XrootdStatus of
                            xrootd_enabled ->
                                ?assert(maps:is_key(<<"xrootd">>, Result)),
                                verify_xrootd_api_samples(FileType, SpaceId, ShareId, FilePathInShare, maps:get(<<"xrootd">>, Result));
                            xrootd_disabled ->
                                ?assertNot(maps:is_key(<<"xrootd">>, Result))
                        end
                    end
                }
            ],
            data_spec = #data_spec{
                bad_values = [{bad_id, <<"NonExistentFile">>, ?ERROR_BAD_VALUE_IDENTIFIER(<<"id">>)}]
            }
        }
    ])).


private_file_api_samples_test(_Config) ->
    #object{
        guid = DirGuid,
        type = ?DIRECTORY_TYPE,
        children = [
            #object{
                guid = FileGuid,
                type = ?REGULAR_FILE_TYPE
            },
            #object{
                guid = SymlinkGuid,
                type = ?SYMLINK_TYPE
            }
        ]
    } = onenv_file_test_utils:create_and_sync_file_tree(user2, space_krk_par,
        #dir_spec{
            metadata = ?EXAMPLE_METADATA_SPEC,
            children = [
                #file_spec{
                    name = <<"file">>,
                    content = ?RAND_STR(),
                    metadata = ?EXAMPLE_METADATA_SPEC
                },
                #symlink_spec{
                    name = <<"symlink">>,
                    symlink_value = <<"some/symlink/value">>
                }
            ]
        }
    ),

    private_file_api_samples_test_base(?SYMLINK_TYPE, SymlinkGuid),
    private_file_api_samples_test_base(?REGULAR_FILE_TYPE, FileGuid),
    private_file_api_samples_test_base(?DIRECTORY_TYPE, DirGuid).

private_file_api_samples_test_base(FileType, FileGuid) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Providers = [P1Node, P2Node],

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [
                    user2, % space owner - doesn't need any perms
                    user3  % files owner (see fun create_shared_file/1)
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]
            },
            scenario_templates = [
                #scenario_template{
                    name = <<"Get private file API samples using gs api">>,
                    type = gs,
                    prepare_args_fun = fun(#api_test_ctx{data = Data0}) ->
                        {TestedId, _} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),
                        #gs_args{
                            operation = get,
                            gri = #gri{type = op_file, id = TestedId, aspect = api_samples, scope = private}
                        }
                    end,
                    validate_result_fun = fun(#api_test_ctx{node = Node}, {ok, Result}) ->
                        ExpApiRoot = str_utils:format_bin("https://~s:443/api/v3/oneprovider", [opw_test_rpc:get_provider_domain(Node)]),
                        rest_api_samples_test_utils:verify_structure(Result, ExpApiRoot, exp_operation_list(private, FileType))
                    end
                }
            ],
            data_spec = #data_spec{
                bad_values = [{bad_id, <<"NonExistentFile">>, ?ERROR_BAD_VALUE_IDENTIFIER(<<"id">>)}]
            }
        }
    ])),

    % testing samples is deferred till the end of the test as one of the operations deletes the file
    ApiSamples = opw_test_rpc:call(?RAND_NODE(), private_file_api_samples, generate_for, [?USER_2_SESS(), FileGuid]),
    rest_api_samples_test_utils:test_samples(
        ApiSamples, get_user_access_token(), cacert_opts(), FileGuid, fun build_sample_test_spec/1
    ).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec exp_operation_list(public | private, file_meta:type()) -> rest_api_samples_test_utils:operation_listing().
exp_operation_list(public, ?DIRECTORY_TYPE) -> [
    {'GET', <<"get_shared_data">>, <<"Download directory (tar)">>},
    {'GET', <<"get_shared_data">>, <<"List directory files and subdirectories">>},
    {'GET', <<"get_shared_data">>, <<"Get attributes">>},
    {'GET', <<"get_shared_data">>, <<"Get extended attributes (xattrs)">>},
    {'GET', <<"get_shared_data">>, <<"Get JSON metadata">>},
    {'GET', <<"get_shared_data">>, <<"Get RDF metadata">>}
];
exp_operation_list(public, ?REGULAR_FILE_TYPE) -> [
    {'GET', <<"get_shared_data">>, <<"Download file content">>},
    {'GET', <<"get_shared_data">>, <<"Get attributes">>},
    {'GET', <<"get_shared_data">>, <<"Get extended attributes (xattrs)">>},
    {'GET', <<"get_shared_data">>, <<"Get JSON metadata">>},
    {'GET', <<"get_shared_data">>, <<"Get RDF metadata">>}
];
exp_operation_list(private, ?DIRECTORY_TYPE) -> [
    {'GET', <<"download_file_content">>, <<"Download directory (tar)">>},
    {'GET', <<"list_children">>, <<"List directory files and subdirectories">>},
    {'POST', <<"create_file">>, <<"Create file in directory">>},
    {'DELETE', <<"remove_file">>, <<"Remove file">>},
    {'GET', <<"get_attrs">>, <<"Get attributes">>},
    {'GET', <<"get_json_metadata">>, <<"Get JSON metadata">>},
    {'PUT', <<"set_json_metadata">>, <<"Set JSON metadata">>},
    {'DELETE', <<"remove_json_metadata">>, <<"Remove JSON metadata">>},
    {'GET', <<"get_rdf_metadata">>, <<"Get RDF metadata">>},
    {'PUT', <<"set_rdf_metadata">>, <<"Set RDF metadata">>},
    {'DELETE', <<"remove_rdf_metadata">>, <<"Remove RDF metadata">>},
    {'GET', <<"get_xattrs">>, <<"Get extended attributes (xattrs)">>},
    {'PUT', <<"set_xattr">>, <<"Set extended attribute (xattr)">>},
    {'DELETE', <<"remove_xattrs">>, <<"Remove extended attributes (xattrs)">>}
];
exp_operation_list(private, ?REGULAR_FILE_TYPE) -> [
    {'GET', <<"download_file_content">>, <<"Download file content">>},
    {'PUT', <<"update_file_content">>, <<"Update file content">>},
    {'GET', <<"get_file_hardlinks">>, <<"Get file hard links">>},
    {'DELETE', <<"remove_file">>, <<"Remove file">>},
    {'GET', <<"get_attrs">>, <<"Get attributes">>},
    {'GET', <<"get_json_metadata">>, <<"Get JSON metadata">>},
    {'PUT', <<"set_json_metadata">>, <<"Set JSON metadata">>},
    {'DELETE', <<"remove_json_metadata">>, <<"Remove JSON metadata">>},
    {'GET', <<"get_rdf_metadata">>, <<"Get RDF metadata">>},
    {'PUT', <<"set_rdf_metadata">>, <<"Set RDF metadata">>},
    {'DELETE', <<"remove_rdf_metadata">>, <<"Remove RDF metadata">>},
    {'GET', <<"get_xattrs">>, <<"Get extended attributes (xattrs)">>},
    {'PUT', <<"set_xattr">>, <<"Set extended attribute (xattr)">>},
    {'DELETE', <<"remove_xattrs">>, <<"Remove extended attributes (xattrs)">>}
];
exp_operation_list(private, ?SYMLINK_TYPE) -> [
    {'GET', <<"get_symlink_value">>, <<"Get symbolic link value">>},
    {'GET', <<"get_file_hardlinks">>, <<"Get file hard links">>},
    {'DELETE', <<"remove_file">>, <<"Remove file">>},
    {'GET', <<"get_attrs">>, <<"Get attributes">>}
].


%% @private
-spec build_sample_test_spec(rest_api_request_sample:name()) -> rest_api_samples_test_utils:sample_test_spec().
build_sample_test_spec(<<"Download directory (tar)">>) -> #sample_test_spec{
    verify_fun = fun(_Guid, ResultBody) ->
        ?assertMatch({ok, _}, erl_tar:extract({binary, ResultBody}, [memory]))
    end
};
build_sample_test_spec(<<"Download file content">>) -> #sample_test_spec{
    verify_fun = fun(Guid, ResultBody) ->
        ?assertEqual(ResultBody, lfm_test_utils:read_file(?RAND_NODE(), ?USER_2_SESS(), Guid, 1000))
    end
};
build_sample_test_spec(<<"List directory files and subdirectories">>) -> #sample_test_spec{
    verify_fun = fun(Guid, ResultBody) ->
        {ok, Children} = lfm_proxy:get_children(?RAND_NODE(), ?USER_2_SESS(), ?FILE_REF(Guid), 0, 100),
        ExpectedChildren = lists:map(fun({ChildGuid, ChildName}) ->
            {ok, ObjectId} = file_id:guid_to_objectid(ChildGuid),
            #{<<"file_id">> => ObjectId, <<"name">> => ChildName}
        end, Children),
        ?assertMatch(#{<<"children">> := _}, json_utils:decode(ResultBody)),
        ?assertMatch(ExpectedChildren, maps:get(<<"children">>, json_utils:decode(ResultBody)))
    end
};
build_sample_test_spec(<<"Create file in directory">>) -> #sample_test_spec{
    setup_fun = fun(Guid) ->
        Filename = generator:gen_name(),
        {Guid, Filename}
    end,
    substitute_placeholder_fun = fun({_Guid, Filename}, <<"$NAME">>) -> Filename end,
    verify_fun = fun({Guid, Filename}, _ResultBody) ->
        {ok, Children} = lfm_proxy:get_children(?RAND_NODE(), ?USER_2_SESS(), ?FILE_REF(Guid), 0, 100),
        ?assertMatch({ok, _}, lists_utils:find(fun({_ChildGuid, ChildName}) -> ChildName == Filename end, Children))
    end
};
build_sample_test_spec(<<"Update file content">>) -> #sample_test_spec{
    substitute_placeholder_fun = fun(_Guid, <<"$NEW_CONTENT">>) -> <<"file_content_set_in_test">> end,
    verify_fun = fun(Guid, _ResultBody) ->
        ?assertEqual(<<"file_content_set_in_test">>, lfm_test_utils:read_file(?RAND_NODE(), ?USER_2_SESS(), Guid, 1000))
    end
};
build_sample_test_spec(<<"Remove file">>) -> #sample_test_spec{
    testing_priority = 20,
    verify_fun = fun(Guid, _ResultBody) ->
        ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(?RAND_NODE(), ?USER_2_SESS(), ?FILE_REF(Guid)), ?ATTEMPTS)
    end
};
build_sample_test_spec(<<"Get file hard links">>) -> #sample_test_spec{
    verify_fun = fun(Guid, ResultBody) ->
        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        ?assertEqual([ObjectId], json_utils:decode(ResultBody))
    end
};
build_sample_test_spec(<<"Get attributes">>) -> #sample_test_spec{
    verify_fun = fun(Guid, ResultBody) ->
        {ok, #file_attr{name = Name}} = lfm_proxy:stat(?RAND_NODE(), ?USER_2_SESS(), ?FILE_REF(Guid)),
        ?assertMatch(#{<<"name">> := Name}, json_utils:decode(ResultBody))
    end
};
build_sample_test_spec(<<"Get JSON metadata">>) -> #sample_test_spec{
    verify_fun = fun(Guid, ResultBody) ->
        ?assertEqual(element(2, {ok, _} = get_custom_metadata(Guid, json)), json_utils:decode(ResultBody))
    end
};
build_sample_test_spec(<<"Set JSON metadata">>) -> #sample_test_spec{
    substitute_placeholder_fun = fun(_Guid, <<"$METADATA">>) ->
        json_utils:encode(#{<<"some_json_key">> => <<"value_set_in_test">>})
    end,
    verify_fun = fun(Guid, _ResultBody) ->
        ?assertEqual({ok, #{<<"some_json_key">> => <<"value_set_in_test">>}}, get_custom_metadata(Guid, json))
    end
};
build_sample_test_spec(<<"Remove JSON metadata">>) -> #sample_test_spec{
    testing_priority = 10,
    verify_fun = fun(Guid, _ResultBody) ->
        ?assertEqual(?ERROR_POSIX(?ENODATA), get_custom_metadata(Guid, json))
    end
};
build_sample_test_spec(<<"Get RDF metadata">>) -> #sample_test_spec{
    verify_fun = fun(Guid, ResultBody) ->
        ?assertEqual(element(2, {ok, _} = get_custom_metadata(Guid, rdf)), ResultBody)
    end
};
build_sample_test_spec(<<"Set RDF metadata">>) -> #sample_test_spec{
    substitute_placeholder_fun = fun(_Guid, <<"$METADATA">>) ->
        <<"<rdf>metadata_set_in_test</rdf>">>
    end,
    verify_fun = fun(Guid, _ResultBody) ->
        ?assertEqual({ok, <<"<rdf>metadata_set_in_test</rdf>">>}, get_custom_metadata(Guid, rdf))
    end
};
build_sample_test_spec(<<"Remove RDF metadata">>) -> #sample_test_spec{
    testing_priority = 10,
    verify_fun = fun(Guid, _ResultBody) ->
        ?assertEqual(?ERROR_POSIX(?ENODATA), get_custom_metadata(Guid, rdf))
    end
};
build_sample_test_spec(<<"Get extended attributes (xattrs)">>) -> #sample_test_spec{
    verify_fun = fun(Guid, ResultBody) ->
        ?assertEqual(get_xattrs(Guid), json_utils:decode(ResultBody))
    end
};
build_sample_test_spec(<<"Set extended attribute (xattr)">>) -> #sample_test_spec{
    substitute_placeholder_fun = fun(_Guid, <<"$XATTRS">>) ->
        json_utils:encode(#{<<"xattr1">> => <<"value1">>, <<"xattr2">> => <<"value2">>})
    end,
    verify_fun = fun(Guid, _ResultBody) ->
        ?assert(maps_utils:is_submap(#{<<"xattr1">> => <<"value1">>, <<"xattr2">> => <<"value2">>}, get_xattrs(Guid)))
    end
};
build_sample_test_spec(<<"Remove extended attributes (xattrs)">>) -> #sample_test_spec{
    testing_priority = 10,
    substitute_placeholder_fun = fun(_Guid, <<"$KEY_LIST">>) ->
        json_utils:encode([<<"xattr1">>, <<"xattr2">>])
    end,
    verify_fun = fun(Guid, _ResultBody) ->
        ?assertNot(maps:is_key(<<"xattr_created_in_test">>, get_xattrs(Guid)))
    end
};
build_sample_test_spec(<<"Get symbolic link value">>) -> #sample_test_spec{
    verify_fun = fun(Guid, ResultBody) ->
        {ok, SymlinkValue} = lfm_proxy:read_symlink(?RAND_NODE(), ?USER_2_SESS(), ?FILE_REF(Guid)),
        ?assertEqual(SymlinkValue, ResultBody)
    end
}.


%% @private
verify_xrootd_api_samples(FileType, SpaceId, ShareId, FilePath, SamplesJson) ->
    ExpectedServerURL = str_utils:format_bin("root://~s", [?DUMMY_XROOTD_SERVER_DOMAIN]),
    ExpectedFullDataPath = <<"/", SpaceId/binary, "/", SpaceId/binary, "/", ShareId/binary, FilePath/binary>>,

    OperationNames = lists:map(fun(Sample) ->
        ?assertMatch(#{<<"name">> := _, <<"description">> := _, <<"command">> := _}, Sample),
        CommandStr = str_utils:join_binary(maps:get(<<"command">>, Sample), <<" ">>),
        ?assert(nomatch /= string:find(CommandStr, ExpectedFullDataPath)),
        ?assert(nomatch /= string:find(CommandStr, ExpectedServerURL)),
        maps:get(<<"name">>, Sample)
    end, SamplesJson),
    ExpectedOperationNames = case FileType of
        ?REGULAR_FILE_TYPE -> [
            <<"Download file">>
        ];
        ?DIRECTORY_TYPE -> [
            <<"Download directory (recursively)">>,
            <<"List directory files and subdirectories">>
        ]
    end,
    ?assertEqual(OperationNames, ExpectedOperationNames).


%% @private
get_user_access_token() ->
    oct_background:get_user_access_token(user2).


%% @private
cacert_opts() ->
    rest_test_utils:cacerts_opts(oct_background:get_random_provider_node(krakow)).


%% @private
get_custom_metadata(Guid, MetadataType) ->
    opt_file_metadata:get_custom_metadata(?RAND_NODE(), ?USER_2_SESS(), ?FILE_REF(Guid), MetadataType, [], false).


%% @private
get_xattrs(Guid) ->
    lfm_test_utils:get_xattrs(?RAND_NODE(), ?USER_2_SESS(), Guid).

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
    Config2 = oct_background:update_background_config(Config),
    lfm_proxy:init(Config2, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
