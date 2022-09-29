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
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").


-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    public_file_api_samples_test/1,
    private_file_api_samples_test/1,
    
    verify_public_samples_directory_test/1,
    verify_public_samples_reg_file_test/1,
    verify_private_samples_directory_test/1,
    verify_private_samples_reg_file_test/1,
    verify_private_samples_symlink_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        public_file_api_samples_test,
        private_file_api_samples_test,

        verify_public_samples_directory_test,
        verify_public_samples_reg_file_test,
        verify_private_samples_directory_test,
        verify_private_samples_reg_file_test,
        verify_private_samples_symlink_test
    ]}
].

all() -> [
    {group, all_tests}
].

-define(DUMMY_XROOTD_SERVER_DOMAIN, <<"xrootd.example.com">>).
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
            shares = [#share_spec{}],
            children = [
                #file_spec{
                    name = <<"file">>,
                    shares = [#share_spec{}]
                },
                #dir_spec{
                    name = <<"nested_dir">>,
                    children = [
                        #file_spec{
                            name = <<"nested_file">>
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
                        case XrootdStatus of
                            xrootd_enabled ->
                                ?assertEqual(lists:sort([<<"rest">>, <<"xrootd">>]), lists:sort(maps:keys(Result))),
                                verify_xrootd_api_samples(FileType, SpaceId, ShareId, FilePathInShare, maps:get(<<"xrootd">>, Result)),
                                verify_public_rest_api_samples(FileType, ShareGuid, maps:get(<<"rest">>, Result));
                            xrootd_disabled ->
                                ?assertEqual([<<"rest">>], maps:keys(Result)),
                                verify_public_rest_api_samples(FileType, ShareGuid, maps:get(<<"rest">>, Result))
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
            children = [
                #file_spec{
                    name = <<"file">>
                },
                #symlink_spec{
                    name = <<"symlink">>,
                    symlink_value = <<"some/symlink/value">>
                }
            ]
        }
    ),
    
    private_file_api_samples_test_base(?DIRECTORY_TYPE, DirGuid),
    private_file_api_samples_test_base(?REGULAR_FILE_TYPE, FileGuid),
    private_file_api_samples_test_base(?SYMLINK_TYPE, SymlinkGuid).

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
                        ?assertEqual([<<"rest">>], maps:keys(Result)),
                        verify_private_rest_api_samples(Node, FileType, FileGuid, maps:get(<<"rest">>, Result))
                    end
                }
            ],
            data_spec = #data_spec{
                bad_values = [{bad_id, <<"NonExistentFile">>, ?ERROR_BAD_VALUE_IDENTIFIER(<<"id">>)}]
            }
        }
    ])).


%%%===================================================================
%%% Verify samples test functions
%%%===================================================================

verify_public_samples_directory_test(_Config) ->
    verify_all_samples(?DIRECTORY_TYPE, public).

verify_public_samples_reg_file_test(_Config) ->
    verify_all_samples(?REGULAR_FILE_TYPE, public).

verify_private_samples_directory_test(_Config) ->
    verify_all_samples(?DIRECTORY_TYPE, private).

verify_private_samples_reg_file_test(_Config) ->
    verify_all_samples(?REGULAR_FILE_TYPE, private).

verify_private_samples_symlink_test(_Config) ->
    verify_all_samples(?SYMLINK_TYPE, private).


verify_all_samples(FileType, Scope) ->
    Guid = setup_verify_sample_env(FileType, Scope),
    #rest_api_samples{samples = Samples, api_root = ApiRoot} = get_samples(Guid, Scope),
    RemoveFileSample = lists:foldl(fun(#rest_api_request_sample{path = PathSuffix} = Sample, Acc) ->
        UpdatedSample = Sample#rest_api_request_sample{path = <<ApiRoot/binary, PathSuffix/binary>>},
        % call remove file sample after all tests, as it will remove the file
        case UpdatedSample#rest_api_request_sample.name of
            <<"Remove file">> -> 
                UpdatedSample;
            _ -> 
                verify_sample(UpdatedSample, Guid),
                Acc
        end
    end, undefined, Samples),
    case RemoveFileSample of
        undefined -> ok;
        _ -> verify_sample(RemoveFileSample, Guid)
    end.
    

verify_sample(#rest_api_request_sample{name = <<"Get attributes">>} = Sample, Guid) ->
    {ok, #file_attr{name = Name}} = lfm_proxy:stat(
        oct_background:get_random_provider_node(krakow), 
        oct_background:get_user_session_id(user2, krakow), 
        ?FILE_REF(Guid)
    ),
    VerifyFun = fun(ResultBody) ->
        DecodedResult = json_utils:decode(ResultBody),
        ?assertMatch(#{<<"name">> := _}, DecodedResult),
        ?assertEqual(Name, maps:get(<<"name">>, DecodedResult))
    end,
    verify_sample_base(Sample, VerifyFun);
verify_sample(#rest_api_request_sample{name = <<"Remove file">>} = Sample, Guid) ->
    VerifyFun = fun(_ResultBody) ->
        ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(
            oct_background:get_random_provider_node(krakow), 
            oct_background:get_user_session_id(user2, krakow), 
            ?FILE_REF(Guid)
        ), ?ATTEMPTS)
    end,
    verify_sample_base(Sample, VerifyFun);
verify_sample(#rest_api_request_sample{name = <<"Get JSON metadata">>} = Sample, Guid) ->
    VerifyFun = fun(ResultBody) ->
        ?assertEqual(get_custom_metadata(Guid, json), {ok, json_utils:decode(ResultBody)})
    end,
    verify_sample_base(Sample, VerifyFun);
verify_sample(#rest_api_request_sample{name = <<"Set JSON metadata">>} = Sample, Guid) ->
    VerifyFun = fun(_ResultBody) ->
        ?assertEqual({ok, #{<<"some_json_key">> => <<"value_set_in_test">>}}, get_custom_metadata(Guid, json))
    end,
    verify_sample_base(Sample, VerifyFun, json_utils:encode(#{<<"some_json_key">> => <<"value_set_in_test">>}));
verify_sample(#rest_api_request_sample{name = <<"Remove JSON metadata">>} = Sample, Guid) ->
    VerifyFun = fun(_ResultBody) ->
        ?assertEqual(?ERROR_POSIX(?ENODATA), get_custom_metadata(Guid, json))
    end,
    verify_sample_base(Sample, VerifyFun);
verify_sample(#rest_api_request_sample{name = <<"Get RDF metadata">>} = Sample, Guid) ->
    VerifyFun = fun(ResultBody) ->
        ?assertEqual(get_custom_metadata(Guid, rdf), {ok, ResultBody})
    end,
    verify_sample_base(Sample, VerifyFun);
verify_sample(#rest_api_request_sample{name = <<"Set RDF metadata">>} = Sample, Guid) ->
    VerifyFun = fun(_ResultBody) ->
        ?assertEqual({ok, <<"rdf_metadata_set_in_test">>}, get_custom_metadata(Guid, rdf))
    end,
    verify_sample_base(Sample, VerifyFun, <<"rdf_metadata_set_in_test">>);
verify_sample(#rest_api_request_sample{name = <<"Remove RDF metadata">>} = Sample, Guid) ->
    VerifyFun = fun(_ResultBody) ->
        ?assertEqual(?ERROR_POSIX(?ENODATA), get_custom_metadata(Guid, rdf))
    end,
    verify_sample_base(Sample, VerifyFun);
verify_sample(#rest_api_request_sample{name = <<"Get extended attributes (xattrs)">>} = Sample, Guid) ->
    VerifyFun = fun(ResultBody) ->
        ?assertEqual(get_xattrs(Guid), json_utils:decode(ResultBody))
    end,
    verify_sample_base(Sample, VerifyFun);
verify_sample(#rest_api_request_sample{name = <<"Set extended attribute (xattr)">>} = Sample, Guid) ->
    VerifyFun = fun(_ResultBody) ->
        ?assertMatch(#{<<"xattr_created_in_test">> := <<"value">>}, get_xattrs(Guid))
    end,
    verify_sample_base(Sample, VerifyFun, json_utils:encode(#{<<"xattr_created_in_test">> => <<"value">>}));
verify_sample(#rest_api_request_sample{name = <<"Remove extended attributes (xattrs)">>} = Sample, Guid) ->
    VerifyFun = fun(_ResultBody) ->
        ?assertNotMatch(#{<<"xattr_key">> := <<"xattr_value">>}, get_xattrs(Guid))
    end,
    verify_sample_base(Sample, VerifyFun, json_utils:encode(#{<<"keys">> => [<<"xattr_key">>]}));
verify_sample(#rest_api_request_sample{name = <<"Create file in directory">>} = Sample, Guid) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user2, krakow),
    Filename = generator:gen_name(),
    VerifyFun = fun(_ResultBody) ->
        {ok, Children} = lfm_proxy:get_children(Node, SessId, ?FILE_REF(Guid), 0, 100),
        ?assertMatch({ok, _}, lists_utils:find(fun({_ChildGuid, ChildName}) -> ChildName == Filename end, Children))
    end,
    verify_sample_base(Sample, VerifyFun, <<>>, #{<<"$NAME">> => Filename});
verify_sample(#rest_api_request_sample{name = <<"List directory files and subdirectories">>} = Sample, Guid) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user2, krakow),
    VerifyFun = fun(ResultBody) ->
        {ok, Children} = lfm_proxy:get_children(Node, SessId, ?FILE_REF(Guid), 0, 100),
        ExpectedChildren = lists:map(fun({ChildGuid, ChildName}) ->
            {ok, ObjectId} = file_id:guid_to_objectid(ChildGuid),
            #{<<"file_id">> => ObjectId, <<"name">> => ChildName}
        end, Children),
        ?assertMatch(#{<<"children">> := _}, json_utils:decode(ResultBody)),
        ?assertMatch(ExpectedChildren, maps:get(<<"children">>, json_utils:decode(ResultBody)))
    end,
    verify_sample_base(Sample, VerifyFun);
verify_sample(#rest_api_request_sample{name = <<"Download directory (tar)">>} = Sample, _Guid) ->
    VerifyFun = fun(ResultBody) ->
        ?assertMatch({ok, _}, erl_tar:extract({binary, ResultBody}, [memory]))
    end,
    verify_sample_base(Sample, VerifyFun);
verify_sample(#rest_api_request_sample{name = <<"Download file content">>} = Sample, Guid) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user2, krakow),
    VerifyFun = fun(ResultBody) ->
        {ok, Handle} = lfm_proxy:open(Node, SessId, ?FILE_REF(Guid), read),
        {ok, ExpectedContent} = lfm_proxy:read(Node, Handle, 0, 100),
        ?assertMatch(ExpectedContent, ResultBody),
        lfm_proxy:close_all(Node)
    end,
    verify_sample_base(Sample, VerifyFun);
verify_sample(#rest_api_request_sample{name = <<"Update file content">>} = Sample, Guid) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user2, krakow),
    VerifyFun = fun(_ResultBody) ->
        {ok, Handle} = lfm_proxy:open(Node, SessId, ?FILE_REF(Guid), read),
        ?assertMatch({ok, <<"file_content_set_in_test">>}, lfm_proxy:read(Node, Handle, 0, 100)),
        lfm_proxy:close_all(Node)
    end,
    verify_sample_base(Sample, VerifyFun, <<"file_content_set_in_test">>);
verify_sample(#rest_api_request_sample{name = <<"Get file hard links">>} = Sample, Guid) ->
    VerifyFun = fun(ResultBody) ->
        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        ?assertEqual([ObjectId], json_utils:decode(ResultBody))
    end,
    verify_sample_base(Sample, VerifyFun);
verify_sample(#rest_api_request_sample{name = <<"Get symbolic link value">>} = Sample, Guid) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user2, krakow),
    VerifyFun = fun(ResultBody) ->
        {ok, SymlinkValue} = lfm_proxy:read_symlink(Node, SessId, ?FILE_REF(Guid)),
        ?assertEqual(SymlinkValue, ResultBody)
    end,
    verify_sample_base(Sample, VerifyFun);
verify_sample(Sample, _) ->
    throw({sample_test_not_implemented, Sample#rest_api_request_sample.name}).


verify_sample_base(Sample, VerifyFun) ->
    verify_sample_base(Sample, VerifyFun, <<>>).

verify_sample_base(Sample, VerifyFun, Body) ->
    verify_sample_base(Sample, VerifyFun, Body, #{}).

verify_sample_base(VerifyFun, #rest_api_request_sample{
    method = Method, 
    path = Path, 
    headers = Headers, 
    requires_authorization = RequiresAuthorization
}, Body, PlaceholderValues) ->
    AuthHeader = case RequiresAuthorization of
        true -> maps:from_list([rest_test_utils:user_token_header(oct_background:get_user_access_token(user2))]);
        false -> #{}
    end,
    SubstitutedPath = maps:fold(fun(Placeholder, Value, P) ->
        binary:replace(P, Placeholder, Value, [global])
    end, Path, PlaceholderValues),
    Opts = [{follow_redirect, true} | rest_test_utils:cacerts_opts(oct_background:get_random_provider_node(krakow))],
    {ok, Code, _, ResultBody} = ?assertMatch({ok, _, _, _}, http_client:request(
        normalize_method(Method),
        SubstitutedPath, 
        maps:merge(AuthHeader, Headers), 
        Body,
        Opts
    )),
    case Code > 300 of
        true ->
            ct:pal("Code: ~p~nResponse: ~p~n~nMethod: ~p~nPath: ~p~nBody: ~p~nHeaders: ~p~n", 
                [Code, ResultBody, Method, SubstitutedPath, Body, Headers]);
        false -> 
            ok
    end,
    VerifyFun(ResultBody).

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
verify_public_rest_api_samples(FileType, ShareGuid, SamplesJson) ->
    {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

    Samples = ?assertMatch(#rest_api_samples{}, jsonable_record:from_json(SamplesJson, rest_api_samples)),
    ExpectedApiRoot = str_utils:format_bin("https://~s/api/v3/onezone", [ozw_test_rpc:get_domain()]),
    ?assertEqual(ExpectedApiRoot, Samples#rest_api_samples.api_root),

    OperationNames = lists:map(fun(Sample) ->
        ?assertMatch(#rest_api_request_sample{}, Sample),
        ?assertNot(string:is_empty(Sample#rest_api_request_sample.description)),
        ?assertEqual('GET', Sample#rest_api_request_sample.method),
        TokenizedPath = filename:split(Sample#rest_api_request_sample.path),
        ?assert(lists:member(ShareObjectId, TokenizedPath)),
        ?assertEqual(undefined, Sample#rest_api_request_sample.data),
        ?assertNot(Sample#rest_api_request_sample.requires_authorization),
        ?assert(Sample#rest_api_request_sample.follow_redirects),
        ?assertEqual(<<"get_shared_data">>, Sample#rest_api_request_sample.swagger_operation_id),
        Sample#rest_api_request_sample.name
    end, Samples#rest_api_samples.samples),

    ?assertEqual(operation_names(public, FileType), OperationNames).


%% @private
verify_private_rest_api_samples(Node, FileType, FileGuid, SamplesJson) ->
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    
    Samples = ?assertMatch(#rest_api_samples{}, jsonable_record:from_json(SamplesJson, rest_api_samples)),
    ExpectedApiRoot = str_utils:format_bin("https://~s:443/api/v3/oneprovider", [opw_test_rpc:get_provider_domain(Node)]),
    ?assertEqual(ExpectedApiRoot, Samples#rest_api_samples.api_root),
    
    OperationNames = lists:map(fun(Sample) ->
        ?assertMatch(#rest_api_request_sample{}, Sample),
        ?assertNot(string:is_empty(Sample#rest_api_request_sample.description)),
        TokenizedPath = filename:split(Sample#rest_api_request_sample.path),
        ?assert(lists:member(ObjectId, TokenizedPath)),
        ?assert(Sample#rest_api_request_sample.requires_authorization),
        ?assertNot(Sample#rest_api_request_sample.follow_redirects),
        Sample#rest_api_request_sample.name
    end, Samples#rest_api_samples.samples),
    
    ?assertEqual(operation_names(private, FileType), OperationNames).


operation_names(public, ?DIRECTORY_TYPE) -> [
    <<"Download directory (tar)">>,
    <<"List directory files and subdirectories">>,
    <<"Get attributes">>,
    <<"Get extended attributes (xattrs)">>,
    <<"Get JSON metadata">>,
    <<"Get RDF metadata">>
];
operation_names(public, ?REGULAR_FILE_TYPE) -> [
    <<"Download file content">>,
    <<"Get attributes">>,
    <<"Get extended attributes (xattrs)">>,
    <<"Get JSON metadata">>,
    <<"Get RDF metadata">>
];
operation_names(private, ?DIRECTORY_TYPE) -> [
    <<"Remove file">>,
    <<"Get attributes">>,
    <<"Get JSON metadata">>,
    <<"Set JSON metadata">>,
    <<"Remove JSON metadata">>,
    <<"Get RDF metadata">>,
    <<"Set RDF metadata">>,
    <<"Remove RDF metadata">>,
    <<"Get extended attributes (xattrs)">>,
    <<"Set extended attribute (xattr)">>,
    <<"Remove extended attributes (xattrs)">>,
    <<"Create file in directory">>,
    <<"List directory files and subdirectories">>,
    <<"Download directory (tar)">>
];
operation_names(private, ?REGULAR_FILE_TYPE) -> [
    <<"Remove file">>,
    <<"Get attributes">>,
    <<"Get JSON metadata">>,
    <<"Set JSON metadata">>,
    <<"Remove JSON metadata">>,
    <<"Get RDF metadata">>,
    <<"Set RDF metadata">>,
    <<"Remove RDF metadata">>,
    <<"Get extended attributes (xattrs)">>,
    <<"Set extended attribute (xattr)">>,
    <<"Remove extended attributes (xattrs)">>,
    <<"Download file content">>,
    <<"Update file content">>,
    <<"Get file hard links">>
];
operation_names(private, ?SYMLINK_TYPE) -> [
    <<"Remove file">>,
    <<"Get attributes">>,
    <<"Get symbolic link value">>,
    <<"Get file hard links">>
].


get_samples(FileGuid, Scope) ->
    Module = case Scope of
        public -> public_file_api_samples;
        private -> private_file_api_samples
    end,
    #{<<"rest">> := RestApiSamplesJson} = opw_test_rpc:call(
        oct_background:get_random_provider_node(krakow),
        Module,
        generate_for,
        [oct_background:get_user_session_id(user2, krakow), FileGuid]
    ),
    jsonable_record:from_json(RestApiSamplesJson, rest_api_samples).


get_custom_metadata(Guid, MetadataType) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user2, krakow),
    opt_file_metadata:get_custom_metadata(Node, SessId, ?FILE_REF(Guid), MetadataType, [], false).


get_xattrs(Guid) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessId = oct_background:get_user_session_id(user2, krakow),
    {ok, Xattrs} = lfm_proxy:list_xattr(Node, SessId, ?FILE_REF(Guid), false, false),
    lists:foldl(fun(Xattr, Acc) ->
        {ok, #xattr{value = Value}} = lfm_proxy:get_xattr(Node, SessId, ?FILE_REF(Guid), Xattr),
        Acc#{Xattr => Value}
    end, #{}, Xattrs).


normalize_method('GET') -> get;
normalize_method('POST') -> post;
normalize_method('PUT') -> put;
normalize_method('DELETE') -> delete.


setup_verify_sample_env(?DIRECTORY_TYPE, Scope) ->
    #object{guid = Guid, shares = [ShareId]} =
        onenv_file_test_utils:create_and_sync_file_tree(user2, space_krk_par, #dir_spec{
            metadata = metadata_spec(),
            shares = [#share_spec{}],
            children = [#file_spec{}, #dir_spec{}]
        }),
    build_test_guid(Scope, Guid, ShareId);
setup_verify_sample_env(?REGULAR_FILE_TYPE, Scope) ->
    #object{guid = Guid, shares = [ShareId]} =
        onenv_file_test_utils:create_and_sync_file_tree(user2, space_krk_par, #file_spec{
            content = <<"some_content">>,
            shares = [#share_spec{}],
            metadata = metadata_spec()
        }),
    build_test_guid(Scope, Guid, ShareId);
setup_verify_sample_env(?SYMLINK_TYPE, Scope) ->
    [#object{guid = Guid, shares = [ShareId]}, _] =
        onenv_file_test_utils:create_and_sync_file_tree(user2, space_krk_par, [
            #file_spec{custom_label = <<"link_target">>},
            #symlink_spec{
                shares = [#share_spec{}],
                symlink_value = {custom_label, <<"link_target">>}
            }
        ]),
    
    build_test_guid(Scope, Guid, ShareId).


build_test_guid(private, Guid, _ShareId) ->
    Guid;
build_test_guid(public, Guid, ShareId) ->
    file_id:guid_to_share_guid(Guid, ShareId).


metadata_spec() ->
    #metadata_spec{
        json = #{<<"some_json_key">> => <<"some_json_value">>},
        rdf = <<"some_rdf">>,
        xattrs = #{<<"xattr_key">> => <<"xattr_value">>}
    }.
    

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
