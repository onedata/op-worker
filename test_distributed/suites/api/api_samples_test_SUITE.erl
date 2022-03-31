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
-include_lib("ctool/include/api_samples/common.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").


-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    public_file_api_samples_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        public_file_api_samples_test
    ]}
].

all() -> [
    {group, all_tests}
].


-define(DUMMY_XROOTD_SERVER_DOMAIN, <<"xrootd.example.com">>).


%%%===================================================================
%%% Test functions
%%%===================================================================


public_file_api_samples_test(_Config) ->
    #object{
        guid = TopDirGuid,
        type = ?DIRECTORY_TYPE,
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
            name = <<"top_dir">>,
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
    public_file_api_samples_test_base(?REGULAR_FILE_TYPE, FileGuid, TopDirShareId, <<"/top_dir/file">>),
    public_file_api_samples_test_base(?REGULAR_FILE_TYPE, NestedFileGuid, TopDirShareId, <<"/top_dir/nested_dir/nested_file">>),
    public_file_api_samples_test_base(?DIRECTORY_TYPE, TopDirGuid, TopDirShareId, <<"/top_dir">>),
    public_file_api_samples_test_base(?DIRECTORY_TYPE, NestedDirGuid, TopDirShareId, <<"/top_dir/nested_dir">>).

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
                                ?assertEqual([<<"rest">>, <<"xrootd">>], maps:keys(Result)),
                                verify_xrootd_api_samples(FileType, SpaceId, ShareId, FilePathInShare, maps:get(<<"xrootd">>, Result)),
                                verify_rest_api_samples(FileType, ShareGuid, maps:get(<<"rest">>, Result));
                            xrootd_disabled ->
                                ?assertEqual([<<"rest">>], maps:keys(Result)),
                                verify_rest_api_samples(FileType, ShareGuid, maps:get(<<"rest">>, Result))
                        end
                    end
                }
            ],
            data_spec = #data_spec{
                bad_values = [{bad_id, <<"NonExistentFile">>, ?ERROR_BAD_VALUE_IDENTIFIER(<<"id">>)}]
            }
        }
    ])).


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
verify_rest_api_samples(FileType, ShareGuid, SamplesJson) ->
    {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

    ?assertMatch(#rest_api_samples{}, jsonable_record:from_json(SamplesJson, rest_api_samples)),
    Samples = jsonable_record:from_json(SamplesJson, rest_api_samples),
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

    ExpectedOperationNames = case FileType of
        ?REGULAR_FILE_TYPE -> [
            <<"Download file content">>,
            <<"Get attributes">>,
            <<"Get extended attributes (xattrs)">>,
            <<"Get JSON metadata">>,
            <<"Get RDF metadata">>
        ];
        ?DIRECTORY_TYPE -> [
            <<"Download directory (tar)">>,
            <<"List directory files and subdirectories">>,
            <<"Get attributes">>,
            <<"Get extended attributes (xattrs)">>,
            <<"Get JSON metadata">>,
            <<"Get RDF metadata">>
        ]
    end,
    ?assertEqual(OperationNames, ExpectedOperationNames).


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
    Config.


end_per_group(_Group, Config) ->
    Config.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
