%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file custom metadata delete API
%%% (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(file_metadata_delete_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("file_metadata_api_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    % Delete rdf metadata test cases
    delete_file_rdf_metadata_with_rdf_set_test/1,
    delete_file_rdf_metadata_without_rdf_set_test/1,

    % Delete json metadata test cases
    delete_file_json_metadata_with_json_set_test/1,
    delete_file_json_metadata_without_json_set_test/1,

    % Delete xattrs test cases
    delete_file_xattrs/1
]).

all() -> [
    delete_file_rdf_metadata_with_rdf_set_test,
    delete_file_rdf_metadata_without_rdf_set_test,

    delete_file_json_metadata_with_json_set_test,
    delete_file_json_metadata_without_json_set_test,

    delete_file_xattrs
].


-type test_setup_variant() :: preset_initial_metadata | no_initial_metadata.


-define(ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


delete_file_rdf_metadata_with_rdf_set_test(Config) ->
    delete_metadata_test_base(
        <<"rdf">>, ?RDF_METADATA_1, preset_initial_metadata, undefined, false, Config
    ).


delete_file_rdf_metadata_without_rdf_set_test(Config) ->
    delete_metadata_test_base(
        <<"rdf">>, ?RDF_METADATA_2, no_initial_metadata, undefined, false, Config
    ).


delete_file_json_metadata_with_json_set_test(Config) ->
    delete_metadata_test_base(
        <<"json">>, ?JSON_METADATA_1, preset_initial_metadata, undefined, false, Config
    ).


delete_file_json_metadata_without_json_set_test(Config) ->
    delete_metadata_test_base(
        <<"json">>, ?JSON_METADATA_2, no_initial_metadata, undefined, false, Config
    ).


delete_file_xattrs(Config) ->
    FullXattrSet = #{
        ?RDF_METADATA_KEY => ?RDF_METADATA_1,
        ?JSON_METADATA_KEY => ?JSON_METADATA_4,
        ?ACL_KEY => ?OWNER_ONLY_ALLOW_ACL,
        ?MIMETYPE_KEY => ?MIMETYPE_1,
        ?TRANSFER_ENCODING_KEY => ?TRANSFER_ENCODING_1,
        ?CDMI_COMPLETION_STATUS_KEY => ?CDMI_COMPLETION_STATUS_1,
        ?XATTR_1_KEY => ?XATTR_1_VALUE
    },

    DataSpec = #data_spec{
        required = [<<"keys">>],
        correct_values = #{<<"keys">> => [
            [?RDF_METADATA_KEY, ?XATTR_1_KEY],
            [?JSON_METADATA_KEY, ?ACL_KEY],
            [<<"dummy.xattr">>],  % Deleting non existent xattr should succeed
            [?RDF_METADATA_KEY, ?JSON_METADATA_KEY, ?ACL_KEY, ?XATTR_1_KEY, <<"dummy.xattr">>]
        ]},
        bad_values = [
            {<<"keys">>, <<"aaa">>, ?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<"keys">>)},
            {<<"keys">>, [<<?ONEDATA_PREFIX/binary, "xattr">>], ?ERROR_POSIX(?EPERM)},
            % Cdmi xattrs (other than acl) can't be deleted via xattr api
            {<<"keys">>, [?MIMETYPE_KEY], ?ERROR_POSIX(?EPERM)},
            {<<"keys">>, [?TRANSFER_ENCODING_KEY], ?ERROR_POSIX(?EPERM)},
            {<<"keys">>, [?CDMI_COMPLETION_STATUS_KEY], ?ERROR_POSIX(?EPERM)},
            {<<"keys">>, [<<?CDMI_PREFIX/binary, "xattr">>], ?ERROR_POSIX(?EPERM)}
        ]
    },

    delete_metadata_test_base(
        <<"xattrs">>, FullXattrSet, preset_initial_metadata, DataSpec, true, Config
    ).


%%%===================================================================
%%% Get metadata generic functions
%%%===================================================================


%% @private
-spec delete_metadata_test_base(
    api_test_utils:metadata_type(),
    Metadata :: term(),
    test_setup_variant(),
    onenv_api_test_runner:data_spec(),
    RandomlySelectScenario :: boolean(),
    api_test_runner:config()
) ->
    ok.
delete_metadata_test_base(
    MetadataType, Metadata, TestSetupVariant, DataSpec, RandomlySelectScenario, Config
) ->
    Nodes = ?config(op_worker_nodes, Config),
    {FileType, _FilePath, FileGuid, ShareId} = api_test_utils:create_and_sync_shared_file_in_space2(
        8#707, Config
    ),
    FileShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Nodes,
            setup_fun = build_setup_fun(TestSetupVariant, FileGuid, MetadataType, Metadata, Nodes),
            verify_fun = build_verify_fun(TestSetupVariant, FileGuid, MetadataType, Metadata, Nodes),
            client_spec = ?CLIENT_SPEC_FOR_SPACE_2,
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Delete ~s metadata for ~s using gs private api", [
                        MetadataType, FileType
                    ]),
                    type = gs,
                    prepare_args_fun = build_delete_metadata_prepare_gs_args_fun(
                        MetadataType, FileGuid, private
                    ),
                    validate_result_fun = fun(_TestCtx, Result) ->
                        ?assertEqual({ok, undefined}, Result)
                    end
                }
            ],
            randomly_select_scenarios = RandomlySelectScenario,
            data_spec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
                FileGuid, ShareId, DataSpec
            )
        },

        #scenario_spec{
            name = str_utils:format("Delete ~s metadata for shared ~s using gs public api", [
                MetadataType, FileType
            ]),
            type = gs_not_supported,
            target_nodes = Nodes,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_delete_metadata_prepare_gs_args_fun(
                MetadataType, FileShareGuid, public
            ),
            validate_result_fun = fun(_TestCaseCtx, Result) ->
                ?assertEqual(?ERROR_NOT_SUPPORTED, Result)
            end,
            data_spec = DataSpec
        }
    ])).


%% @private
-spec build_setup_fun(test_setup_variant(), file_id:file_guid(), api_test_utils:metadata_type(),
    Metadata :: term(), [node()]) -> onenv_api_test_runner:verify_fun().
build_setup_fun(preset_initial_metadata, FileGuid, MetadataType, Metadata, Nodes) ->
    fun() ->
        % Check to prevent race condition in tests (see onenv_api_test_runner
        % COMMON PITFALLS 1).
        RandNode = lists_utils:random_element(Nodes),
        case {ok, Metadata} == api_test_utils:get_metadata(RandNode, FileGuid, MetadataType) of
            true -> ok;
            false -> api_test_utils:set_and_sync_metadata(Nodes, FileGuid, MetadataType, Metadata)
        end
    end;
build_setup_fun(no_initial_metadata, _, _, _, _) ->
    fun() -> ok end.


%% @private
-spec build_verify_fun(test_setup_variant(), file_id:file_guid(), api_test_utils:metadata_type(),
    Metadata :: term(), [node()]) -> onenv_api_test_runner:verify_fun().
build_verify_fun(preset_initial_metadata, FileGuid, <<"xattrs">>, FullXattrSet, Nodes) ->
    fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            ?assertMatch({ok, FullXattrSet}, api_test_utils:get_xattrs(TestNode, FileGuid), ?ATTEMPTS),
            true;
        (expected_success, #api_test_ctx{data = #{<<"keys">> := Keys}}) ->
            ExpXattrs = maps:without(Keys, FullXattrSet),
            lists:foreach(fun(Node) ->
                ?assertEqual({ok, ExpXattrs}, api_test_utils:get_xattrs(Node, FileGuid), ?ATTEMPTS)
            end, Nodes),
            true
    end;
build_verify_fun(preset_initial_metadata, FileGuid, MetadataType, ExpMetadata, Nodes) ->
    fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            ?assertMatch(
                {ok, ExpMetadata},
                api_test_utils:get_metadata(TestNode, FileGuid, MetadataType),
                ?ATTEMPTS
            ),
            true;
        (expected_success, _) ->
            lists:foreach(fun(Node) ->
                ?assertMatch(
                    {error, ?ENODATA},
                    api_test_utils:get_metadata(Node, FileGuid, MetadataType),
                    ?ATTEMPTS
                )
            end, Nodes),
            true
    end;
build_verify_fun(no_initial_metadata, FileGuid, MetadataType, _ExpMetadata, Nodes) ->
    fun(_, _) ->
        lists:foreach(fun(Node) ->
            ?assertMatch(
                {error, ?ENODATA},
                api_test_utils:get_metadata(Node, FileGuid, MetadataType),
                ?ATTEMPTS
            )
        end, Nodes),
        true
    end.


%% @private
build_delete_metadata_prepare_gs_args_fun(MetadataType, FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),

        Aspect = case MetadataType of
            <<"json">> -> json_metadata;
            <<"rdf">> -> rdf_metadata;
            <<"xattrs">> -> xattrs
        end,
        #gs_args{
            operation = delete,
            gri = #gri{type = op_file, id = GriId, aspect = Aspect, scope = Scope},
            data = Data1
        }
    end.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    application:start(ssl),
    hackney:start(),
    api_test_env:init_per_suite(Config, #onenv_test_config{envs = [
        {op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}
    ]}).


end_per_suite(_Config) ->
    hackney:stop(),
    application:stop(ssl).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 30}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
