%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file custom metadata set API
%%% (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(file_metadata_set_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("file_metadata_api_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    % Set rdf metadata test cases
    set_file_rdf_metadata_test/1,
    set_file_rdf_metadata_on_provider_not_supporting_space_test/1,

    % Set json metadata test cases
    set_file_json_metadata_test/1,
    set_file_primitive_json_metadata_test/1,
    set_file_json_metadata_on_provider_not_supporting_space_test/1,

    % Set xattrs test cases
    set_file_xattrs_test/1,
    set_file_xattrs_on_provider_not_supporting_space_test/1
]).

all() -> [
    set_file_rdf_metadata_test,
    set_file_rdf_metadata_on_provider_not_supporting_space_test,

    set_file_json_metadata_test,
    set_file_primitive_json_metadata_test,
    set_file_json_metadata_on_provider_not_supporting_space_test,

    set_file_xattrs_test,
    set_file_xattrs_on_provider_not_supporting_space_test
].


-define(ATTEMPTS, 30).


%%%===================================================================
%%% Set rdf metadata functions
%%%===================================================================


set_file_rdf_metadata_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),
    {FileType, FilePath, FileGuid, ShareId} = api_test_utils:create_and_sync_shared_file_in_space2(
        8#707, Config
    ),

    DataSpec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
        FileGuid, ShareId, #data_spec{
            required = [<<"metadata">>],
            correct_values = #{
                <<"metadata">> => [
                    ?RDF_METADATA_1, ?RDF_METADATA_2, ?RDF_METADATA_3, ?RDF_METADATA_4
                ]
            }
        }
    ),

    % Setting rdf should always succeed for correct clients
    GetExpCallResultFun = fun(_TestCtx) -> ok end,

    VerifyEnvFun = fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            ?assertMatch({error, ?ENODATA}, get_rdf(TestNode, FileGuid), ?ATTEMPTS),
            true;
        (expected_success, #api_test_ctx{node = TestNode, data = #{<<"metadata">> := Metadata}}) ->
            lists:foreach(fun(Node) ->
                ?assertMatch({ok, Metadata}, get_rdf(Node, FileGuid), ?ATTEMPTS)
            end, Providers),

            case Metadata == ?RDF_METADATA_4 of
                true ->
                    % Remove ?RDF_METADATA_2 to test setting ?RDF_METADATA_1 by other clients
                    % on clean state
                    ?assertMatch(ok, remove_rdf(TestNode, FileGuid)),
                    % Wait for removal to be synced between providers.
                    lists:foreach(fun(Node) ->
                        ?assertMatch({error, ?ENODATA}, get_rdf(Node, FileGuid), ?ATTEMPTS)
                    end, Providers);
                false ->
                    ok
            end,
            true
    end,

    set_metadata_test_base(
        <<"rdf">>,
        FileType, FilePath, FileGuid, ShareId,
        build_set_metadata_validate_rest_call_fun(GetExpCallResultFun),
        build_set_metadata_validate_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun, Providers, ?CLIENT_SPEC_FOR_SPACE_2, DataSpec, _QsParams = [],
        _RandomlySelectScenario = true,
        Config
    ).


set_file_rdf_metadata_on_provider_not_supporting_space_test(Config) ->
    P2Id = api_test_env:get_provider_id(p2, Config),
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    {FileType, FilePath, FileGuid, ShareId} = api_test_utils:create_shared_file_in_space1(Config),

    DataSpec = #data_spec{
        required = [<<"metadata">>],
        correct_values = #{<<"metadata">> => [?RDF_METADATA_1]}
    },

    GetExpCallResultFun = fun(_TestCtx) -> ?ERROR_SPACE_NOT_SUPPORTED_BY(P2Id) end,

    VerifyEnvFun = fun(_, _) ->
        ?assertMatch({error, ?ENODATA}, get_rdf(P1Node, FileGuid), ?ATTEMPTS),
        true
    end,

    set_metadata_test_base(
        <<"rdf">>,
        FileType, FilePath, FileGuid, ShareId,
        build_set_metadata_validate_rest_call_fun(GetExpCallResultFun),
        build_set_metadata_validate_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun, [P2Node], ?CLIENT_SPEC_FOR_SPACE_1, DataSpec, _QsParams = [],
        _RandomlySelectScenario = false,
        Config
    ).


%% @private
get_rdf(Node, FileGuid) ->
    lfm_proxy:get_metadata(Node, ?ROOT_SESS_ID, {guid, FileGuid}, rdf, [], false).


%% @private
remove_rdf(Node, FileGuid) ->
    lfm_proxy:remove_metadata(Node, ?ROOT_SESS_ID, {guid, FileGuid}, rdf).


%%%===================================================================
%%% Set json metadata functions
%%%===================================================================


set_file_json_metadata_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),
    {FileType, FilePath, FileGuid, ShareId} = api_test_utils:create_and_sync_shared_file_in_space2(
        8#707, Config
    ),

    ExampleJson = #{<<"attr1">> => [0, 1, <<"val">>]},

    DataSpec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
        FileGuid, ShareId, #data_spec{
            required = [<<"metadata">>],
            optional = QsParams = [<<"filter_type">>, <<"filter">>],
            correct_values = #{
                <<"metadata">> => [ExampleJson],
                <<"filter_type">> => [<<"keypath">>],
                <<"filter">> => [
                    <<"[1]">>,                  % Test creating placeholder array for nonexistent
                                                % previously json
                    <<"[1].attr1.[1]">>,        % Test setting attr in existing array
                    <<"[1].attr1.[2].attr22">>, % Test error when trying to set subjson to binary
                                                % (<<"val">> in ExampleJson)
                    <<"[1].attr1.[5]">>,        % Test setting attr beyond existing array
                    <<"[1].attr2.[2]">>         % Test setting attr in nonexistent array
                ]
            },
            bad_values = [
                % invalid json error can be returned only for rest (invalid json is send as
                % body without modification) and not gs (#{<<"metadata">> => some_binary} is send,
                % so no matter what that some_binary is it will be treated as string)
                {<<"metadata">>, <<"aaa">>, {rest_handler, ?ERROR_BAD_VALUE_JSON(<<"metadata">>)}},
                {<<"metadata">>, <<"{">>, {rest_handler, ?ERROR_BAD_VALUE_JSON(<<"metadata">>)}},
                {<<"metadata">>, <<"{\"aaa\": aaa}">>, {rest_handler,
                    ?ERROR_BAD_VALUE_JSON(<<"metadata">>)}},

                {<<"filter_type">>, <<"dummy">>,
                    ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filter_type">>, [<<"keypath">>])},

                % Below differences between error returned by rest and gs are results of sending
                % parameters via qs in REST, so they lost their original type and are cast to binary
                {<<"filter_type">>, 100, {rest_with_file_path,
                    ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filter_type">>, [<<"keypath">>])}},
                {<<"filter_type">>, 100, {rest,
                    ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filter_type">>, [<<"keypath">>])}},
                {<<"filter_type">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"filter_type">>)}},
                {<<"filter">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"filter">>)}}
            ]
        }
    ),

    GetRequestFilterArg = fun(#api_test_ctx{data = Data}) ->
        FilterType = maps:get(<<"filter_type">>, Data, undefined),
        Filter = maps:get(<<"filter">>, Data, undefined),

        case {FilterType, Filter} of
            {undefined, _} ->
                {ok, []};
            {<<"keypath">>, undefined} ->
                ?ERROR_MISSING_REQUIRED_VALUE(<<"filter">>);
            {<<"keypath">>, _} ->
                case binary:split(Filter, <<".">>, [global]) of
                    [<<"[1]">>, <<"attr1">>, <<"[2]">>, <<"attr22">>] ->
                        % ?ENODATA is returned due to trying to set attribute in
                        % string (see data_spec and order of requests)
                        ?ERROR_POSIX(?ENODATA);
                    ExistingPath ->
                        {ok, ExistingPath}
                end
        end
    end,
    GetExpCallResultFun = fun(TestCtx) ->
        case GetRequestFilterArg(TestCtx) of
            {ok, _Filters} -> ok;
            {error, _} = Error -> Error
        end
    end,

    VerifyEnvFun = fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            ?assertMatch({error, ?ENODATA}, get_json(TestNode, FileGuid), ?ATTEMPTS),
            true;
        (expected_success, #api_test_ctx{node = TestNode} = TestCtx) ->
            FilterOrError = GetRequestFilterArg(TestCtx),
            % Expected metadata depends on the tested parameters combination order.
            % First only required params will be tested, then those with only one optional params,
            % next with 2 and so on. If optional param has multiple values then those
            % will be also tested later.
            ExpResult = case FilterOrError of
                {ok, []} ->
                    {ok, ExampleJson};
                ?ERROR_MISSING_REQUIRED_VALUE(_) ->
                    % Test failed to set json because of specifying
                    % filter_type without specifying filter
                    {error, ?ENODATA};
                {ok, [<<"[1]">>]} ->
                    {ok, [null, ExampleJson]};
                {ok, [<<"[1]">>, <<"attr1">>, <<"[1]">>]} ->
                    {ok, [null, #{<<"attr1">> => [0, ExampleJson, <<"val">>]}]};
                ?ERROR_POSIX(?ENODATA) ->
                    % Operation failed and nothing should be changed -
                    % it should match the same json as above
                    {ok, [null, #{<<"attr1">> => [0, ExampleJson, <<"val">>]}]};
                {ok, [<<"[1]">>, <<"attr1">>, <<"[5]">>]} ->
                    {ok, [null, #{
                        <<"attr1">> => [0, ExampleJson, <<"val">>, null, null, ExampleJson]
                    }]};
                {ok, [<<"[1]">>, <<"attr2">>, <<"[2]">>]} ->
                    {ok, [null, #{
                        <<"attr1">> => [0, ExampleJson, <<"val">>, null, null, ExampleJson],
                        <<"attr2">> => [null, null, ExampleJson]
                    }]}
            end,
            lists:foreach(fun(Node) ->
                ?assertMatch(ExpResult, get_json(Node, FileGuid), ?ATTEMPTS)
            end, Providers),

            case FilterOrError of
                {ok, Filter} when Filter == [] orelse Filter == [<<"[1]">>, <<"attr2">>, <<"[2]">>] ->
                    % Remove metadata after:
                    %   - last successful params combination tested so that test cases for next
                    %     client can be run on clean state,
                    %   - combinations without all params. Because they do not use filters it
                    %     is impossible to tell whether operation failed or value was overridden
                    %     (those test cases are setting the same ExampleJson).
                    % Metadata are not removed after other test cases so that they can test
                    % updating metadata using `filter` param.
                    ?assertMatch(ok, remove_json(TestNode, FileGuid)),
                    % Wait for changes to be synced between providers. Otherwise it can possible
                    % interfere with tests on other node (e.g. information about deletion that
                    % comes after setting ExampleJson and before setting using filter results in
                    % json metadata removal. In such case next test using 'filter' parameter should
                    % expect ExpMetadata = #{<<"attr1">> => [null, null, null, null, null, ExampleJson]}
                    % rather than above one as that will be the result of setting ExampleJson
                    % with attr1.[5] filter and no prior json set)
                    lists:foreach(fun(Node) ->
                        ?assertMatch({error, ?ENODATA}, get_json(Node, FileGuid), ?ATTEMPTS)
                    end, Providers);
                _ ->
                    ok
            end,
            true
    end,

    set_metadata_test_base(
        <<"json">>,
        FileType, FilePath, FileGuid, ShareId,
        build_set_metadata_validate_rest_call_fun(GetExpCallResultFun),
        build_set_metadata_validate_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun, Providers, ?CLIENT_SPEC_FOR_SPACE_2, DataSpec, QsParams,
        _RandomlySelectScenario = true,
        Config
    ).


set_file_primitive_json_metadata_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),
    {FileType, FilePath, FileGuid, ShareId} = api_test_utils:create_and_sync_shared_file_in_space2(
        8#707, Config
    ),

    DataSpec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
        FileGuid, ShareId, #data_spec{
            required = [<<"metadata">>],
            correct_values = #{<<"metadata">> => [
                <<"{}">>, <<"[]">>, <<"true">>, <<"0">>, <<"0.1">>,
                <<"null">>, <<"\"string\"">>
            ]}
        }
    ),

    GetExpCallResultFun = fun(_TestCtx) -> ok end,

    VerifyEnvFun = fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            ?assertMatch({error, ?ENODATA}, get_json(TestNode, FileGuid), ?ATTEMPTS),
            true;
        (expected_success, #api_test_ctx{node = TestNode, data = #{<<"metadata">> := Metadata}}) ->
            ExpMetadata = json_utils:decode(Metadata),
            lists:foreach(fun(Node) ->
                ?assertMatch({ok, ExpMetadata}, get_json(Node, FileGuid), ?ATTEMPTS)
            end, Providers),

            case Metadata of
                <<"\"string\"">> ->
                    % Remove metadata after last successful parameters combination tested so that
                    % tests for next clients can start from setting rather then updating metadata
                    ?assertMatch(ok, remove_json(TestNode, FileGuid)),
                    lists:foreach(fun(Node) ->
                        ?assertMatch({error, ?ENODATA}, get_json(Node, FileGuid), ?ATTEMPTS)
                    end, Providers);
                _ ->
                    ok
            end,
            true
    end,

    set_metadata_test_base(
        <<"json">>,
        FileType, FilePath, FileGuid, ShareId,
        build_set_metadata_validate_rest_call_fun(GetExpCallResultFun),
        build_set_metadata_validate_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun, Providers, ?CLIENT_SPEC_FOR_SPACE_2, DataSpec, _QsParams = [],
        _RandomlySelectScenario = true,
        Config
    ).


set_file_json_metadata_on_provider_not_supporting_space_test(Config) ->
    P2Id = api_test_env:get_provider_id(p2, Config),
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    {FileType, FilePath, FileGuid, ShareId} = api_test_utils:create_shared_file_in_space1(Config),

    DataSpec = #data_spec{
        required = [<<"metadata">>],
        correct_values = #{<<"metadata">> => [?JSON_METADATA_4]}
    },

    GetExpCallResultFun = fun(_TestCtx) -> ?ERROR_SPACE_NOT_SUPPORTED_BY(P2Id) end,

    VerifyEnvFun = fun(_, _) ->
        ?assertMatch({error, ?ENODATA}, get_json(P1Node, FileGuid), ?ATTEMPTS),
        true
    end,

    set_metadata_test_base(
        <<"json">>,
        FileType, FilePath, FileGuid, ShareId,
        build_set_metadata_validate_rest_call_fun(GetExpCallResultFun),
        build_set_metadata_validate_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun, [P2Node], ?CLIENT_SPEC_FOR_SPACE_1, DataSpec, _QsParams = [],
        _RandomlySelectScenario = false,
        Config
    ).


%% @private
get_json(Node, FileGuid) ->
    lfm_proxy:get_metadata(Node, ?ROOT_SESS_ID, {guid, FileGuid}, json, [], false).


%% @private
remove_json(Node, FileGuid) ->
    lfm_proxy:remove_metadata(Node, ?ROOT_SESS_ID, {guid, FileGuid}, json).


%%%===================================================================
%%% Set xattrs functions
%%%===================================================================


set_file_xattrs_test(Config) ->
    Providers = ?config(op_worker_nodes, Config),
    User2Id = api_test_env:get_user_id(user2, Config),
    User3Id = api_test_env:get_user_id(user3, Config),
    {FileType, FilePath, FileGuid, ShareId} = api_test_utils:create_and_sync_shared_file_in_space2(
        8#707, Config
    ),

    DataSpec = api_test_utils:add_file_id_errors_for_operations_not_available_in_share_mode(
        FileGuid, ShareId, #data_spec{
            required = [<<"metadata">>],
            correct_values = #{<<"metadata">> => [
                % Tests setting multiple xattrs at once
                #{?XATTR_1_KEY => ?XATTR_1_VALUE, ?XATTR_2_KEY => ?XATTR_2_VALUE},
                % Tests setting xattr internal types
                #{?ACL_KEY => ?ACL_3},
                #{?MIMETYPE_KEY => ?MIMETYPE_1},
                #{?TRANSFER_ENCODING_KEY => ?TRANSFER_ENCODING_1},
                #{?CDMI_COMPLETION_STATUS_KEY => ?CDMI_COMPLETION_STATUS_1},
                #{?JSON_METADATA_KEY => ?JSON_METADATA_4},
                #{?RDF_METADATA_KEY => ?RDF_METADATA_1}
            ]},
            bad_values = [
                {<<"metadata">>, <<"aaa">>, ?ERROR_BAD_VALUE_JSON(<<"metadata">>)},
                % Keys with prefixes `cdmi_` and `onedata_` are forbidden with exception
                % for those listed in above correct_values
                {<<"metadata">>, #{<<"cdmi_attr">> => <<"val">>}, ?ERROR_POSIX(?EPERM)},
                {<<"metadata">>, #{<<"onedata_attr">> => <<"val">>}, ?ERROR_POSIX(?EPERM)}
            ]
        }
    ),

    GetExpCallResultFun = fun(#api_test_ctx{client = Client, data = #{<<"metadata">> := Xattrs}}) ->
        case {Client, maps:is_key(?ACL_KEY, Xattrs)} of
            {?USER(UserId), true} when UserId /= User2Id andalso UserId /= User3Id ->
                % Only space owner or file owner can set acl in posix mode
                ?ERROR_POSIX(?EACCES);
            _ ->
                ok
        end
    end,
    VerifyEnvFun = fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            assert_no_xattrs_set(TestNode, FileGuid),
            true;
        (expected_success, #api_test_ctx{
            node = TestNode,
            client = Client,
            data = #{<<"metadata">> := Xattrs
        }}) ->
            case {Client, maps:is_key(?ACL_KEY, Xattrs)} of
                {?USER(UserId), true} when UserId /= User2Id andalso UserId /= User3Id ->
                    % Only owner (?USER_IN_BOTH_SPACES) can set acl in posix mode
                    ?assertMatch({error, ?ENODATA}, get_xattr(TestNode, FileGuid, ?ACL_KEY), ?ATTEMPTS);
                _ ->
                    assert_all_xattrs_set(Providers, FileGuid, Xattrs),
                    remove_xattrs(TestNode, Providers, FileGuid, Xattrs)
            end,
            true
    end,

    set_metadata_test_base(
        <<"xattrs">>,
        FileType, FilePath, FileGuid, ShareId,
        build_set_metadata_validate_rest_call_fun(GetExpCallResultFun),
        build_set_metadata_validate_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun, Providers, ?CLIENT_SPEC_FOR_SPACE_2, DataSpec, _QsParams = [],
        _RandomlySelectScenario = true,
        Config
    ).


set_file_xattrs_on_provider_not_supporting_space_test(Config) ->
    P2Id = api_test_env:get_provider_id(p2, Config),
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    {FileType, FilePath, FileGuid, ShareId} = api_test_utils:create_shared_file_in_space1(Config),

    DataSpec = #data_spec{
        required = [<<"metadata">>],
        correct_values = #{<<"metadata">> => [#{?XATTR_1_KEY => ?XATTR_1_VALUE}]}
    },

    GetExpCallResultFun = fun(_TestCtx) -> ?ERROR_SPACE_NOT_SUPPORTED_BY(P2Id) end,

    VerifyEnvFun = fun(_, _) ->
        ?assertMatch({error, ?ENODATA}, get_xattr(P1Node, FileGuid, ?XATTR_1_KEY), ?ATTEMPTS),
        true
    end,

    set_metadata_test_base(
        <<"xattrs">>,
        FileType, FilePath, FileGuid, ShareId,
        build_set_metadata_validate_rest_call_fun(GetExpCallResultFun),
        build_set_metadata_validate_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun, [P2Node], ?CLIENT_SPEC_FOR_SPACE_1, DataSpec, _QsParams = [],
        _RandomlySelectScenario = false,
        Config
    ).


%% @private
assert_all_xattrs_set(Nodes, FileGuid, Xattrs) ->
    lists:foreach(fun(Node) ->
        lists:foreach(fun({Key, Value}) ->
            ?assertMatch(
                {ok, #xattr{name = Key, value = Value}},
                get_xattr(Node, FileGuid, Key),
                ?ATTEMPTS
            )
        end, maps:to_list(Xattrs))
    end, Nodes).


%% @private
assert_no_xattrs_set(Node, FileGuid) ->
    ?assertMatch(
        {ok, []},
        lfm_proxy:list_xattr(Node, ?ROOT_SESS_ID, {guid, FileGuid}, false, true)
    ).


%% @private
remove_xattrs(TestNode, Nodes, FileGuid, Xattrs) ->
    {FileUuid, _SpaceId, _} = file_id:unpack_share_guid(FileGuid),

    lists:foreach(fun({Key, _}) ->
        case Key of
            ?ACL_KEY ->
                ?assertMatch(ok, lfm_proxy:remove_acl(TestNode, ?ROOT_SESS_ID, {guid, FileGuid}));
            <<?CDMI_PREFIX_STR, _/binary>> ->
                % Because cdmi attributes don't have api to remove them removal must be carried by
                % calling custom_metadata directly
                ?assertMatch(ok, rpc:call(TestNode, custom_metadata, remove_xattr, [FileUuid, Key]));
            _ ->
                ?assertMatch(ok, lfm_proxy:remove_xattr(TestNode, ?ROOT_SESS_ID, {guid, FileGuid}, Key))
        end,
        lists:foreach(fun(Node) ->
            ?assertMatch({error, ?ENODATA}, get_xattr(Node, FileGuid, Key), ?ATTEMPTS)
        end, Nodes)
    end, maps:to_list(Xattrs)).


%% @private
get_xattr(Node, FileGuid, XattrKey) ->
    lfm_proxy:get_xattr(Node, ?ROOT_SESS_ID, {guid, FileGuid}, XattrKey).


%%%===================================================================
%%% Set metadata generic functions
%%%===================================================================


%% @private
build_set_metadata_validate_rest_call_fun(GetExpResultFun) ->
    fun(TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
        case GetExpResultFun(TestCtx) of
            ok ->
                ?assertEqual({?HTTP_204_NO_CONTENT, #{}}, {RespCode, RespBody});
            {error, _} = Error ->
                ExpRestError = {errors:to_http_code(Error), ?REST_ERROR(Error)},
                ?assertEqual(ExpRestError, {RespCode, RespBody})
        end
    end.


%% @private
build_set_metadata_validate_gs_call_fun(GetExpResultFun) ->
    fun(TestCtx, Result) ->
        case GetExpResultFun(TestCtx) of
            ok ->
                ?assertEqual({ok, undefined}, Result);
            {error, _} = ExpError ->
                ?assertEqual(ExpError, Result)
        end
    end.


%% @private
-spec set_metadata_test_base(
    MetadataType :: binary(),  %% <<"json">> | <<"rdf">> | <<"xattrs">>
    api_test_utils:file_type(), file_meta:path(), file_id:file_guid(),
    od_share:id(),
    ValidateRestCallResultFun :: validate_call_result_fun(),
    ValidateGsCallResultFun :: validate_call_result_fun(),
    verify_fun(),
    Providers :: [node()],
    client_spec(), data_spec(),
    QsParameters :: [binary()],
    RandomlySelectScenario :: boolean(),
    api_test_runner:config()
) ->
    ok.
set_metadata_test_base(
    MetadataType, FileType, FilePath, FileGuid, ShareId,
    ValidateRestCallResultFun, ValidateGsCallResultFun, VerifyEnvFun,
    Providers, ClientSpec, DataSpec, QsParameters, RandomlySelectScenario, Config
) ->
    FileShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ClientSpec,
            verify_fun = VerifyEnvFun,
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Set ~s metadata for ~s using rest endpoint", [
                        MetadataType, FileType
                    ]),
                    type = rest,
                    prepare_args_fun = build_set_metadata_prepare_new_id_rest_args_fun(
                        MetadataType, FileObjectId, QsParameters
                    ),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = str_utils:format("Set ~s metadata for ~s using deprecated path rest endpoint", [
                        MetadataType, FileType
                    ]),
                    type = rest_with_file_path,
                    prepare_args_fun = build_set_metadata_prepare_deprecated_path_rest_args_fun(
                        MetadataType, FilePath, QsParameters
                    ),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = str_utils:format("Set ~s metadata for ~s using deprecated id rest endpoint", [
                        MetadataType, FileType
                    ]),
                    type = rest,
                    prepare_args_fun = build_set_metadata_prepare_deprecated_id_rest_args_fun(
                        MetadataType, FileObjectId, QsParameters
                    ),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = str_utils:format("Set ~s metadata for ~s using gs private api", [
                        MetadataType, FileType
                    ]),
                    type = gs,
                    prepare_args_fun = build_set_metadata_prepare_gs_args_fun(
                        MetadataType, FileGuid, private
                    ),
                    validate_result_fun = ValidateGsCallResultFun
                }
            ],
            randomly_select_scenarios = RandomlySelectScenario,
            data_spec = DataSpec
        },

        #scenario_spec{
            name = str_utils:format("Set ~s metadata for shared ~s using gs public api", [
                MetadataType, FileType
            ]),
            type = gs_not_supported,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_set_metadata_prepare_gs_args_fun(
                MetadataType, FileShareGuid, public
            ),
            validate_result_fun = fun(_TestCaseCtx, Result) ->
                ?assertEqual(?ERROR_NOT_SUPPORTED, Result)
            end,
            data_spec = DataSpec
        }
    ])).


%% @private
build_set_metadata_prepare_new_id_rest_args_fun(MetadataType, FileObjectId, QsParams) ->
    build_set_metadata_prepare_rest_args_fun(new_id, MetadataType, FileObjectId, QsParams).


%% @private
build_set_metadata_prepare_deprecated_path_rest_args_fun(MetadataType, FilePath, QsParams) ->
    build_set_metadata_prepare_rest_args_fun(deprecated_path, MetadataType, FilePath, QsParams).


%% @private
build_set_metadata_prepare_deprecated_id_rest_args_fun(MetadataType, FileObjectId, QsParams) ->
    build_set_metadata_prepare_rest_args_fun(deprecated_id, MetadataType, FileObjectId, QsParams).


%% @private
build_set_metadata_prepare_rest_args_fun(Endpoint, MetadataType, ValidId, QsParams) ->
    fun(#api_test_ctx{data = Data0}) ->
        % 'metadata' is required key but it may not be present in Data in case of
        % missing required data test cases. Because it is send via http body and
        % as such server will interpret this as empty string <<>> those test cases
        % must be skipped.
        case maps:is_key(<<"metadata">>, Data0) of
            false ->
                skip;
            true ->
                {Id, Data1} = api_test_utils:maybe_substitute_bad_id(ValidId, Data0),

                RestPath = case Endpoint of
                    new_id -> ?NEW_ID_METADATA_REST_PATH(Id, MetadataType);
                    deprecated_path -> ?DEPRECATED_PATH_METADATA_REST_PATH(Id, MetadataType);
                    deprecated_id -> ?DEPRECATED_ID_METADATA_REST_PATH(Id, MetadataType)
                end,

                #rest_args{
                    method = put,
                    headers = case MetadataType of
                        <<"rdf">> -> #{<<"content-type">> => <<"application/rdf+xml">>};
                        _ -> #{<<"content-type">> => <<"application/json">>}
                    end,
                    path = http_utils:append_url_parameters(
                        RestPath,
                        maps:with(QsParams, Data1)
                    ),
                    body = case maps:get(<<"metadata">>, Data1) of
                        Metadata when is_binary(Metadata) -> Metadata;
                        Metadata when is_map(Metadata) -> json_utils:encode(Metadata)
                    end
                }
        end
    end.


%% @private
build_set_metadata_prepare_gs_args_fun(MetadataType, FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Aspect, Data1} = case MetadataType of
            <<"json">> ->
                % 'metadata' is required key but it may not be present in
                % Data in case of missing required data test cases
                case maps:take(<<"metadata">>, Data0) of
                    {Meta, _} ->
                        % Primitive metadata were specified as binaries to be send via REST,
                        % but gs needs them decoded first to be able to send them properly
                        {json_metadata, Data0#{<<"metadata">> => maybe_decode_json(Meta)}};
                    error ->
                        {json_metadata, Data0}
                end;
            <<"rdf">> ->
                {rdf_metadata, Data0};
            <<"xattrs">> ->
                {xattrs, Data0}
        end,
        {GriId, Data2} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data1),

        #gs_args{
            operation = create,
            gri = #gri{type = op_file, id = GriId, aspect = Aspect, scope = Scope},
            data = Data2
        }
    end.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    api_test_env:init_per_suite(Config, #onenv_test_config{
        envs = [
            {op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}
        ],
        posthook = fun(NewConfig) ->
            application:start(ssl),
            hackney:start(),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    hackney:stop(),
    application:stop(ssl).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 30}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
maybe_decode_json(MaybeEncodedJson) ->
    try
        json_utils:decode(MaybeEncodedJson)
    catch _:_ ->
        MaybeEncodedJson
    end.
