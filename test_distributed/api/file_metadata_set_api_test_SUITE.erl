%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file metadata set basic API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(file_metadata_set_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("file_metadata_api_test_utils.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    % Set rdf metadata test cases
    set_file_rdf_metadata_test/1,
    set_shared_file_rdf_metadata_test/1,
    set_file_rdf_metadata_on_provider_not_supporting_space_test/1,

    % Set json metadata test cases
    set_file_json_metadata_test/1,
    set_file_primitive_json_metadata_test/1,
    set_shared_file_json_metadata_test/1,
    set_file_json_metadata_on_provider_not_supporting_space_test/1,

    % Set xattrs test cases
    set_file_xattrs_test/1,
    set_shared_file_xattrs_test/1,
    set_file_xattrs_on_provider_not_supporting_space_test/1
]).

all() ->
    ?ALL([
        set_file_rdf_metadata_test,
        set_shared_file_rdf_metadata_test,
        set_file_rdf_metadata_on_provider_not_supporting_space_test,

        set_file_json_metadata_test,
        set_file_primitive_json_metadata_test,
        set_shared_file_json_metadata_test,
        set_file_json_metadata_on_provider_not_supporting_space_test,

        set_file_xattrs_test,
        set_shared_file_xattrs_test,
        set_file_xattrs_on_provider_not_supporting_space_test
    ]).


%%%===================================================================
%%% Set rdf metadata functions
%%%===================================================================


set_file_rdf_metadata_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    {FileType, FilePath, FileGuid, _} = create_random_file(P1, P2, ?SPACE_2, false, Config),

    GetExpCallResultFun = fun(_TestCtx) -> ok end,

    DataSpec = #data_spec{
        required = [<<"metadata">>],
        correct_values = #{<<"metadata">> => [?RDF_METADATA_1, ?RDF_METADATA_2]}
    },
    VerifyEnvFun = fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            ?assertMatch({error, ?ENODATA}, get_rdf(TestNode, FileGuid, Config), ?ATTEMPTS),
            true;
        (expected_success, #api_test_ctx{node = TestNode, data = #{<<"metadata">> := Metadata}}) ->
            lists:foreach(fun(Node) ->
                ?assertMatch({ok, Metadata}, get_rdf(Node, FileGuid, Config), ?ATTEMPTS)
            end, Providers),

            case Metadata == ?RDF_METADATA_2 of
                true ->
                    % Remove ?RDF_METADATA_2 to test setting ?RDF_METADATA_1 in other scenario on clean state
                    ?assertMatch(ok, remove_rdf(TestNode, FileGuid, Config)),
                    % Wait for removal to be synced between providers.
                    lists:foreach(fun(Node) ->
                        ?assertMatch({error, ?ENODATA}, get_rdf(Node, FileGuid, Config), ?ATTEMPTS)
                    end, Providers);
                false ->
                    ok
            end,
            true
    end,

    set_metadata_test_base(
        <<"rdf">>,
        FileType, FilePath, FileGuid, undefined,
        create_validate_set_metadata_rest_call_fun(GetExpCallResultFun),
        create_validate_set_metadata_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun,
        Providers,
        ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
        DataSpec,
        _QsParams = [],
        Config
    ).


set_shared_file_rdf_metadata_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    {FileType, FilePath, FileGuid, ShareId} = create_random_file(P1, P2, ?SPACE_2, true, Config),

    GetExpCallResultFun = fun(_TestCtx) -> ?ERROR_NOT_SUPPORTED end,

    DataSpec = #data_spec{
        required = [<<"metadata">>],
        correct_values = #{<<"metadata">> => [?RDF_METADATA_1, ?RDF_METADATA_2]}
    },
    VerifyEnvFun = fun(_, #api_test_ctx{node = TestNode}) ->
        ?assertMatch({error, ?ENODATA}, get_rdf(TestNode, FileGuid, Config), ?ATTEMPTS),
        true
    end,

    set_metadata_test_base(
        <<"rdf">>,
        FileType, FilePath, FileGuid, ShareId,
        create_validate_set_metadata_rest_call_fun(GetExpCallResultFun),
        create_validate_set_metadata_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun,
        Providers,
        ?CLIENT_SPEC_FOR_SHARE_SCENARIOS(Config),
        DataSpec,
        _QsParams = [],
        Config
    ).


set_file_rdf_metadata_on_provider_not_supporting_space_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    {FileType, FilePath, FileGuid, _} = create_random_file(P1, P1, ?SPACE_1, false, Config),

    GetExpCallResultFun = fun(_TestCtx) -> ?ERROR_SPACE_NOT_SUPPORTED_BY(?GET_DOMAIN_BIN(P2)) end,

    DataSpec = #data_spec{
        required = [<<"metadata">>],
        correct_values = #{<<"metadata">> => [?RDF_METADATA_1]}
    },
    VerifyEnvFun = fun(_, #api_test_ctx{node = TestNode}) ->
        ?assertMatch({error, ?ENODATA}, get_rdf(TestNode, FileGuid, Config), ?ATTEMPTS),
        true
    end,

    set_metadata_test_base(
        <<"rdf">>,
        FileType, FilePath, FileGuid, undefined,
        create_validate_set_metadata_rest_call_fun(GetExpCallResultFun),
        create_validate_set_metadata_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun,
        [P2],
        ?CLIENT_SPEC_FOR_SPACE_1_SCENARIOS(Config),
        DataSpec,
        _QsParams = [],
        Config
    ).


%% @private
get_rdf(Node, FileGuid, Config) ->
    SessId = ?USER_IN_BOTH_SPACES_SESS_ID(Node, Config),
    lfm_proxy:get_metadata(Node, SessId, {guid, FileGuid}, rdf, [], false).


%% @private
remove_rdf(Node, FileGuid, Config) ->
    SessId = ?USER_IN_BOTH_SPACES_SESS_ID(Node, Config),
    lfm_proxy:remove_metadata(Node, SessId, {guid, FileGuid}, rdf).


%%%===================================================================
%%% Set json metadata functions
%%%===================================================================


set_file_json_metadata_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    {FileType, FilePath, FileGuid, _} = create_random_file(P1, P2, ?SPACE_2, false, Config),

    ExampleJson = #{<<"attr1">> => [0, 1, <<"val">>]},

    DataSpec = #data_spec{
        required = [<<"metadata">>],
        optional = QsParams = [<<"filter_type">>, <<"filter">>],
        correct_values = #{
            <<"metadata">> => [ExampleJson],
            <<"filter_type">> => [<<"keypath">>],
            <<"filter">> => [
                <<"[1]">>,                  % Test creating placeholder array for nonexistent previously json
                <<"[1].attr1.[1]">>,        % Test setting attr in existing array
                <<"[1].attr1.[2].attr22">>, % Test error when trying to set subjson to binary (<<"val">> in ExampleJson)
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
            {<<"metadata">>, <<"{\"aaa\": aaa}">>, {rest_handler, ?ERROR_BAD_VALUE_JSON(<<"metadata">>)}},

            {<<"filter_type">>, <<"dummy">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filter_type">>, [<<"keypath">>])},

            % Below differences between error returned by rest and gs are results of sending
            % parameters via qs in REST, so they lost their original type and are cast to binary
            {<<"filter_type">>, 100, {rest_with_file_path, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filter_type">>, [<<"keypath">>])}},
            {<<"filter_type">>, 100, {rest, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filter_type">>, [<<"keypath">>])}},
            {<<"filter_type">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"filter_type">>)}},
            {<<"filter">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"filter">>)}}
        ]
    },
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
            ?assertMatch({error, ?ENODATA}, get_json(TestNode, FileGuid, Config), ?ATTEMPTS),
            true;
        (expected_success, #api_test_ctx{node = TestNode} = TestCtx) ->
            FilterOrError = GetRequestFilterArg(TestCtx),
            lists:foreach(fun(Node) ->
                % Below expected metadata depends on the tested parameters combination order.
                % First only required params will be tested, then with only one optional params,
                % next with 2 and so on. If optional param has multiple values then those later
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
                        {ok, [null, #{<<"attr1">> => [0, ExampleJson, <<"val">>, null, null, ExampleJson]}]};
                    {ok, [<<"[1]">>, <<"attr2">>, <<"[2]">>]} ->
                        {ok, [null, #{
                            <<"attr1">> => [0, ExampleJson, <<"val">>, null, null, ExampleJson],
                            <<"attr2">> => [null, null, ExampleJson]
                        }]}
                end,
                ?assertMatch(ExpResult, get_json(Node, FileGuid, Config), ?ATTEMPTS)
            end, Providers),

            case FilterOrError of
                {ok, Filter} when Filter == [] orelse Filter == [<<"[1]">>, <<"attr2">>, <<"[2]">>] ->
                    % Remove metadata after:
                    %   - last successful params combination tested so that test cases for next
                    %     client can be run on clean state,
                    %   - combinations without all params. Because they do not use filters it
                    %     is impossible to tell whether operation failed or value was overridden
                    %     (those tes cases are setting the same ExampleJson).
                    % Metadata are not removed after other test cases so that they can test
                    % updating metadata using `filter` param.
                    ?assertMatch(ok, remove_json(TestNode, FileGuid, Config)),
                    % Wait for changes to be synced between providers. Otherwise it can possible
                    % interfere with tests on other node (e.g. information about deletion that
                    % comes after setting ExampleJson and before setting using filter results in
                    % json metadata removal. In such case next test using 'filter' parameter should expect
                    % ExpMetadata = #{<<"attr1">> => [null, null, null, null, null, ExampleJson]}
                    % rather than above one as that will be the result of setting ExampleJson
                    % with attr1.[5] filter and no prior json set)
                    lists:foreach(fun(Node) ->
                        ?assertMatch({error, ?ENODATA}, get_json(Node, FileGuid, Config), ?ATTEMPTS)
                    end, Providers);
                _ ->
                    ok
            end,
            true
    end,

    set_metadata_test_base(
        <<"json">>,
        FileType, FilePath, FileGuid, undefined,
        create_validate_set_metadata_rest_call_fun(GetExpCallResultFun),
        create_validate_set_metadata_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun,
        Providers,
        ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
        DataSpec,
        QsParams,
        Config
    ).


set_file_primitive_json_metadata_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    {FileType, FilePath, FileGuid, _} = create_random_file(P1, P2, ?SPACE_2, false, Config),

    GetExpCallResultFun = fun(_TestCtx) -> ok end,

    DataSpec = #data_spec{
        required = [<<"metadata">>],
        correct_values = #{<<"metadata">> => [
            <<"{}">>, <<"[]">>, <<"true">>, <<"0">>, <<"0.1">>,
            <<"null">>, <<"\"string\"">>
        ]}
    },
    VerifyEnvFun = fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            ?assertMatch({error, ?ENODATA}, get_json(TestNode, FileGuid, Config), ?ATTEMPTS),
            true;
        (expected_success, #api_test_ctx{node = TestNode, data = #{<<"metadata">> := Metadata}}) ->
            ExpMetadata = json_utils:decode(Metadata),
            lists:foreach(fun(Node) ->
                ?assertMatch({ok, ExpMetadata}, get_json(Node, FileGuid, Config), ?ATTEMPTS)
            end, Providers),

            case Metadata of
                <<"\"string\"">> ->
                    % Remove metadata after last successful parameters combination tested so that
                    % next tests can start from setting rather then updating metadata
                    ?assertMatch(ok, remove_json(TestNode, FileGuid, Config)),
                    lists:foreach(fun(Node) ->
                        ?assertMatch({error, ?ENODATA}, get_json(Node, FileGuid, Config), ?ATTEMPTS)
                    end, Providers);
                _ ->
                    ok
            end,
            true
    end,

    set_metadata_test_base(
        <<"json">>,
        FileType, FilePath, FileGuid, undefined,
        create_validate_set_metadata_rest_call_fun(GetExpCallResultFun),
        create_validate_set_metadata_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun,
        Providers,
        ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
        DataSpec,
        _QsParams = [],
        Config
    ).


set_shared_file_json_metadata_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    {FileType, FilePath, FileGuid, ShareId} = create_random_file(P1, P2, ?SPACE_2, true, Config),

    GetExpCallResultFun = fun(_TestCtx) -> ?ERROR_NOT_SUPPORTED end,

    DataSpec = #data_spec{
        required = [<<"metadata">>],
        correct_values = #{<<"metadata">> => [?JSON_METADATA_4, ?JSON_METADATA_5]}
    },
    VerifyEnvFun = fun(_, #api_test_ctx{node = TestNode}) ->
        ?assertMatch({error, ?ENODATA}, get_json(TestNode, FileGuid, Config), ?ATTEMPTS),
        true
    end,

    set_metadata_test_base(
        <<"json">>,
        FileType, FilePath, FileGuid, ShareId,
        create_validate_set_metadata_rest_call_fun(GetExpCallResultFun),
        create_validate_set_metadata_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun,
        Providers,
        ?CLIENT_SPEC_FOR_SHARE_SCENARIOS(Config),
        DataSpec,
        _QsParams = [],
        Config
    ).


set_file_json_metadata_on_provider_not_supporting_space_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    {FileType, FilePath, FileGuid, _} = create_random_file(P1, P1, ?SPACE_1, false, Config),

    GetExpCallResultFun = fun(_TestCtx) -> ?ERROR_SPACE_NOT_SUPPORTED_BY(?GET_DOMAIN_BIN(P2)) end,

    DataSpec = #data_spec{
        required = [<<"metadata">>],
        correct_values = #{<<"metadata">> => [?JSON_METADATA_4]}
    },
    VerifyEnvFun = fun(_, #api_test_ctx{node = TestNode}) ->
        ?assertMatch({error, ?ENODATA}, get_json(TestNode, FileGuid, Config), ?ATTEMPTS),
        true
    end,

    set_metadata_test_base(
        <<"json">>,
        FileType, FilePath, FileGuid, undefined,
        create_validate_set_metadata_rest_call_fun(GetExpCallResultFun),
        create_validate_set_metadata_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun,
        [P2],
        ?CLIENT_SPEC_FOR_SPACE_1_SCENARIOS(Config),
        DataSpec,
        _QsParams = [],
        Config
    ).


%% @private
get_json(Node, FileGuid, Config) ->
    SessId = ?USER_IN_BOTH_SPACES_SESS_ID(Node, Config),
    lfm_proxy:get_metadata(Node, SessId, {guid, FileGuid}, json, [], false).


%% @private
remove_json(Node, FileGuid, Config) ->
    SessId = ?USER_IN_BOTH_SPACES_SESS_ID(Node, Config),
    lfm_proxy:remove_metadata(Node, SessId, {guid, FileGuid}, json).


%%%===================================================================
%%% Set xattrs functions
%%%===================================================================


set_file_xattrs_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    {FileType, FilePath, FileGuid, _} = create_random_file(P1, P2, ?SPACE_2, false, Config),

    DataSpec = #data_spec{
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
    },
    GetExpCallResultFun = fun(#api_test_ctx{client = Client, data = #{<<"metadata">> := Xattrs}}) ->
        case {Client, maps:is_key(?ACL_KEY, Xattrs)} of
            {?USER_IN_SPACE_2_AUTH, true} ->
                % Only owner can set acl in posix mode
                ?ERROR_POSIX(?EACCES);
            _ ->
                ok
        end
    end,
    VerifyEnvFun = fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            assert_no_xattrs_set(TestNode, FileGuid, Config),
            true;
        (expected_success, #api_test_ctx{node = TestNode, client = Client, data = #{<<"metadata">> := Xattrs}}) ->
            case {Client, maps:is_key(?ACL_KEY, Xattrs)} of
                {?USER_IN_SPACE_2_AUTH, true} ->
                    % Only owner (?USER_IN_BOTH_SPACES) can set acl in posix mode
                    ?assertMatch({error, ?ENODATA}, get_xattr(TestNode, FileGuid, ?ACL_KEY, Config), ?ATTEMPTS);
                _ ->
                    assert_xattrs_set(Providers, FileGuid, Xattrs, Config),
                    remove_xattrs(TestNode, Providers, FileGuid, Xattrs, Config)
            end,
            true
    end,

    set_metadata_test_base(
        <<"xattrs">>,
        FileType, FilePath, FileGuid, undefined,
        create_validate_set_metadata_rest_call_fun(GetExpCallResultFun),
        create_validate_set_metadata_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun,
        Providers,
        ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config),
        DataSpec,
        _QsParams = [],
        Config
    ).


set_shared_file_xattrs_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    {FileType, FilePath, FileGuid, ShareId} = create_random_file(P1, P2, ?SPACE_2, true, Config),

    GetExpCallResultFun = fun(_TestCtx) -> ?ERROR_NOT_SUPPORTED end,

    DataSpec = #data_spec{
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
        ]}
    },
    VerifyEnvFun = fun(_, #api_test_ctx{node = TestNode}) ->
        assert_no_xattrs_set(TestNode, FileGuid, Config),
        true
    end,

    set_metadata_test_base(
        <<"xattrs">>,
        FileType, FilePath, FileGuid, ShareId,
        create_validate_set_metadata_rest_call_fun(GetExpCallResultFun),
        create_validate_set_metadata_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun,
        Providers,
        ?CLIENT_SPEC_FOR_SHARE_SCENARIOS(Config),
        DataSpec,
        _QsParams = [],
        Config
    ).


set_file_xattrs_on_provider_not_supporting_space_test(Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    {FileType, FilePath, FileGuid, _} = create_random_file(P1, P1, ?SPACE_1, false, Config),

    GetExpCallResultFun = fun(_TestCtx) -> ?ERROR_SPACE_NOT_SUPPORTED_BY(?GET_DOMAIN_BIN(P2)) end,

    DataSpec = #data_spec{
        required = [<<"metadata">>],
        correct_values = #{<<"metadata">> => [#{?XATTR_1_KEY => ?XATTR_1_VALUE}]}
    },
    VerifyEnvFun = fun(_, #api_test_ctx{node = TestNode}) ->
        ?assertMatch({error, ?ENODATA}, get_xattr(TestNode, FileGuid, ?XATTR_1_KEY, Config), ?ATTEMPTS),
        true
    end,

    set_metadata_test_base(
        <<"xattrs">>,
        FileType, FilePath, FileGuid, undefined,
        create_validate_set_metadata_rest_call_fun(GetExpCallResultFun),
        create_validate_set_metadata_gs_call_fun(GetExpCallResultFun),
        VerifyEnvFun,
        [P2],
        ?CLIENT_SPEC_FOR_SPACE_1_SCENARIOS(Config),
        DataSpec,
        _QsParams = [],
        Config
    ).


%% @private
assert_xattrs_set(Nodes, FileGuid, Xattrs, Config) ->
    lists:foreach(fun(Node) ->
        lists:foreach(fun({Key, Value}) ->
            ?assertMatch(
                {ok, #xattr{name = Key, value = Value}},
                get_xattr(Node, FileGuid, Key, Config),
                ?ATTEMPTS
            )
        end, maps:to_list(Xattrs))
    end, Nodes).


%% @private
assert_no_xattrs_set(Node, FileGuid, Config) ->
    ?assertMatch(
        {ok, []},
        lfm_proxy:list_xattr(Node, ?USER_IN_BOTH_SPACES_SESS_ID(Node, Config), {guid, FileGuid}, false, true)
    ).


%% @private
remove_xattrs(TestNode, Nodes, FileGuid, Xattrs, Config) ->
    {FileUuid, _SpaceId, _} = file_id:unpack_share_guid(FileGuid),

    lists:foreach(fun({Key, _}) ->
        case Key of
            ?ACL_KEY ->
                ?assertMatch(ok, lfm_proxy:remove_acl(TestNode, ?USER_IN_BOTH_SPACES_SESS_ID(TestNode, Config), {guid, FileGuid}));
            <<?CDMI_PREFIX_STR, _/binary>> ->
                % Because cdmi attributes don't have api to remove them removal must be carried by
                % calling custom_metadata directly
                ?assertMatch(ok, rpc:call(TestNode, custom_metadata, remove_xattr, [FileUuid, Key]));
            _ ->
                ?assertMatch(ok, lfm_proxy:remove_xattr(TestNode, ?USER_IN_BOTH_SPACES_SESS_ID(TestNode, Config), {guid, FileGuid}, Key))
        end,
        lists:foreach(fun(Node) ->
            ?assertMatch({error, ?ENODATA}, get_xattr(Node, FileGuid, Key, Config), ?ATTEMPTS)
        end, Nodes)
    end, maps:to_list(Xattrs)).


%% @private
get_xattr(Node, FileGuid, XattrKey, Config) ->
    lfm_proxy:get_xattr(Node, ?USER_IN_BOTH_SPACES_SESS_ID(Node, Config), {guid, FileGuid}, XattrKey).


%%%===================================================================
%%% Set metadata generic functions
%%%===================================================================


%% @private
create_validate_set_metadata_rest_call_fun(GetExpResultFun) ->
    fun(TestCtx, {ok, RespCode, RespBody}) ->
        case GetExpResultFun(TestCtx) of
            ok ->
                ?assertEqual({?HTTP_204_NO_CONTENT, #{}}, {RespCode, RespBody});
            {error, _} = Error ->
                ExpRestError = {errors:to_http_code(Error), ?REST_ERROR(Error)},
                ?assertEqual(ExpRestError, {RespCode, RespBody})
        end
    end.


%% @private
create_validate_set_metadata_gs_call_fun(GetExpResultFun) ->
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
    FileType :: binary(),      %% <<"dir">> | <<"file">>
    FilePath :: file_meta:path(),
    FileGuid :: file_id:file_guid(),
    ShareId :: undefined | od_share:id(),
    ValidateRestCallResultFun :: fun((api_test_ctx(), {ok, RespCode :: pos_integer(), RespBody :: term()}) -> ok),
    ValidateGsCallResultFun :: fun((api_test_ctx(), Result :: term()) -> ok),
    VerifyEnvFun :: fun((ShouldSucceed :: boolean(), api_test_env()) -> boolean()),
    Providers :: [node()],
    client_spec(),
    data_spec(),
    QsParameters :: [binary()],
    Config :: proplists:proplist()
) ->
    ok.
set_metadata_test_base(
    MetadataType, FileType, FilePath, FileGuid, _ShareId = undefined,
    ValidateRestCallResultFun, ValidateGsCallResultFun, VerifyEnvFun,
    Providers, ClientSpec, DataSpec, QsParameters, Config
) ->
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ClientSpec,
            verify_fun = VerifyEnvFun,
            scenario_templates = [
                #scenario_template{
                    name = <<"Set ", MetadataType/binary, " metadata for ", FileType/binary, " using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_new_id_set_metadata_rest_args_fun(MetadataType, FileObjectId, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Set ", MetadataType/binary, " metadata for ", FileType/binary, " using deprecated path rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = create_prepare_deprecated_path_set_metadata_rest_args_fun(MetadataType, FilePath, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Set ", MetadataType/binary, " metadata for ", FileType/binary, " using deprecated id rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_deprecated_id_set_metadata_rest_args_fun(MetadataType, FileObjectId, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Set ", MetadataType/binary, " metadata for ", FileType/binary, " using gs endpoint">>,
                    type = gs,
                    prepare_args_fun = create_prepare_set_metadata_gs_args_fun(MetadataType, FileGuid, private),
                    validate_result_fun = ValidateGsCallResultFun
                }
            ],
            data_spec = DataSpec
        }
    ]));
set_metadata_test_base(
    MetadataType, FileType, _FilePath, FileGuid, ShareId,
    ValidateRestCallResultFun, ValidateGsCallResultFun, VerifyEnvFun,
    Providers, ClientSpec, DataSpec, QsParameters, Config
) ->
    FileShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, FileShareObjectId} = file_id:guid_to_objectid(FileShareGuid),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ClientSpec,
            verify_fun = VerifyEnvFun,
            scenario_templates = [
                #scenario_template{
                    name = <<"Set ", MetadataType/binary, " metadata for shared ", FileType/binary, " using /data/ rest endpoint">>,
                    type = rest_not_supported,
                    prepare_args_fun = create_prepare_new_id_set_metadata_rest_args_fun(MetadataType, FileShareObjectId, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Set ", MetadataType/binary, " metadata for shared ", FileType/binary, " using /files-id/ rest endpoint">>,
                    type = rest_not_supported,
                    prepare_args_fun = create_prepare_deprecated_id_set_metadata_rest_args_fun(MetadataType, FileShareObjectId, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Set ", MetadataType/binary, " metadata for shared ", FileType/binary, " using gs public api">>,
                    type = gs_not_supported,
                    prepare_args_fun = create_prepare_set_metadata_gs_args_fun(MetadataType, FileShareGuid, public),
                    validate_result_fun = ValidateGsCallResultFun
                },
                #scenario_template{
                    name = <<"Set ", MetadataType/binary, " metadata for shared ", FileType/binary, " using gs private api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_set_metadata_gs_args_fun(MetadataType, FileShareGuid, private),
                    validate_result_fun = fun(_, Result) ->
                        ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                    end
                }
            ],
            data_spec = DataSpec
        }
    ])).


%% @private
create_prepare_new_id_set_metadata_rest_args_fun(MetadataType, FileObjectId, QsParams) ->
    create_prepare_set_metadata_rest_args_fun(
        MetadataType,
        ?NEW_ID_METADATA_REST_PATH(FileObjectId, MetadataType),
        QsParams
    ).


%% @private
create_prepare_deprecated_path_set_metadata_rest_args_fun(MetadataType, FilePath, QsParams) ->
    create_prepare_set_metadata_rest_args_fun(
        MetadataType,
        ?DEPRECATED_PATH_METADATA_REST_PATH(FilePath, MetadataType),
        QsParams
    ).


%% @private
create_prepare_deprecated_id_set_metadata_rest_args_fun(MetadataType, FileObjectId, QsParams) ->
    create_prepare_set_metadata_rest_args_fun(
        MetadataType,
        ?DEPRECATED_ID_METADATA_REST_PATH(FileObjectId, MetadataType),
        QsParams
    ).


%% @private
create_prepare_set_metadata_rest_args_fun(MetadataType, RestPath, QsParams) ->
    fun(#api_test_ctx{data = Data}) ->
        #rest_args{
            method = put,
            headers = case MetadataType of
                <<"rdf">> -> #{<<"content-type">> => <<"application/rdf+xml">>};
                _ -> #{<<"content-type">> => <<"application/json">>}
            end,
            path = http_utils:append_url_parameters(
                RestPath,
                maps:with(QsParams, utils:ensure_defined(Data, undefined, #{}))
            ),
            body = case maps:get(<<"metadata">>, Data) of
                Metadata when is_binary(Metadata) -> Metadata;
                Metadata when is_map(Metadata) -> json_utils:encode(Metadata)
            end
        }
    end.


%% @private
create_prepare_set_metadata_gs_args_fun(MetadataType, FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {Aspect, Data1} = case MetadataType of
            <<"json">> ->
                % Primitive metadata were specified as binaries to be send via REST,
                % but gs needs them decoded first to be able to send them properly
                Meta = maps:get(<<"metadata">>, Data0),
                {json_metadata, Data0#{<<"metadata">> => maybe_decode_json(Meta)}};
            <<"rdf">> ->
                {rdf_metadata, Data0};
            <<"xattrs">> ->
                {xattrs, Data0}
        end,
        #gs_args{
            operation = create,
            gri = #gri{type = op_file, id = FileGuid, aspect = Aspect, scope = Scope},
            data = Data1
        }
    end.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        NewConfig3 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        ),
        initializer:mock_auth_manager(NewConfig3, _CheckIfUserIsSupported = true),
        lists:foreach(fun(Worker) ->
            % TODO VFS-6251
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, 20),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, 20),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, 20),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, 20),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, 20), % TODO - change to 2 seconds
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off),
            test_utils:set_env(Worker, ?APP_NAME, public_block_size_treshold, 0),
            test_utils:set_env(Worker, ?APP_NAME, public_block_percent_treshold, 0)
        end, ?config(op_worker_nodes, NewConfig3)),
        application:start(ssl),
        hackney:start(),
        NewConfig3
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    hackney:stop(),
    application:stop(ssl),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(_Case, Config) ->
    initializer:mock_share_logic(Config),
    ct:timetrap({minutes, 30}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_random_file(node(), node(), od_space:name(), boolean(), proplists:proplist()) ->
    {binary(), file_meta:path(), file_id:file_guid(), undefined | od_share:id()}.
create_random_file(CreationNode, AssertionNode, SpaceName, CreateShare, Config) ->
    CreationNodeSessId = ?USER_IN_BOTH_SPACES_SESS_ID(CreationNode, Config),
    AssertionNodeSessId = ?USER_IN_BOTH_SPACES_SESS_ID(AssertionNode, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", SpaceName, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, CreationNode, CreationNodeSessId, FilePath),
    ShareId = case CreateShare of
        true ->
            {ok, Id} = ?assertMatch(
                {ok, _},
                lfm_proxy:create_share(CreationNode, CreationNodeSessId, {guid, FileGuid}, <<"share">>)
            ),
            Id;
        false ->
            undefined
    end,

    api_test_utils:wait_for_file_sync(AssertionNode, AssertionNodeSessId, FileGuid),

    {FileType, FilePath, FileGuid, ShareId}.


%% @private
maybe_decode_json(MaybeEncodedJson) ->
    try
        json_utils:decode(MaybeEncodedJson)
    catch _:_ ->
        MaybeEncodedJson
    end.
