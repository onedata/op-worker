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

-include("api_test_runner.hrl").
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

-type metadata_type() :: rdf | json | xattrs.
-type set_metadata_policy():: set_metadata | do_not_set_metadata.


-define(ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


delete_file_rdf_metadata_with_rdf_set_test(Config) ->
    delete_file_metadata_test_base(rdf, ?RDF_METADATA_1, set_metadata, Config).


delete_file_rdf_metadata_without_rdf_set_test(Config) ->
    delete_file_metadata_test_base(rdf, ?RDF_METADATA_2, do_not_set_metadata, Config).


delete_file_json_metadata_with_json_set_test(Config) ->
    delete_file_metadata_test_base(json, ?JSON_METADATA_1, set_metadata, Config).


delete_file_json_metadata_without_json_set_test(Config) ->
    delete_file_metadata_test_base(json, ?JSON_METADATA_2, do_not_set_metadata, Config).


%% @private
-spec delete_file_metadata_test_base(metadata_type(), term(), set_metadata_policy(),
    api_test_runner:config()) -> ok.
delete_file_metadata_test_base(MetadataType, Metadata, SetMetadataPolicy, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    {FileType, FileGuid, ShareId} = create_shared_file(Config),

    SetupFun = build_setup_fun(SetMetadataPolicy, FileGuid, MetadataType, Metadata, Nodes),
    VerifyFun = build_verify_fun(SetMetadataPolicy, FileGuid, MetadataType, Metadata, Nodes),

    delete_metadata_test_base(
        MetadataType, FileType, FileGuid, ShareId,
        Nodes, SetupFun, VerifyFun, undefined,
        _RandomlySelectScenario = false, Config
    ).


delete_file_xattrs(Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    {FileType, FileGuid, ShareId} = create_shared_file(Config),

    FullXattrSet = #{
        ?RDF_METADATA_KEY => ?RDF_METADATA_1,
        ?JSON_METADATA_KEY => ?JSON_METADATA_4,
        ?ACL_KEY => ?OWNER_ONLY_ALLOW_ACL,
        ?MIMETYPE_KEY => ?MIMETYPE_1,
        ?TRANSFER_ENCODING_KEY => ?TRANSFER_ENCODING_1,
        ?CDMI_COMPLETION_STATUS_KEY => ?CDMI_COMPLETION_STATUS_1,
        ?XATTR_1_KEY => ?XATTR_1_VALUE
    },

    SetupFun = fun() ->
        % Check to prevent race condition in tests (see onenv_api_test_runner
        % common pitfalls 1).
        RandNode = lists_utils:random_element(Nodes),
        case {ok, FullXattrSet} == get_xattrs(RandNode, FileGuid) of
            true ->
                ok;
            false ->
                set_xattrs(lists_utils:random_element(Nodes), FileGuid, FullXattrSet),
                lists:foreach(fun(Node) ->
                    ?assertEqual({ok, FullXattrSet}, get_xattrs(Node, FileGuid), ?ATTEMPTS)
                end, Nodes)
        end
    end,

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

    VerifyFun = fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            ?assertMatch({ok, FullXattrSet}, get_xattrs(TestNode, FileGuid), ?ATTEMPTS),
            true;
        (expected_success, #api_test_ctx{data = #{<<"keys">> := Keys}}) ->
            ExpXattrs = maps:without(Keys, FullXattrSet),
            lists:foreach(fun(Node) ->
                ?assertEqual({ok, ExpXattrs}, get_xattrs(Node, FileGuid), ?ATTEMPTS)
            end, Nodes),
            true
    end,

    delete_metadata_test_base(
        xattrs, FileType, FileGuid, ShareId,
        Nodes, SetupFun, VerifyFun, DataSpec,
        _RandomlySelectScenario = true, Config
    ).


%%%===================================================================
%%% Get metadata generic functions
%%%===================================================================


%% @private
-spec create_shared_file(api_test_runner:config()) ->
    {api_test_utils:file_type(), file_id:file_guid(), od_share:id()}.
create_shared_file(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),

    SpaceOwnerSessIdP1 = api_test_env:get_user_session_id(user2, p1, Config),
    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    UserSessIdP2 = api_test_env:get_user_session_id(user3, p2, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1Node, UserSessIdP1, FilePath, 8#707),
    {ok, ShareId} = lfm_proxy:create_share(P1Node, SpaceOwnerSessIdP1, {guid, FileGuid}, <<"share">>),

    api_test_utils:wait_for_file_sync(P2Node, UserSessIdP2, FileGuid),

    {FileType, FileGuid, ShareId}.


%% @private
-spec build_setup_fun(set_metadata_policy(), file_id:file_guid(), metadata_type(),
    Metadata :: term(), [node()]) -> verify_fun().
build_setup_fun(set_metadata, FileGuid, MetadataType, Metadata, Nodes) ->
    fun() ->
        % Check to prevent race condition in tests (see onenv_api_test_runner
        % common pitfalls 1).
        RandNode = lists_utils:random_element(Nodes),
        case {ok, Metadata} == get_metadata(RandNode, FileGuid, MetadataType) of
            true ->
                ok;
            false ->
                set_metadata(RandNode, FileGuid, MetadataType, Metadata),
                lists:foreach(fun(Node) ->
                    ?assertMatch({ok, Metadata}, get_metadata(Node, FileGuid, MetadataType), ?ATTEMPTS)
                end, Nodes)
        end
    end;
build_setup_fun(do_not_set_metadata, _, _, _, _) ->
    fun() -> ok end.


%% @private
-spec build_verify_fun(set_metadata_policy(), file_id:file_guid(), metadata_type(),
    Metadata :: term(), [node()]) -> verify_fun().
build_verify_fun(set_metadata, FileGuid, MetadataType, ExpMetadata, Nodes) ->
    fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            ?assertMatch({ok, ExpMetadata}, get_metadata(TestNode, FileGuid, MetadataType), ?ATTEMPTS),
            true;
        (expected_success, _) ->
            lists:foreach(fun(Node) ->
                ?assertMatch({error, ?ENODATA}, get_metadata(Node, FileGuid, MetadataType), ?ATTEMPTS)
            end, Nodes),
            true
    end;
build_verify_fun(do_not_set_metadata, FileGuid, MetadataType, _ExpMetadata, Nodes) ->
    fun(_, _) ->
        lists:foreach(fun(Node) ->
            ?assertMatch({error, ?ENODATA}, get_metadata(Node, FileGuid, MetadataType), ?ATTEMPTS)
        end, Nodes),
        true
    end.


%% @private
-spec set_metadata(node(), file_id:file_guid(), metadata_type(), term()) -> ok.
set_metadata(Node, FileGuid, rdf, Metadata) ->
    lfm_proxy:set_metadata(Node, ?ROOT_SESS_ID, {guid, FileGuid}, rdf, Metadata, []);
set_metadata(Node, FileGuid, json, Metadata) ->
    lfm_proxy:set_metadata(Node, ?ROOT_SESS_ID, {guid, FileGuid}, json, Metadata, []);
set_metadata(Node, FileGuid, xattrs, Metadata) ->
    set_xattrs(Node, FileGuid, Metadata).


%% @private
-spec get_metadata(node(), file_id:file_guid(), metadata_type()) -> {ok, term()}.
get_metadata(Node, FileGuid, rdf) ->
    lfm_proxy:get_metadata(Node, ?ROOT_SESS_ID, {guid, FileGuid}, rdf, [], false);
get_metadata(Node, FileGuid, json) ->
    lfm_proxy:get_metadata(Node, ?ROOT_SESS_ID, {guid, FileGuid}, json, [], false);
get_metadata(Node, FileGuid, xattrs) ->
    get_xattrs(Node, FileGuid).


%% @private
-spec set_xattrs(node(), file_id:file_guid(), map()) -> ok.
set_xattrs(Node, FileGuid, Xattrs) ->
    lists:foreach(fun({Key, Val}) ->
        lfm_proxy:set_xattr(
            Node, ?ROOT_SESS_ID, {guid, FileGuid}, #xattr{
                name = Key,
                value = Val
            }
        )
    end, maps:to_list(Xattrs)).


%% @private
-spec get_xattrs(node(), file_id:file_guid()) -> {ok, map()}.
get_xattrs(Node, FileGuid) ->
    FileKey = {guid, FileGuid},
    {ok, Keys} = lfm_proxy:list_xattr(Node, ?ROOT_SESS_ID, FileKey, true, true),

    {ok, lists:foldl(fun(Key, Acc) ->
        % Check in case of race between listing xattrs and fetching xattr value
        case lfm_proxy:get_xattr(Node, ?ROOT_SESS_ID, FileKey, Key) of
            {ok, #xattr{name = Name, value = Value}} ->
                Acc#{Name => Value};
            {error, _} ->
                Acc
        end
    end, #{}, Keys)}.


%% @private
-spec delete_metadata_test_base(
    metadata_type(),
    api_test_utils:file_type(), file_id:file_guid(), undefined | od_share:id(),
    [node()], setup_fun(), verify_fun(), data_spec(),
    RandomlySelectScenario :: boolean(),
    api_test_runner:config()
) ->
    ok.
delete_metadata_test_base(
    MetadataType, FileType, FileGuid, ShareId,
    Nodes, SetupFun, VerifyFun, DataSpec,
    RandomlySelectScenario, Config
) ->
    FileShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Nodes,
            setup_fun = SetupFun,
            verify_fun = VerifyFun,
            client_spec = #client_spec{
                correct = [
                    user2, % space owner - doesn't need any perms
                    user3  % files owner (see fun create_shared_file/1)
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1],
                forbidden_in_space = [{user4, ?ERROR_POSIX(?EACCES)}]
            },
            scenario_templates = [
                #scenario_template{
                    name = str_utils:format("Delete ~p metadata for ~s using gs private api", [
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
            name = str_utils:format("Delete ~p metadata for shared ~s using gs public api", [
                MetadataType, FileType
            ]),
            type = gs_not_supported,
            target_nodes = Nodes,
            client_spec = #client_spec{
                correct = [nobody, user1, user2, user3, user4]
            },
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
build_delete_metadata_prepare_gs_args_fun(MetadataType, FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),

        Aspect = case MetadataType of
            json -> json_metadata;
            rdf -> rdf_metadata;
            xattrs -> xattrs
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
