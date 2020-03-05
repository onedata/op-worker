%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning data basic API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(data_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("api_test_utils.hrl").
-include("lfm_permissions_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    list_children_test/1
]).

all() ->
    ?ALL([
        list_children_test
    ]).


-define(ATTEMPTS, 90).
-define(SCENARIO_NAME, atom_to_binary(?FUNCTION_NAME, utf8)).


%%%===================================================================
%%% Test functions
%%%===================================================================


list_children_test(Config) ->
    [Provider2, Provider1] = ?config(op_worker_nodes, Config),
    Provider2DomainBin = atom_to_binary(?GET_DOMAIN(Provider2), utf8),

    UserId = <<"user2">>,
    SessId = fun(Node) -> ?config({session_id, {UserId, ?GET_DOMAIN(Node)}}, Config) end,

    UserRootDirGuid = fslogic_uuid:user_root_dir_guid(UserId),
    {ok, UserRootDirObjectId} = file_id:guid_to_objectid(UserRootDirGuid),

    Space1 = <<"space1">>,
    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(Space1),
    {ok, Space1ObjectId} = file_id:guid_to_objectid(Space1Guid),

    Space2 = <<"space2">>,
    Space2Guid = fslogic_uuid:spaceid_to_space_dir_guid(Space2),

    Spaces = [
        {Space1Guid, Space1, <<"/", Space1/binary>>},
        {Space2Guid, Space2, <<"/", Space2/binary>>}
    ],

    UserSessId = SessId(Provider2),

    RootDirName = ?SCENARIO_NAME,
    RootDirPath = filename:join(["/", Space2, RootDirName]),
    {ok, RootDirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, RootDirPath, 8#777),
    {ok, RootDirObjectId} = file_id:guid_to_objectid(RootDirGuid),

    {ok, ShareId} = lfm_proxy:create_share(Provider2, UserSessId, {guid, RootDirGuid}, <<"share">>),
    ShareRootDirGuid = file_id:guid_to_share_guid(RootDirGuid, ShareId),
    {ok, ShareRootDirObjectId} = file_id:guid_to_objectid(ShareRootDirGuid),

    Files = [{FileGuid1, _, FilePath1} | _] = lists:map(fun(Num) ->
        FileName = <<"file", Num>>,
        {ok, FileGuid} = lfm_proxy:create(Provider2, UserSessId, RootDirGuid, FileName, 8#777),
        {FileGuid, FileName, filename:join([RootDirPath, FileName])}
    end, [$0, $1, $2, $3, $4]),
    {ok, FileObjectId1} = file_id:guid_to_objectid(FileGuid1),

    % Wait for sync between providers
    ?assertMatch({ok, _}, lfm_proxy:stat(Provider1, SessId(Provider1), {guid, FileGuid1}), ?ATTEMPTS),

    ClientSpec = #client_spec{
        correct = [{user, UserId}],
        unauthorized = [nobody],
        forbidden = [{user, <<"user1">>}]
    },

    ShareClientSpec = #client_spec{
        correct = [nobody, {user, <<"user1">>}, {user, UserId}],
        unauthorized = [],
        forbidden = []
    },

    ConstructPrepareRestArgsFun = fun(FileId) ->
        fun(_Env, Data) ->
            Qs = prepare_list_dir_qs(Data),
            #rest_args{
                method = get,
                path = <<"data/", FileId/binary, "/children", Qs/binary>>
            }
        end
    end,
    ConstructPrepareDeprecatedPathRestArgsFun = fun(FilePath) ->
        fun(_Env, Data) ->
            Qs = prepare_list_dir_qs(Data),
            #rest_args{
                method = get,
                path = <<"files", FilePath/binary, Qs/binary>>
            }
        end
    end,
    ConstructPrepareDeprecatedIdRestArgsFun = fun(Fileid) ->
        fun(_Env, Data) ->
            Qs = prepare_list_dir_qs(Data),
            #rest_args{
                method = get,
                path = <<"files-id/", Fileid/binary, Qs/binary>>
            }
        end
    end,
    ConstructPrepareGsArgsFun = fun(FileId, Scope) ->
        fun(_Env, Data) ->
            #gs_args{
                operation = get,
                gri = #gri{type = op_file, id = FileId, aspect = children, scope = Scope},
                data = Data
            }
        end
    end,
    ValidateListedChildren = fun(ScenarioType, ObjectListed, ShareId, Data, Result) ->
        Limit = maps:get(<<"limit">>, Data, 100),
        Offset = maps:get(<<"offset">>, Data, 0),
        ExpFiles0 = case ObjectListed of
            files -> Files;
            spaces -> Spaces
        end,
        ExpFiles1 = case Offset >= length(ExpFiles0) of
            true -> [];
            false -> lists:sublist(ExpFiles0, Offset+1, Limit)
        end,
        ExpFiles2 = lists:map(fun({Guid, Name, Path}) ->
            {file_id:guid_to_share_guid(Guid, ShareId), Name, Path}
        end, ExpFiles1),
        ExpFiles3 = case ScenarioType of
            deprecated_rest ->
                lists:map(fun({Guid, _Name, Path}) ->
                    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
                    #{
                        <<"id">> => ObjectId,
                        <<"path">> => Path
                    }
                end, ExpFiles2);
            rest ->
                #{<<"children">> => lists:map(fun({Guid, Name, _Path}) ->
                    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
                    #{
                        <<"id">> => ObjectId,
                        <<"name">> => Name
                    }
                end, ExpFiles2)};
            gs ->
                #{<<"children">> => lists:map(fun({Guid, _Name, _Path}) ->
                    Guid
                end, ExpFiles2)}
        end,
        ?assertEqual(ExpFiles3, Result)
    end,
    ParamsSpec = #data_spec{
        optional = [<<"limit">>, <<"offset">>],
        correct_values = #{
            <<"limit">> => [1, 100],
            <<"offset">> => [1, 10]
        },
        bad_values = [
            {<<"limit">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
            {<<"limit">>, -100, ?ERROR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},
            {<<"limit">>, 0, ?ERROR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},
            {<<"offset">>, <<"abc">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)}
        ]
    },

    ?assert(api_test_utils:run_scenarios(Config, [

        %% TEST LISTING NORMAL DIR

        #scenario_spec{
            type = rest,
            target_nodes = [Provider1, Provider2],
            client_spec = ClientSpec,
            prepare_args_fun = ConstructPrepareRestArgsFun(RootDirObjectId),
            validate_result_fun = fun(_TargetNode, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
                ValidateListedChildren(rest, files, undefined, Data, Response)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            type = gs,
            target_nodes = [Provider1, Provider2],
            client_spec = ClientSpec,
            prepare_args_fun = ConstructPrepareGsArgsFun(RootDirGuid, private),
            validate_result_fun = fun(_TargetNode, {ok, Result}, _Env, Data) ->
                ValidateListedChildren(gs, files, undefined, Data, Result)
            end,
            data_spec = ParamsSpec
        },

        %% TEST LISTING SHARE DIR

        #scenario_spec{
            type = rest,
            target_nodes = [Provider1, Provider2],
            client_spec = ShareClientSpec,
            prepare_args_fun = ConstructPrepareRestArgsFun(ShareRootDirObjectId),
            validate_result_fun = fun(_TargetNode, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
                ValidateListedChildren(rest, files, ShareId, Data, Response)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            type = gs,
            target_nodes = [Provider1, Provider2],
            client_spec = ShareClientSpec,
            prepare_args_fun = ConstructPrepareGsArgsFun(ShareRootDirGuid, public),
            validate_result_fun = fun(_TargetNode, {ok, Result}, _Env, Data) ->
                ValidateListedChildren(gs, files, ShareId, Data, Result)
            end,
            data_spec = ParamsSpec
        },
        % 'private' scope is forbidden for shares even if user would be able to
        % list children using normal guid
        #scenario_spec{
            type = gs,
            target_nodes = [Provider1, Provider2],
            client_spec = ShareClientSpec,
            prepare_args_fun = ConstructPrepareGsArgsFun(ShareRootDirGuid, private),
            validate_result_fun = fun(_TargetNode, Result, _Env, _Data) ->
                ?assertEqual(?ERROR_UNAUTHORIZED, Result)
            end,
            data_spec = ParamsSpec
        },

        %% LISTING FILE SHOULD FAIL WITH ?ENOTDIR ERROR

        #scenario_spec{
            type = rest,
            target_nodes = [Provider1, Provider2],
            client_spec = ClientSpec,
            prepare_args_fun = ConstructPrepareRestArgsFun(FileObjectId1),
            validate_result_fun = fun(_TargetNode, {ok, ?HTTP_400_BAD_REQUEST, Response}, _Env, _Data) ->
                ExpError = #{<<"error">> => errors:to_json(?ERROR_POSIX(?ENOTDIR))},
                ?assertEqual(ExpError, Response)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            type = gs,
            target_nodes = [Provider1, Provider2],
            client_spec = ClientSpec,
            prepare_args_fun = ConstructPrepareGsArgsFun(FileGuid1, private),
            validate_result_fun = fun(_TargetNode, Result, _, _) ->
                ?assertEqual(?ERROR_POSIX(?ENOTDIR), Result)
            end,
            data_spec = ParamsSpec
        },

        %% LISTING USER ROOT DIR SHOULD LIST ALL SPACES ALSO THOSE NOT SUPPORTED LOCALLY

        #scenario_spec{
            type = rest,
            target_nodes = [Provider1, Provider2],
            client_spec = ClientSpec,
            prepare_args_fun = ConstructPrepareRestArgsFun(UserRootDirObjectId),
            validate_result_fun = fun(_TargetNode, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
                ValidateListedChildren(rest, spaces, undefined, Data, Response)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            type = gs,
            target_nodes = [Provider1, Provider2],
            client_spec = ClientSpec,
            prepare_args_fun = ConstructPrepareGsArgsFun(UserRootDirGuid, private),
            validate_result_fun = fun(_TargetNode, {ok, Result}, _, Data) ->
                ValidateListedChildren(gs, spaces, undefined, Data, Result)
            end,
            data_spec = ParamsSpec
        },

        %% TEST LISTING ON PROVIDERS NOT SUPPORTING SPACE

        #scenario_spec{
            type = rest,
            target_nodes = [Provider1, Provider2],
            client_spec = ClientSpec#client_spec{forbidden = [{user, <<"user3">>}]},
            prepare_args_fun = ConstructPrepareRestArgsFun(Space1ObjectId),
            validate_result_fun = fun
                (Target, {ok, ?HTTP_400_BAD_REQUEST, Response}, _Env, _Data) when Target == Provider2 ->
                    ExpError = #{<<"error">> => errors:to_json(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin))},
                    ?assertEqual(ExpError, Response);
                (_TargetNode, {ok, ?HTTP_200_OK, Response}, _Env, _Data) ->
                    ?assertEqual(#{<<"children">> => []}, Response)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            type = gs,
            target_nodes = [Provider1, Provider2],
            client_spec = ClientSpec#client_spec{forbidden = [{user, <<"user3">>}]},
            prepare_args_fun = ConstructPrepareGsArgsFun(Space1Guid, private),
            validate_result_fun = fun
                (Target, Result, _, _Data) when Target == Provider2 ->
                    ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin), Result);
                (_TargetNode, {ok, Result}, _, _Data) ->
                    ?assertEqual(#{<<"children">> => []}, Result)
            end,
            data_spec = ParamsSpec
        },

        %% TEST DEPRECATED LISTING NORMAL DIR ENDPOINTS

        #scenario_spec{
            type = rest_with_file_path,
            target_nodes = [Provider2, Provider1],
            client_spec = ClientSpec,
            prepare_args_fun = ConstructPrepareDeprecatedPathRestArgsFun(RootDirPath),
            validate_result_fun = fun(_TargetNode, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
                ValidateListedChildren(deprecated_rest, files, undefined, Data, Response)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            type = rest,
            target_nodes = [Provider2, Provider1],
            client_spec = ClientSpec,
            prepare_args_fun = ConstructPrepareDeprecatedIdRestArgsFun(RootDirObjectId),
            validate_result_fun = fun(_TargetNode, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
                ValidateListedChildren(deprecated_rest, files, undefined, Data, Response)
            end,
            data_spec = ParamsSpec
        },

        %% TEST DEPRECATED LISTING SHARE ENDPOINTS

        #scenario_spec{
            type = rest_not_supported,
            target_nodes = [Provider2, Provider1],
            client_spec = ClientSpec,
            prepare_args_fun = ConstructPrepareDeprecatedIdRestArgsFun(ShareRootDirObjectId),
            validate_result_fun = fun(_TargetNode, {ok, ?HTTP_400_BAD_REQUEST, Response}, _Env, _Data) ->
                ExpError = #{<<"error">> => errors:to_json(?ERROR_NOT_SUPPORTED)},
                ?assertEqual(ExpError, Response)
            end,
            data_spec = ParamsSpec
        },

        %% TEST DEPRECATED LISTING FILE ENDPOINTS

        #scenario_spec{
            type = rest_with_file_path,
            target_nodes = [Provider2, Provider1],
            client_spec = ClientSpec,
            prepare_args_fun = ConstructPrepareDeprecatedPathRestArgsFun(FilePath1),
            validate_result_fun = fun(_TargetNode, {ok, ?HTTP_200_OK, Response}, _Env, _Data) ->
                ?assertEqual([#{
                    <<"id">> => FileObjectId1,
                    <<"path">> => FilePath1
                }], Response)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            type = rest,
            target_nodes = [Provider2, Provider1],
            client_spec = ClientSpec,
            prepare_args_fun = ConstructPrepareDeprecatedIdRestArgsFun(FileObjectId1),
            validate_result_fun = fun(_TargetNode, {ok, ?HTTP_200_OK, Response}, _Env, _Data) ->
                ?assertEqual([#{
                    <<"id">> => FileObjectId1,
                    <<"path">> => FilePath1
                }], Response)
            end,
            data_spec = ParamsSpec
        },

        %% TEST DEPRECATED LISTING USER ROOT DIR SHOULD LIST ALL SPACES ALSO THOSE NOT SUPPORTED LOCALLY

        #scenario_spec{
            type = rest_with_file_path,
            target_nodes = [Provider1, Provider2],
            client_spec = ClientSpec#client_spec{unauthorized = [], forbidden = []},
            prepare_args_fun = ConstructPrepareDeprecatedPathRestArgsFun(<<"/">>),
            validate_result_fun = fun(_TargetNode, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
                ValidateListedChildren(deprecated_rest, spaces, undefined, Data, Response)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            type = rest,
            target_nodes = [Provider1, Provider2],
            client_spec = ClientSpec,
            prepare_args_fun = ConstructPrepareDeprecatedIdRestArgsFun(UserRootDirObjectId),
            validate_result_fun = fun(_TargetNode, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
                ValidateListedChildren(deprecated_rest, spaces, undefined, Data, Response)
            end,
            data_spec = ParamsSpec
        },

        %% TEST DEPRECATED LISTING ON PROVIDERS NOT SUPPORTING SPACE

        #scenario_spec{
            type = rest_with_file_path,
            target_nodes = [Provider1, Provider2],
            client_spec = ClientSpec#client_spec{forbidden = [{user, <<"user3">>}]},
            prepare_args_fun = ConstructPrepareDeprecatedPathRestArgsFun(<<"/", Space1/binary>>),
            validate_result_fun = fun
                (Target, {ok, ?HTTP_400_BAD_REQUEST, Response}, _Env, _Data) when Target == Provider2 ->
                    ExpError = #{<<"error">> => errors:to_json(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin))},
                    ?assertEqual(ExpError, Response);
                (_TargetNode, {ok, ?HTTP_200_OK, Response}, _Env, _Data) ->
                    ?assertEqual([], Response)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            type = rest,
            target_nodes = [Provider1, Provider2],
            client_spec = ClientSpec#client_spec{forbidden = [{user, <<"user3">>}]},
            prepare_args_fun = ConstructPrepareDeprecatedIdRestArgsFun(Space1ObjectId),
            validate_result_fun = fun
                (Target, {ok, ?HTTP_400_BAD_REQUEST, Response}, _Env, _Data) when Target == Provider2 ->
                    ExpError = #{<<"error">> => errors:to_json(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin))},
                    ?assertEqual(ExpError, Response);
                (_TargetNode, {ok, ?HTTP_200_OK, Response}, _Env, _Data) ->
                    ?assertEqual([], Response)
            end,
            data_spec = ParamsSpec
        }
    ])).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)), % TODO - change to 2 seconds
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off),
            test_utils:set_env(Worker, ?APP_NAME, public_block_size_treshold, 0),
            test_utils:set_env(Worker, ?APP_NAME, public_block_percent_treshold, 0)
        end, ?config(op_worker_nodes, NewConfig2)),
        application:start(ssl),
        hackney:start(),
        NewConfig3 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        ),
        initializer:mock_auth_manager(NewConfig3),
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
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


prepare_list_dir_qs(Data) ->
    QsParams = [QsParam || QsParam <- [
        prepare_qs_param(<<"limit">>, Data),
        prepare_qs_param(<<"offset">>, Data)
    ], QsParam /= <<>>],

    case str_utils:join_binary(QsParams, <<"&">>) of
        <<>> ->
            <<>>;
        Qs ->
            <<"?", Qs/binary>>
    end.


prepare_qs_param(ParamName, Data) ->
    case maps:get(ParamName, Data, undefined) of
        undefined ->
            <<>>;
        Value ->
            <<ParamName/binary, "=", (str_utils:to_binary(Value))/binary>>
    end.
