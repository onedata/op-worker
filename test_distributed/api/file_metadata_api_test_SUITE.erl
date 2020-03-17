%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file metadata basic API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(file_metadata_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_test_utils.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    get_rdf_metadata_test/1
]).

all() ->
    ?ALL([
        get_rdf_metadata_test
    ]).


-define(ATTEMPTS, 90).
-define(SCENARIO_NAME, atom_to_binary(?FUNCTION_NAME, utf8)).


-define(SPACE_1, <<"space1">>).
-define(SPACE_2, <<"space2">>).

-define(USER_IN_SPACE_1, <<"user1">>).
-define(USER_IN_SPACE_1_AUTH, ?USER(?USER_IN_SPACE_1)).

-define(USER_IN_SPACE_2, <<"user3">>).
-define(USER_IN_SPACE_2_AUTH, ?USER(?USER_IN_SPACE_2)).

-define(USER_IN_BOTH_SPACES, <<"user2">>).
-define(USER_IN_BOTH_SPACES_AUTH, ?USER(?USER_IN_BOTH_SPACES)).


%%%===================================================================
%%% Test functions
%%%===================================================================


get_rdf_metadata_test(Config) ->
    [Provider2, Provider1] = Providers = ?config(op_worker_nodes, Config),
    Provider2DomainBin = ?GET_DOMAIN_BIN(Provider2),

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    UserSessId = GetSessionFun(Provider2),

    RootDirPath = filename:join(["/", ?SPACE_2, ?SCENARIO_NAME]),
    {ok, _RootDirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, RootDirPath, 8#777),

    RdfMetadata = <<"<rdf>metadata</rdf>">>,

    DirWithoutRdfMetadataPath = filename:join([RootDirPath, <<"dir_without_rdf_metadata">>]),
    {ok, DirWithoutRdfMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithoutRdfMetadataPath, 8#777),

    DirWithRdfMetadataPath = filename:join([RootDirPath, <<"dir_with_rdf_metadata">>]),
    {ok, DirWithRdfMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithRdfMetadataPath, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, DirWithRdfMetadataGuid}, rdf, RdfMetadata, []),

    RegularFileWithoutRdfMetadataPath = filename:join([RootDirPath, <<"file_without_rdf_metadata">>]),
    {ok, RegularFileWithoutRdfMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithoutRdfMetadataPath, 8#777),

    RegularFileWithRdfMetadataPath = filename:join([RootDirPath, <<"file_with_rdf_metadata">>]),
    {ok, RegularFileWithRdfMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithRdfMetadataPath, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, RegularFileWithRdfMetadataGuid}, rdf, RdfMetadata, []),

    SupportedClientsPerNode = #{
        Provider1 => [?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        Provider2 => [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH]
    },

    ClientSpecForGetRdfInSpace2Scenarios = #client_spec{
        correct = [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [?NOBODY],
        forbidden = [?USER_IN_SPACE_1_AUTH],
        supported_clients_per_node = SupportedClientsPerNode
    },

    % Special case -> any user can make requests for shares but if request is
    % being made using credentials by user not supported on specific provider
    % ?ERROR_USER_NOT_SUPPORTED will be returned
    ClientSpecForShareScenarios = #client_spec{
        correct = [?NOBODY, ?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [],
        forbidden = [],
        supported_clients_per_node = SupportedClientsPerNode
    },

    ConstructPrepareRestArgsFun = fun(FileId) -> fun(_) ->
        #rest_args{
            method = get,
            path = <<"data/", FileId/binary, "/metadata/rdf">>
        }
    end end,
    ConstructPrepareDeprecatedFilePathRestArgsFun = fun(FilePath) -> fun(_) ->
        #rest_args{
            method = get,
            path = <<"metadata/rdf/", FilePath/binary>>
        }
    end end,
    ConstructPrepareDeprecatedFileIdRestArgsFun = fun(Fileid) -> fun(_) ->
        #rest_args{
            method = get,
            path = <<"metadata-id/rdf/", Fileid/binary>>
        }
    end end,
    ConstructPrepareGsArgsFun = fun(FileId, Scope) -> fun(_) ->
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = FileId, aspect = rdf_metadata, scope = Scope}
        }
    end end,

    ValidateSuccessfulRestResultFun = fun(_, {ok, ?HTTP_200_OK, Response}) ->
        ?assertEqual(RdfMetadata, Response)
    end,
    ValidateSuccessfulGsResultFun = fun(_, {ok, Result}) ->
        ?assertEqual(#{<<"metadata">> => RdfMetadata}, Result)
    end,
    ValidateNoRdfSetRestResultFun = fun(_, {ok, ?HTTP_400_BAD_REQUEST, Response}) ->
        ?assertEqual(?REST_ERROR(?ERROR_POSIX(?ENODATA)), Response)
    end,

    lists:foreach(fun({
        FileType,
        FileWithoutRdfMetadataPath, FileWithoutRdfMetadataGuid,
        FileWithRdfMetadataPath, FileWithRdfMetadataGuid
    }) ->
        {ok, FileWithoutRdfMetadataObjectId} = file_id:guid_to_objectid(FileWithoutRdfMetadataGuid),
        {ok, FileWithRdfMetadataObjectId} = file_id:guid_to_objectid(FileWithRdfMetadataGuid),

        {ok, ShareId} = lfm_proxy:create_share(Provider2, UserSessId, {guid, FileWithRdfMetadataGuid}, <<"share">>),
        ShareGuid = file_id:guid_to_share_guid(FileWithRdfMetadataGuid, ShareId),
        {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

        % Wait for metadata sync between providers
        ?assertMatch(
            {ok, #file_attr{shares = [ShareId]}},
            lfm_proxy:stat(Provider1, GetSessionFun(Provider1), {guid, FileWithRdfMetadataGuid}),
            ?ATTEMPTS
        ),

        ?assert(api_test_utils:run_scenarios(Config, [

            %% TEST GET RDF METADATA FOR FILE WITH RDF METADATA SET

            #scenario_spec{
                name = <<"Get rdf metadata from ", FileType/binary, " with rdf set using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetRdfInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(FileWithRdfMetadataObjectId),
                validate_result_fun = ValidateSuccessfulRestResultFun
            },
            #scenario_spec{
                name = <<"Get rdf metadata from ", FileType/binary, " with rdf set using /files/ rest endpoint">>,
                type = rest_with_file_path,
                target_nodes = Providers,
                client_spec = ClientSpecForGetRdfInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(FileWithRdfMetadataPath),
                validate_result_fun = ValidateSuccessfulRestResultFun
            },
            #scenario_spec{
                name = <<"Get rdf metadata from ", FileType/binary, " with rdf set using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetRdfInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(FileWithRdfMetadataObjectId),
                validate_result_fun = ValidateSuccessfulRestResultFun
            },
            #scenario_spec{
                name = <<"Get rdf metadata from ", FileType/binary, " with rdf set using gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForGetRdfInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(FileWithRdfMetadataGuid, private),
                validate_result_fun = ValidateSuccessfulGsResultFun
            },

            %% TEST GET RDF METADATA FOR SHARED FILE WITH RDF METADATA SET

            #scenario_spec{
                name = <<"Get rdf metadata from shared ", FileType/binary, " with rdf set using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(ShareObjectId),
                validate_result_fun = ValidateSuccessfulRestResultFun
            },
            #scenario_spec{
                name = <<"Get rdf metadata from shared ", FileType/binary, " with rdf set using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(ShareObjectId),
                validate_result_fun = ValidateSuccessfulRestResultFun
            },
            #scenario_spec{
                name = <<"Get rdf metadata from shared ", FileType/binary, " with rdf set using gs private api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, public),
                validate_result_fun = ValidateSuccessfulGsResultFun
            },
            #scenario_spec{
                name = <<"Get rdf metadata from shared ", FileType/binary, " with rdf set using public gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, private),
                validate_result_fun = fun(_, Result) ->
                    ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                end
            },

            %% TEST GET RDF METADATA FOR FILE WITHOUT RDF METADATA SET

            #scenario_spec{
                name = <<"Get rdf metadata from ", FileType/binary, " without rdf set using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetRdfInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(FileWithoutRdfMetadataObjectId),
                validate_result_fun = ValidateNoRdfSetRestResultFun
            },
            #scenario_spec{
                name = <<"Get rdf metadata from ", FileType/binary, " without rdf set using /files/ rest endpoint">>,
                type = rest_with_file_path,
                target_nodes = Providers,
                client_spec = ClientSpecForGetRdfInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(FileWithoutRdfMetadataPath),
                validate_result_fun = ValidateNoRdfSetRestResultFun
            },
            #scenario_spec{
                name = <<"Get rdf metadata from ", FileType/binary, " without rdf set using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetRdfInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(FileWithoutRdfMetadataObjectId),
                validate_result_fun = ValidateNoRdfSetRestResultFun
            },
            #scenario_spec{
                name = <<"Get rdf metadata from ", FileType/binary, " without rdf set using gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForGetRdfInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(FileWithoutRdfMetadataGuid, private),
                validate_result_fun = fun(_, Result) ->
                    ?assertEqual(?ERROR_POSIX(?ENODATA), Result)
                end
            }
        ]))
    end, [
        {
            <<"dir">>,
            DirWithoutRdfMetadataPath, DirWithoutRdfMetadataGuid,
            DirWithRdfMetadataPath, DirWithRdfMetadataGuid
        },
        {
            <<"file">>,
            RegularFileWithoutRdfMetadataPath, RegularFileWithoutRdfMetadataGuid,
            RegularFileWithRdfMetadataPath, RegularFileWithRdfMetadataGuid
        }
    ]),

    %% TEST GET RDF METADATA FOR FILE ON PROVIDER NOT SUPPORTING USER

    ClientSpecForGetRdfInSpace1Scenarios = #client_spec{
        correct = [?USER_IN_SPACE_1_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [?NOBODY],
        forbidden = [?USER_IN_SPACE_2_AUTH],
        supported_clients_per_node = SupportedClientsPerNode
    },
    ValidateRestGetRdfOnProvidersNotSupportingUserFun = fun
        (#api_test_ctx{node = Node, client = Client}, {ok, ?HTTP_400_BAD_REQUEST, Response}) when
            Node == Provider2,
            Client == ?USER_IN_BOTH_SPACES_AUTH
        ->
            ?assertEqual(?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin)), Response);
        (_TestCaseCtx, {ok, ?HTTP_200_OK, Response}) ->
            ?assertEqual(RdfMetadata, Response)
    end,
    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_1),
    lfm_proxy:set_metadata(Provider1, ?ROOT_SESS_ID, {guid, Space1Guid}, rdf, RdfMetadata, []),
    {ok, Space1ObjectId} = file_id:guid_to_objectid(Space1Guid),

    ?assert(api_test_utils:run_scenarios(Config, [
        #scenario_spec{
            name = <<"Get rdf metadata from ", ?SPACE_1/binary, " with rdf set on provider not supporting user using /data/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForGetRdfInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareRestArgsFun(Space1ObjectId),
            validate_result_fun = ValidateRestGetRdfOnProvidersNotSupportingUserFun
        },
        #scenario_spec{
            name = <<"Get rdf metadata from ", ?SPACE_1/binary, " with rdf set on provider not supporting user using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = Providers,
            client_spec = ClientSpecForGetRdfInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(<<"/", ?SPACE_1/binary>>),
            validate_result_fun = ValidateRestGetRdfOnProvidersNotSupportingUserFun
        },
        #scenario_spec{
            name = <<"Get rdf metadata from ", ?SPACE_1/binary, " with rdf set on provider not supporting user using /files-id/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForGetRdfInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(Space1ObjectId),
            validate_result_fun = ValidateRestGetRdfOnProvidersNotSupportingUserFun
        },
        #scenario_spec{
            name = <<"Get rdf metadata from ", ?SPACE_1/binary, " with rdf set on provider not supporting user using gs api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpecForGetRdfInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareGsArgsFun(Space1Guid, private),
            validate_result_fun = fun
                (#api_test_ctx{node = Node, client = Client}, Result) when
                    Node == Provider2,
                    Client == ?USER_IN_BOTH_SPACES_AUTH
                ->
                    ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin), Result);
                (_TestCaseCtx, {ok, Result}) ->
                    ?assertEqual(#{<<"metadata">> => RdfMetadata}, Result)
            end
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
        initializer:mock_auth_manager(NewConfig3, _CheckIfUserIsSupported = true),
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
