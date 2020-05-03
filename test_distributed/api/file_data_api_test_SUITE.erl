%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file data basic API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(file_data_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
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
    get_children_test/1,
    get_attrs_test/1,
    set_mode_test/1
]).

all() ->
    ?ALL([
        get_children_test,
        get_attrs_test,
        set_mode_test
    ]).


%%%===================================================================
%%% Test functions
%%%===================================================================


get_children_test(Config) ->
    [Provider2, Provider1] = Providers = ?config(op_worker_nodes, Config),
    Provider2DomainBin = ?GET_DOMAIN_BIN(Provider2),

    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_1),
    {ok, Space1ObjectId} = file_id:guid_to_objectid(Space1Guid),

    Space2Guid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_2),

    Spaces = [
        {Space1Guid, ?SPACE_1, <<"/", ?SPACE_1/binary>>},
        {Space2Guid, ?SPACE_2, <<"/", ?SPACE_2/binary>>}
    ],

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    UserInBothSpacesRootDirGuid = fslogic_uuid:user_root_dir_guid(?USER_IN_BOTH_SPACES),
    {ok, UserInBothSpacesRootDirObjectId} = file_id:guid_to_objectid(UserInBothSpacesRootDirGuid),

    UserSessId = GetSessionFun(Provider2),

    DirName = ?SCENARIO_NAME,
    DirPath = filename:join(["/", ?SPACE_2, DirName]),
    {ok, DirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirPath, 8#777),
    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    {ok, ShareId} = lfm_proxy:create_share(Provider2, UserSessId, {guid, DirGuid}, <<"share">>),
    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),
    {ok, ShareDirObjectId} = file_id:guid_to_objectid(ShareDirGuid),

    Files = [{FileGuid1, FileName1, FilePath1} | _] = lists:map(fun(Num) ->
        FileName = <<"file", Num>>,
        {ok, FileGuid} = lfm_proxy:create(Provider2, UserSessId, DirGuid, FileName, 8#777),
        {FileGuid, FileName, filename:join([DirPath, FileName])}
    end, [$0, $1, $2, $3, $4]),
    {ok, FileObjectId1} = file_id:guid_to_objectid(FileGuid1),

    % Wait for metadata sync between providers
    ?assertMatch(
        {ok, _},
        lfm_proxy:stat(Provider1, GetSessionFun(Provider1), {guid, FileGuid1}),
        ?ATTEMPTS
    ),

    SupportedClientsPerNode = #{
        Provider1 => [?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        Provider2 => [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH]
    },

    ClientSpecForSpace1Listing = #client_spec{
        correct = [?USER_IN_SPACE_1_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [?NOBODY],
        forbidden = [?USER_IN_SPACE_2_AUTH],
        supported_clients_per_node = SupportedClientsPerNode
    },

    ClientSpecForSpace2Listing = #client_spec{
        correct = [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [?NOBODY],
        forbidden = [?USER_IN_SPACE_1_AUTH],
        supported_clients_per_node = SupportedClientsPerNode
    },

    ClientSpecForUserInBothSpacesUserRootDirListing = #client_spec{
        correct = [?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [?NOBODY],
        forbidden = [?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH],
        supported_clients_per_node = SupportedClientsPerNode
    },

    % Special case -> any user can make requests for shares but if request is
    % being made using credentials by user not supported on specific provider
    % ?ERROR_USER_NOT_SUPPORTED will be returned
    ClientSpecForShareListing = #client_spec{
        correct = [?NOBODY, ?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [],
        forbidden = [],
        supported_clients_per_node = SupportedClientsPerNode
    },

    ParamsSpec = #data_spec{
        optional = [<<"limit">>, <<"offset">>],
        correct_values = #{
            <<"limit">> => [1, 100],
            <<"offset">> => [1, 10]
        },
        bad_values = [
            {<<"limit">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
            {<<"limit">>, -100, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
            {<<"limit">>, 0, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
            {<<"limit">>, 1001, ?ERROR_BAD_VALUE_NOT_IN_RANGE(<<"limit">>, 1, 1000)},
            {<<"offset">>, <<"abc">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)}
        ]
    },

    ConstructPrepareRestArgsFun = fun(FileId) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = get,
                path = http_utils:append_url_parameters(
                    <<"data/", FileId/binary, "/children">>,
                    maps:with([<<"limit">>, <<"offset">>], Data)
                )
            }
        end
    end,
    ConstructPrepareDeprecatedFilePathRestArgsFun = fun(FilePath) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = get,
                path = http_utils:append_url_parameters(
                    <<"files", FilePath/binary>>,
                    maps:with([<<"limit">>, <<"offset">>], Data)
                )
            }
        end
    end,
    ConstructPrepareDeprecatedFileIdRestArgsFun = fun(Fileid) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = get,
                path = http_utils:append_url_parameters(
                    <<"files-id/", Fileid/binary>>,
                    maps:with([<<"limit">>, <<"offset">>], Data)
                )
            }
        end
    end,
    ConstructPrepareGsArgsFun = fun(FileId, Scope) ->
        fun(#api_test_ctx{data = Data}) ->
            #gs_args{
                operation = get,
                gri = #gri{type = op_file, id = FileId, aspect = children, scope = Scope},
                data = Data
            }
        end
    end,

    ValidateRestListedFilesOnProvidersNotSupportingSpace = fun(ExpSuccessResult) -> fun
        (#api_test_ctx{node = Node, client = Client}, {ok, ?HTTP_400_BAD_REQUEST, Response}) when
            Node == Provider2,
            Client == ?USER_IN_BOTH_SPACES_AUTH
        ->
            ?assertEqual(?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin)), Response);
        (_TestCaseCtx, {ok, ?HTTP_200_OK, Response}) ->
            ?assertEqual(ExpSuccessResult, Response)
    end end,

    ?assert(api_test_runner:run_tests(Config, [

        %% TEST LISTING NORMAL DIR

        #scenario_spec{
            name = <<"List normal dir using /data/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace2Listing,
            prepare_args_fun = ConstructPrepareRestArgsFun(DirObjectId),
            validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                validate_listed_files(Response, rest, undefined, Data, Files)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List normal dir using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace2Listing,
            prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(DirPath),
            validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                validate_listed_files(Response, deprecated_rest, undefined, Data, Files)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List normal dir using /files-id/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace2Listing,
            prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(DirObjectId),
            validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                validate_listed_files(Response, deprecated_rest, undefined, Data, Files)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List normal dir using gs api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace2Listing,
            prepare_args_fun = ConstructPrepareGsArgsFun(DirGuid, private),
            validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                validate_listed_files(Result, gs, undefined, Data, Files)
            end,
            data_spec = ParamsSpec
        },

        %% TEST LISTING SHARE DIR

        #scenario_spec{
            name = <<"List shared dir using /data/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForShareListing,
            prepare_args_fun = ConstructPrepareRestArgsFun(ShareDirObjectId),
            validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                validate_listed_files(Response, rest, ShareId, Data, Files)
            end,
            data_spec = ParamsSpec
        },
        % Old endpoint returns "id" and "path" - for now get_path is forbidden
        % for shares so this method returns ?ERROR_NOT_SUPPORTED in case of
        % listing share dir
        #scenario_spec{
            name = <<"List shared dir using /files-id/ rest endpoint">>,
            type = rest_not_supported,
            target_nodes = Providers,
            client_spec = ClientSpecForShareListing,
            prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(ShareDirObjectId),
            validate_result_fun = fun(_TestCaseCtx, {ok, ?HTTP_400_BAD_REQUEST, Response}) ->
                ?assertEqual(?REST_ERROR(?ERROR_NOT_SUPPORTED), Response)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List shared dir using gs api with public scope">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpecForShareListing,
            prepare_args_fun = ConstructPrepareGsArgsFun(ShareDirGuid, public),
            validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                validate_listed_files(Result, gs, ShareId, Data, Files)
            end,
            data_spec = ParamsSpec
        },
        % 'private' scope is forbidden for shares even if user would be able to
        % list children using normal guid
        #scenario_spec{
            name = <<"List shared dir using gs api with private scope">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpecForShareListing,
            prepare_args_fun = ConstructPrepareGsArgsFun(ShareDirGuid, private),
            validate_result_fun = fun(_TestCaseCtx, Result) ->
                ?assertEqual(?ERROR_UNAUTHORIZED, Result)
            end,
            data_spec = ParamsSpec
        }

        %% TEST LISTING FILE

        #scenario_spec{
            name = <<"List file using /data/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace2Listing,
            prepare_args_fun = ConstructPrepareRestArgsFun(FileObjectId1),
            validate_result_fun = fun(_TestCaseCtx, {ok, ?HTTP_200_OK, Response}) ->
                ?assertEqual(#{<<"children">> => [#{
                    <<"id">> => FileObjectId1,
                    <<"name">> => FileName1
                }]}, Response)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List file using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace2Listing,
            prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(FilePath1),
            validate_result_fun = fun(_TestCaseCtx, {ok, ?HTTP_200_OK, Response}) ->
                ?assertEqual(
                    [#{<<"id">> => FileObjectId1, <<"path">> => FilePath1}],
                    Response
                )
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List file using /files-id/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace2Listing,
            prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(FileObjectId1),
            validate_result_fun = fun(_TestCaseCtx, {ok, ?HTTP_200_OK, Response}) ->
                ?assertEqual(
                    [#{<<"id">> => FileObjectId1, <<"path">> => FilePath1}],
                    Response
                )
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List file using gs api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace2Listing,
            prepare_args_fun = ConstructPrepareGsArgsFun(FileGuid1, private),
            validate_result_fun = fun(_TestCaseCtx, {ok, Result}) ->
                ?assertEqual(#{<<"children">> => [FileGuid1]}, Result)
            end,
            data_spec = ParamsSpec
        },

        % LISTING USER ROOT DIR SHOULD LIST ALL SPACES ALSO THOSE NOT SUPPORTED LOCALLY

        #scenario_spec{
            name = <<"List user root dir using /data/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForUserInBothSpacesUserRootDirListing,
            prepare_args_fun = ConstructPrepareRestArgsFun(UserInBothSpacesRootDirObjectId),
            validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                validate_listed_files(Response, rest, undefined, Data, Spaces)
            end,
            data_spec = ParamsSpec
        },
        % Special case - listing files using path '/' works for all users but
        % returns only user spaces
        #scenario_spec{
            name = <<"List user root dir using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = Providers,
            client_spec = #client_spec{
                correct = [?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
                unauthorized = [?NOBODY],
                forbidden = [],
                supported_clients_per_node = SupportedClientsPerNode
            },
            prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(<<"/">>),
            validate_result_fun = fun(#api_test_ctx{client = Client, data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                ClientSpaces = case Client of
                    ?USER_IN_SPACE_1_AUTH ->
                        [{Space1Guid, ?SPACE_1, <<"/", ?SPACE_1/binary>>}];
                    ?USER_IN_SPACE_2_AUTH ->
                        [{Space2Guid, ?SPACE_2, <<"/", ?SPACE_2/binary>>}];
                    ?USER_IN_BOTH_SPACES_AUTH ->
                        Spaces
                end,
                validate_listed_files(Response, deprecated_rest, undefined, Data, ClientSpaces)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List user root dir using /files-id/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForUserInBothSpacesUserRootDirListing,
            prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(UserInBothSpacesRootDirObjectId),
            validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, ?HTTP_200_OK, Response}) ->
                validate_listed_files(Response, deprecated_rest, undefined, Data, Spaces)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List user root dir using gs api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpecForUserInBothSpacesUserRootDirListing,
            prepare_args_fun = ConstructPrepareGsArgsFun(UserInBothSpacesRootDirGuid, private),
            validate_result_fun = fun(#api_test_ctx{data = Data}, {ok, Result}) ->
                validate_listed_files(Result, gs, undefined, Data, Spaces)
            end,
            data_spec = ParamsSpec
        },

        %% TEST LISTING ON PROVIDERS NOT SUPPORTING SPACE

        #scenario_spec{
            name = <<"List dir on provider not supporting user using /data/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace1Listing,
            prepare_args_fun = ConstructPrepareRestArgsFun(Space1ObjectId),
            validate_result_fun = ValidateRestListedFilesOnProvidersNotSupportingSpace(#{<<"children">> => []}),
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List dir on provider not supporting user using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace1Listing,
            prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(<<"/", ?SPACE_1/binary>>),
            validate_result_fun = ValidateRestListedFilesOnProvidersNotSupportingSpace([]),
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List dir on provider not supporting user using /files-id/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace1Listing,
            prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(Space1ObjectId),
            validate_result_fun = ValidateRestListedFilesOnProvidersNotSupportingSpace([]),
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List dir on provider not supporting user using gs api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace1Listing,
            prepare_args_fun = ConstructPrepareGsArgsFun(Space1Guid, private),
            validate_result_fun = fun
                (#api_test_ctx{node = Node, client = Client}, Result) when
                    Node == Provider2,
                    Client == ?USER_IN_BOTH_SPACES_AUTH
                ->
                    ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin), Result);
                (_TestCaseCtx, {ok, Result}) ->
                    ?assertEqual(#{<<"children">> => []}, Result)
            end,
            data_spec = ParamsSpec
        }
    ])).


get_attrs_test(Config) ->
    [Provider2, Provider1] = Providers = ?config(op_worker_nodes, Config),

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    UserSessId = GetSessionFun(Provider2),

    RootDirPath = filename:join(["/", ?SPACE_2, ?SCENARIO_NAME]),
    {ok, _RootDirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, RootDirPath, 8#777),

    DirPath = filename:join([RootDirPath, <<"dir">>]),
    {ok, DirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirPath, 8#777),

    RegularFilePath = filename:join([RootDirPath, <<"file">>]),
    {ok, RegularFileGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFilePath, 8#777),

    SupportedClientsPerNode = #{
        Provider1 => [?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        Provider2 => [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH]
    },

    ClientSpecForSpace2Scenarios = #client_spec{
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

    PrivateDataSpec = #data_spec{
        optional = [<<"attribute">>],
        correct_values = #{<<"attribute">> => ?PRIVATE_BASIC_ATTRIBUTES},
        bad_values = [
            {<<"attribute">>, true, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, ?PRIVATE_BASIC_ATTRIBUTES)},
            {<<"attribute">>, 10, {gs, ?ERROR_BAD_VALUE_BINARY(<<"attribute">>)}},
            {<<"attribute">>, <<"NaN">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, ?PRIVATE_BASIC_ATTRIBUTES)}
        ]
    },
    PublicDataSpec = #data_spec{
        optional = [<<"attribute">>],
        correct_values = #{<<"attribute">> => ?PUBLIC_BASIC_ATTRIBUTES},
        bad_values = [
            {<<"attribute">>, true, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, ?PUBLIC_BASIC_ATTRIBUTES)},
            {<<"attribute">>, 10, {gs, ?ERROR_BAD_VALUE_BINARY(<<"attribute">>)}},
            {<<"attribute">>, <<"NaN">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, ?PUBLIC_BASIC_ATTRIBUTES)},
            {<<"attribute">>, <<"owner_id">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"attribute">>, ?PUBLIC_BASIC_ATTRIBUTES)}
        ]
    },

    GetExpectedResultFun = fun(#api_test_ctx{data = Data}, ShareId, JsonAttrs) ->
        RequestedAttributes = case maps:get(<<"attribute">>, Data, undefined) of
            undefined ->
                case ShareId of
                    undefined -> ?PRIVATE_BASIC_ATTRIBUTES;
                    _ -> ?PUBLIC_BASIC_ATTRIBUTES
                end;
            Attr ->
                [Attr]
        end,
        {ok, maps:with(RequestedAttributes, JsonAttrs)}
    end,
    ConstructValidateSuccessfulRestResultFun = fun(ShareId, JsonAttrsInNormalMode) ->
        fun(TestCtx, {ok, RespCode, RespBody}) ->
            {ExpCode, ExpBody} = case GetExpectedResultFun(TestCtx, ShareId, JsonAttrsInNormalMode) of
                {ok, ExpResult} ->
                    {?HTTP_200_OK, ExpResult};
                {error, _} = ExpError ->
                    {errors:to_http_code(ExpError), ?REST_ERROR(ExpError)}
            end,
            ?assertEqual({ExpCode, ExpBody}, {RespCode, RespBody})
        end
    end,
    ConstructValidateSuccessfulGsResultFun = fun(ShareId, JsonAttrsInNormalMode) ->
        fun(TestCtx, Result) ->
            case GetExpectedResultFun(TestCtx, ShareId, JsonAttrsInNormalMode) of
                {ok, ExpResult} ->
                    ?assertEqual({ok, #{<<"attributes">> => ExpResult}}, Result);
                {error, _} = ExpError ->
                    ?assertEqual(ExpError, Result)
            end
        end
    end,

    ConstructPrepareRestArgsFun = fun(FileId) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = get,
                path = http_utils:append_url_parameters(
                    <<"data/", FileId/binary>>,
                    maps:with([<<"attribute">>], Data)
                )
            }
        end
    end,
    ConstructPrepareDeprecatedFilePathRestArgsFun = fun(FilePath) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = get,
                path = http_utils:append_url_parameters(
                    <<"metadata/attrs", FilePath/binary>>,
                    maps:with([<<"attribute">>], Data)
                )
            }
        end
    end,
    ConstructPrepareDeprecatedFileIdRestArgsFun = fun(Fileid) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = get,
                path = http_utils:append_url_parameters(
                    <<"metadata-id/attrs/", Fileid/binary>>,
                    maps:with([<<"attribute">>], Data)
                )
            }
        end
    end,
    ConstructPrepareGsArgsFun = fun(FileId, Scope) ->
        fun(#api_test_ctx{data = Data}) ->
            #gs_args{
                operation = get,
                gri = #gri{type = op_file, id = FileId, aspect = attrs, scope = Scope},
                data = Data
            }
        end
    end,

    lists:foreach(fun({FileType, FilePath, FileGuid}) ->
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

        {ok, ShareId1} = lfm_proxy:create_share(Provider2, UserSessId, {guid, FileGuid}, <<"share1">>),
        {ok, ShareId2} = lfm_proxy:create_share(Provider2, UserSessId, {guid, FileGuid}, <<"share2">>),

        ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId1),
        {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

        {ok, FileAttrs} = ?assertMatch(
            {ok, #file_attr{shares = [ShareId2, ShareId1]}},
            lfm_proxy:stat(Provider1, GetSessionFun(Provider1), {guid, FileGuid}),
            ?ATTEMPTS
        ),
        JsonAttrsInNormalMode = attrs_to_json(undefined, FileAttrs),
        JsonAttrsInShareMode = attrs_to_json(ShareId1, FileAttrs),

        ?assert(api_test_runner:run_tests(Config, [

            %% TEST GET ATTRS FOR FILE IN NORMAL MODE

            #scenario_spec{
                name = <<"Get attrs from ", FileType/binary, " using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForSpace2Scenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(FileObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, JsonAttrsInNormalMode),
                data_spec = PrivateDataSpec
            },
            #scenario_spec{
                name = <<"Get attrs from ", FileType/binary, " using /files/ rest endpoint">>,
                type = rest_with_file_path,
                target_nodes = Providers,
                client_spec = ClientSpecForSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(FilePath),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, JsonAttrsInNormalMode),
                data_spec = PrivateDataSpec
            },
            #scenario_spec{
                name = <<"Get attrs from ", FileType/binary, " using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(FileObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, JsonAttrsInNormalMode),
                data_spec = PrivateDataSpec
            },
            #scenario_spec{
                name = <<"Get attrs from ", FileType/binary, " using gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForSpace2Scenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(FileGuid, private),
                validate_result_fun = ConstructValidateSuccessfulGsResultFun(undefined, JsonAttrsInNormalMode),
                data_spec = PrivateDataSpec
            },

            %% TEST GET ATTRS FOR SHARED FILE

            #scenario_spec{
                name = <<"Get attrs from shared ", FileType/binary, " using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(ShareObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(ShareId1, JsonAttrsInShareMode),
                data_spec = PublicDataSpec
            },
            #scenario_spec{
                name = <<"Get attrs from shared ", FileType/binary, " using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(ShareObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(ShareId1, JsonAttrsInShareMode),
                data_spec = PublicDataSpec
            },
            #scenario_spec{
                name = <<"Get attrs from shared ", FileType/binary, " using gs public api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, public),
                validate_result_fun = ConstructValidateSuccessfulGsResultFun(ShareId1, JsonAttrsInShareMode),
                data_spec = PublicDataSpec
            },
            #scenario_spec{
                name = <<"Get attrs from shared ", FileType/binary, " using private gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, private),
                validate_result_fun = fun(_, Result) ->
                    ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                end,
                data_spec = PrivateDataSpec
            }
        ]))
    end, [
        {<<"dir">>, DirPath, DirGuid},
        {<<"file">>, RegularFilePath, RegularFileGuid}
    ]),

    %% TEST GET ATTRS FOR FILE ON PROVIDER NOT SUPPORTING USER

    Provider2DomainBin = ?GET_DOMAIN_BIN(Provider2),

    ClientSpecForSpace1Scenarios = #client_spec{
        correct = [?USER_IN_SPACE_1_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [?NOBODY],
        forbidden = [?USER_IN_SPACE_2_AUTH],
        supported_clients_per_node = SupportedClientsPerNode
    },

    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_1),
    {ok, Space1ObjectId} = file_id:guid_to_objectid(Space1Guid),
    {ok, Space1Attrs} = lfm_proxy:stat(Provider1, GetSessionFun(Provider1), {guid, Space1Guid}),
    Space1JsonAttrs = attrs_to_json(undefined, Space1Attrs),

    ValidateRestGetMetadataOnProvidersNotSupportingUserFun = fun
        (#api_test_ctx{node = Node, client = Client}, {ok, ?HTTP_400_BAD_REQUEST, Response}) when
            Node == Provider2,
            Client == ?USER_IN_BOTH_SPACES_AUTH
        ->
            ?assertEqual(?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin)), Response);
        (TestCaseCtx, {ok, ?HTTP_200_OK, Response}) ->
            {ok, ExpAttrs} = GetExpectedResultFun(TestCaseCtx, undefined, Space1JsonAttrs),
            ?assertEqual(ExpAttrs, Response)
    end,

    ?assert(api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = <<"Get attrs from ", ?SPACE_1/binary, " on provider not supporting user using /data/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace1Scenarios,
            prepare_args_fun = ConstructPrepareRestArgsFun(Space1ObjectId),
            validate_result_fun = ValidateRestGetMetadataOnProvidersNotSupportingUserFun,
            data_spec = PrivateDataSpec
        },
        #scenario_spec{
            name = <<"Get attrs from ", ?SPACE_1/binary, " on provider not supporting user using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace1Scenarios,
            prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(<<"/", ?SPACE_1/binary>>),
            validate_result_fun = ValidateRestGetMetadataOnProvidersNotSupportingUserFun,
            data_spec = PrivateDataSpec
        },
        #scenario_spec{
            name = <<"Get attrs from ", ?SPACE_1/binary, " on provider not supporting user using /files-id/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace1Scenarios,
            prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(Space1ObjectId),
            validate_result_fun = ValidateRestGetMetadataOnProvidersNotSupportingUserFun,
            data_spec = PrivateDataSpec
        },
        #scenario_spec{
            name = <<"Get attrs from ", ?SPACE_1/binary, " on provider not supporting user using gs api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace1Scenarios,
            prepare_args_fun = ConstructPrepareGsArgsFun(Space1Guid, private),
            validate_result_fun = fun
                (#api_test_ctx{node = Node, client = Client}, Result) when
                    Node == Provider2,
                    Client == ?USER_IN_BOTH_SPACES_AUTH
                ->
                    ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin), Result);
                (TestCaseCtx, {ok, Result}) ->
                    {ok, ExpAttrs} = GetExpectedResultFun(TestCaseCtx, undefined, Space1JsonAttrs),
                    ?assertEqual(#{<<"attributes">> => ExpAttrs}, Result)
            end,
            data_spec = PrivateDataSpec
        }
    ])).


set_mode_test(Config) ->
    [Provider2, Provider1] = Providers = ?config(op_worker_nodes, Config),

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    UserSessId = GetSessionFun(Provider2),

    RootDirPath = filename:join(["/", ?SPACE_2, ?SCENARIO_NAME]),
    {ok, RootDirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, RootDirPath, 8#777),
    {ok, ShareId} = lfm_proxy:create_share(Provider2, UserSessId, {guid, RootDirGuid}, <<"share">>),

    DirPath = filename:join([RootDirPath, <<"dir">>]),
    {ok, DirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirPath, 8#777),

    RegularFilePath = filename:join([RootDirPath, <<"file">>]),
    {ok, RegularFileGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFilePath, 8#777),

    % Wait for metadata sync between providers
    ?assertMatch(
        {ok, _},
        lfm_proxy:stat(Provider1, GetSessionFun(Provider1), {guid, RegularFileGuid}),
        ?ATTEMPTS
    ),

    SupportedClientsPerNode = #{
        Provider1 => [?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        Provider2 => [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH]
    },

    ClientSpecForSpace2Scenarios = #client_spec{
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

    DataSpec = #data_spec{
        required = [<<"mode">>],
        correct_values = #{<<"mode">> => [<<"0000">>, <<"0111">>, <<"0777">>]},
        bad_values = [
            {<<"mode">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)},
            {<<"mode">>, <<"integer">>, ?ERROR_BAD_VALUE_INTEGER(<<"mode">>)}
        ]
    },

    ConstructPrepareRestArgsFun = fun(FileId) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = put,
                path = <<"data/", FileId/binary>>,
                headers = #{<<"content-type">> => <<"application/json">>},
                body = json_utils:encode(Data)
            }
        end
    end,
    ConstructPrepareDeprecatedFilePathRestArgsFun = fun(FilePath) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = put,
                path = <<"metadata/attrs", FilePath/binary>>,
                headers = #{<<"content-type">> => <<"application/json">>},
                body = json_utils:encode(Data)
            }
        end
    end,
    ConstructPrepareDeprecatedFileIdRestArgsFun = fun(Fileid) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = put,
                path = <<"metadata-id/attrs/", Fileid/binary>>,
                headers = #{<<"content-type">> => <<"application/json">>},
                body = json_utils:encode(Data)
            }
        end
    end,
    ConstructPrepareGsArgsFun = fun(FileId, Scope) ->
        fun(#api_test_ctx{data = Data}) ->
            #gs_args{
                operation = create,
                gri = #gri{type = op_file, id = FileId, aspect = attrs, scope = Scope},
                data = Data
            }
        end
    end,

    SetMode = fun(Node, FileGuid, NewMode) ->
        ?assertMatch(ok, lfm_proxy:set_perms(Node, GetSessionFun(Node), {guid, FileGuid}, NewMode))
    end,
    GetMode = fun(Node, FileGuid) ->
        {ok, #file_attr{mode = Mode}} = ?assertMatch(
            {ok, _},
            lfm_proxy:stat(Node, GetSessionFun(Node), {guid, FileGuid})
        ),
        Mode
    end,

    GetExpectedResultFun = fun
        (#api_test_ctx{client = ?USER_IN_BOTH_SPACES_AUTH}) -> ok;
        (_) -> ?ERROR_POSIX(?EACCES)
    end,
    ValidateRestSuccessfulCallFun =  fun(TestCtx, {ok, RespCode, RespBody}) ->
        {ExpCode, ExpBody} = case GetExpectedResultFun(TestCtx) of
            ok ->
                {?HTTP_204_NO_CONTENT, #{}};
            {error, _} = ExpError ->
                {errors:to_http_code(ExpError), ?REST_ERROR(ExpError)}
        end,
        ?assertEqual({ExpCode, ExpBody}, {RespCode, RespBody})
    end,
    ValidateRestOperationNotSupportedFun = fun(_, {ok, ?HTTP_400_BAD_REQUEST, Response}) ->
        ?assertEqual(?REST_ERROR(?ERROR_NOT_SUPPORTED), Response)
    end,

    ConstructVerifyEnvForSuccessfulCallsFun = fun(FileGuid) -> fun
        (expected_failure, #api_test_ctx{node = TestNode}) ->
            ?assertMatch(8#777, GetMode(TestNode, FileGuid), ?ATTEMPTS),
            true;
        (expected_success, #api_test_ctx{client = ?USER_IN_SPACE_2_AUTH, node = TestNode}) ->
            ?assertMatch(8#777, GetMode(TestNode, FileGuid), ?ATTEMPTS),
            true;
        (expected_success, #api_test_ctx{client = ?USER_IN_BOTH_SPACES_AUTH, data = #{<<"mode">> := ModeBin}}) ->
            Mode = binary_to_integer(ModeBin, 8),
            lists:foreach(fun(Node) -> ?assertMatch(Mode, GetMode(Node, FileGuid), ?ATTEMPTS) end, Providers),
            true
    end end,

    lists:foreach(fun({FileType, FilePath, FileGuid}) ->
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

        ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
        {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

        ?assert(api_test_runner:run_tests(Config, [

            %% TEST SET MODE FOR FILE IN NORMAL MODE

            #scenario_spec{
                name = <<"Set mode for ", FileType/binary, " using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForSpace2Scenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(FileObjectId),
                validate_result_fun = ValidateRestSuccessfulCallFun,
                verify_fun = ConstructVerifyEnvForSuccessfulCallsFun(FileGuid),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Set mode for ", FileType/binary, " using /files/ rest endpoint">>,
                type = rest_with_file_path,
                target_nodes = Providers,
                client_spec = ClientSpecForSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(FilePath),
                validate_result_fun = ValidateRestSuccessfulCallFun,
                verify_fun = ConstructVerifyEnvForSuccessfulCallsFun(FileGuid),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Set mode for ", FileType/binary, " using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(FileObjectId),
                validate_result_fun = ValidateRestSuccessfulCallFun,
                verify_fun = ConstructVerifyEnvForSuccessfulCallsFun(FileGuid),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Set mode for ", FileType/binary, " using gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForSpace2Scenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(FileGuid, private),
                validate_result_fun = fun(TestCtx, Result) ->
                    case GetExpectedResultFun(TestCtx) of
                        ok ->
                            ?assertEqual({ok, undefined}, Result);
                        {error, _} = ExpError ->
                            ?assertEqual(ExpError, Result)
                    end
                end,
                verify_fun = ConstructVerifyEnvForSuccessfulCallsFun(FileGuid),
                data_spec = DataSpec
            }
        ])),

        %% TEST SET MODE FOR SHARED FILE SHOULD FAIL

        % Reset mode for file
        lists:foreach(fun(Node) -> SetMode(Node, FileGuid, 8#777) end, Providers),

        VerifyEnvForShareCallsFun = fun(_, #api_test_ctx{node = Node}) ->
            ?assertMatch(8#777, GetMode(Node, FileGuid)),
            true
        end,

        ?assert(api_test_runner:run_tests(Config, [

            #scenario_spec{
                name = <<"Set mode for shared ", FileType/binary, " using /data/ rest endpoint">>,
                type = rest_not_supported,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(ShareObjectId),
                validate_result_fun = ValidateRestOperationNotSupportedFun,
                verify_fun = VerifyEnvForShareCallsFun,
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Set mode for shared ", FileType/binary, " using /files-id/ rest endpoint">>,
                type = rest_not_supported,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(ShareObjectId),
                validate_result_fun = ValidateRestOperationNotSupportedFun,
                verify_fun = VerifyEnvForShareCallsFun,
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Set mode for shared ", FileType/binary, " using gs public api">>,
                type = gs_not_supported,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, public),
                validate_result_fun = fun(_TestCaseCtx, Result) ->
                    ?assertEqual(?ERROR_NOT_SUPPORTED, Result)
                end,
                verify_fun = VerifyEnvForShareCallsFun,
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Set mode for shared ", FileType/binary, " using private gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, private),
                validate_result_fun = fun(_, Result) ->
                    ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                end,
                verify_fun = VerifyEnvForShareCallsFun,
                data_spec = DataSpec
            }
        ]))
    end, [
        {<<"dir">>, DirPath, DirGuid},
        {<<"file">>, RegularFilePath, RegularFileGuid}
    ]),

    %% TEST SET MODE FOR FILE ON PROVIDER NOT SUPPORTING USER

    Space1RootDirPath = filename:join(["/", ?SPACE_1, ?SCENARIO_NAME]),
    {ok, Space1RootDirGuid} = lfm_proxy:mkdir(Provider1, GetSessionFun(Provider1), Space1RootDirPath, 8#777),
    {ok, Space1RootObjectId} = file_id:guid_to_objectid(Space1RootDirGuid),

    ClientSpecForSpace1Scenarios = #client_spec{
        correct = [?USER_IN_SPACE_1_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [?NOBODY],
        forbidden = [?USER_IN_SPACE_2_AUTH],
        supported_clients_per_node = SupportedClientsPerNode
    },
    Provider2DomainBin = ?GET_DOMAIN_BIN(Provider2),

    ValidateRestSetMetadataOnProvidersNotSupportingUserFun = fun(_TestCtx, {ok, RespCode, RespBody}) ->
        ExpCode = ?HTTP_400_BAD_REQUEST,
        ExpBody = ?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin)),
        ?assertEqual({ExpCode, ExpBody}, {RespCode, RespBody})
    end,
    VerifyEnvFunForSetModeInSpace1Scenarios = fun
        (_, #api_test_ctx{node = Node}) ->
            ?assertMatch(8#777, GetMode(Node, Space1RootDirGuid), ?ATTEMPTS),
            true
    end,

    ?assert(api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = <<"Set mode for root dir in ", ?SPACE_1/binary, " on provider not supporting user using /data/ rest endpoint">>,
            type = rest,
            target_nodes = [Provider2],
            client_spec = ClientSpecForSpace1Scenarios,
            prepare_args_fun = ConstructPrepareRestArgsFun(Space1RootObjectId),
            validate_result_fun = ValidateRestSetMetadataOnProvidersNotSupportingUserFun,
            verify_fun = VerifyEnvFunForSetModeInSpace1Scenarios,
            data_spec = DataSpec
        },
        #scenario_spec{
            name = <<"Set mode for root dir in ", ?SPACE_1/binary, " on provider not supporting user using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = [Provider2],
            client_spec = ClientSpecForSpace1Scenarios,
            prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(Space1RootDirPath),
            validate_result_fun = ValidateRestSetMetadataOnProvidersNotSupportingUserFun,
            verify_fun = VerifyEnvFunForSetModeInSpace1Scenarios,
            data_spec = DataSpec
        },
        #scenario_spec{
            name = <<"Set mode for root dir in ", ?SPACE_1/binary, " on provider not supporting user using /files-id/ rest endpoint">>,
            type = rest,
            target_nodes = [Provider2],
            client_spec = ClientSpecForSpace1Scenarios,
            prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(Space1RootObjectId),
            validate_result_fun = ValidateRestSetMetadataOnProvidersNotSupportingUserFun,
            verify_fun = VerifyEnvFunForSetModeInSpace1Scenarios,
            data_spec = DataSpec
        },
        #scenario_spec{
            name = <<"Set mode for root dir in ", ?SPACE_1/binary, " on provider not supporting user using gs api">>,
            type = gs,
            target_nodes = [Provider2],
            client_spec = ClientSpecForSpace1Scenarios,
            prepare_args_fun = ConstructPrepareGsArgsFun(Space1RootDirGuid, private),
            validate_result_fun = fun(_TestCtx, Result) ->
                ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin), Result)
            end,
            verify_fun = VerifyEnvFunForSetModeInSpace1Scenarios,
            data_spec = DataSpec
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
            % TODO VFS-6251
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


%% @private
-spec validate_listed_files(
    ListedChildren :: term(),
    Format :: gs | rest | deprecated_rest,
    ShareId :: undefined | od_share:id(),
    Params :: map(),
    AllFiles :: [{file_id:file_guid(), Name :: binary(), Path :: binary()}]
) ->
    ok | no_return().
validate_listed_files(ListedChildren, Format, ShareId, Params, AllFiles) ->
    Limit = maps:get(<<"limit">>, Params, 1000),
    Offset = maps:get(<<"offset">>, Params, 0),

    ExpFiles1 = case Offset >= length(AllFiles) of
        true ->
            [];
        false ->
            lists:sublist(AllFiles, Offset+1, Limit)
    end,

    ExpFiles2 = lists:map(fun({Guid, Name, Path}) ->
        {file_id:guid_to_share_guid(Guid, ShareId), Name, Path}
    end, ExpFiles1),

    ExpFiles3 = case Format of
        gs ->
            #{<<"children">> => lists:map(fun({Guid, _Name, _Path}) ->
                Guid
            end, ExpFiles2)};
        rest ->
            #{<<"children">> => lists:map(fun({Guid, Name, _Path}) ->
                {ok, ObjectId} = file_id:guid_to_objectid(Guid),
                #{
                    <<"id">> => ObjectId,
                    <<"name">> => Name
                }
            end, ExpFiles2)};
        deprecated_rest ->
            lists:map(fun({Guid, _Name, Path}) ->
                {ok, ObjectId} = file_id:guid_to_objectid(Guid),
                #{
                    <<"id">> => ObjectId,
                    <<"path">> => Path
                }
            end, ExpFiles2)
    end,

    ?assertEqual(ExpFiles3, ListedChildren).


%% @private
-spec attrs_to_json(od_share:id(), #file_attr{}) -> map().
attrs_to_json(ShareId, #file_attr{
    guid = Guid,
    name = Name,
    mode = Mode,
    uid = Uid,
    gid = Gid,
    atime = ATime,
    mtime = MTime,
    ctime = CTime,
    type = Type,
    size = Size,
    shares = Shares,
    provider_id = ProviderId,
    owner_id = OwnerId
}) ->
    PublicAttrs = #{
        <<"name">> => Name,
        <<"atime">> => ATime,
        <<"mtime">> => MTime,
        <<"ctime">> => CTime,
        <<"type">> => case Type of
            ?REGULAR_FILE_TYPE -> <<"reg">>;
            ?DIRECTORY_TYPE -> <<"dir">>;
            ?SYMLINK_TYPE -> <<"lnk">>
        end,
        <<"size">> => Size
    },

    case ShareId of
        undefined ->
            {ok, ObjectId} = file_id:guid_to_objectid(Guid),

            PublicAttrs#{
                <<"file_id">> => ObjectId,
                <<"mode">> => <<"0", (integer_to_binary(Mode, 8))/binary>>,
                <<"storage_user_id">> => Uid,
                <<"storage_group_id">> => Gid,
                <<"shares">> => Shares,
                <<"provider_id">> => ProviderId,
                <<"owner_id">> => OwnerId
            };
        _ ->
            ShareGuid = file_id:guid_to_share_guid(Guid, ShareId),
            {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

            PublicAttrs#{
                <<"file_id">> => ShareObjectId,
                <<"mode">> => <<"0", (integer_to_binary(2#111 band Mode, 8))/binary>>,
                <<"storage_user_id">> => ?SHARE_UID,
                <<"storage_group_id">> => ?SHARE_GID,
                <<"shares">> => case lists:member(ShareId, Shares) of
                    true -> [ShareId];
                    false -> []
                end,
                <<"provider_id">> => <<"unknown">>,
                <<"owner_id">> => <<"unknown">>
            }
    end.
