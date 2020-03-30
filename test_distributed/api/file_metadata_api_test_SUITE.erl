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
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/aai/aai.hrl").
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
    get_rdf_metadata_test/1,
    get_json_metadata_test/1,
    get_xattr_metadata_test/1
]).

all() ->
    ?ALL([
        get_rdf_metadata_test,
        get_json_metadata_test,
        get_xattr_metadata_test
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

-define(JSON_METADATA_1, #{
    <<"attr1">> => 1,
    <<"attr2">> => #{
        <<"attr21">> => <<"val21">>,
        <<"attr22">> => <<"val22">>
    }
}).
-define(JSON_METADATA_2, <<"metadata">>).
-define(JSON_METADATA_3, #{
    <<"attr2">> => #{
        <<"attr22">> => [1, 2, 3],
        <<"attr23">> => <<"val23">>
    },
    <<"attr3">> => <<"val3">>
}).
-define(JSON_METADATA_4, #{
    <<"attr3">> => #{
        <<"attr31">> => null
    }
}).
-define(JSON_METADATA_5, #{
    <<"attr3">> => #{
        <<"attr32">> => [<<"a">>, <<"b">>, <<"c">>]
    }
}).

-define(RDF_METADATA_1, <<"<rdf>metadata_1</rdf>">>).
-define(RDF_METADATA_2, <<"<rdf>metadata_2</rdf>">>).

-define(MIMETYPE_1, <<"text/plain">>).
-define(MIMETYPE_2, <<"text/javascript">>).

-define(TRANSFER_ENCODING_1, <<"utf-8">>).
-define(TRANSFER_ENCODING_2, <<"base64">>).

-define(CDMI_COMPLETION_STATUS_1, <<"Completed">>).
-define(CDMI_COMPLETION_STATUS_2, <<"Processing">>).

-define(XATTR_KEY, <<"custom_xattr">>).
-define(XATTR_1_VALUE, <<"value1">>).
-define(XATTR_1, #xattr{name = ?XATTR_KEY, value = ?XATTR_1_VALUE}).
-define(XATTR_2_VALUE, <<"value2">>).
-define(XATTR_2, #xattr{name = ?XATTR_KEY, value = ?XATTR_2_VALUE}).

-define(ACL_1, [#{
    <<"acetype">> => <<"0x", (integer_to_binary(?allow_mask, 16))/binary>>,
    <<"identifier">> => ?everyone,
    <<"aceflags">> => <<"0x", (integer_to_binary(?no_flags_mask, 16))/binary>>,
    <<"acemask">> => <<"0x", (integer_to_binary(?all_container_perms_mask, 16))/binary>>
}]).
-define(ACL_2, [#{
    <<"acetype">> => <<"0x", (integer_to_binary(?allow_mask, 16))/binary>>,
    <<"identifier">> => ?everyone,
    <<"aceflags">> => <<"0x", (integer_to_binary(?no_flags_mask, 16))/binary>>,
    <<"acemask">> => <<"0x", (integer_to_binary(
        ?read_attributes_mask bor ?read_metadata_mask bor ?read_acl_mask,
        16
    ))/binary>>
}]).

-define(CDMI_XATTRS_KEY, [
    ?ACL_KEY,
    ?MIMETYPE_KEY,
    ?TRANSFER_ENCODING_KEY,
    ?CDMI_COMPLETION_STATUS_KEY
]).
-define(ONEDATA_XATTRS_KEY, [
    ?JSON_METADATA_KEY,
    ?RDF_METADATA_KEY
]).
-define(ALL_XATTRS_KEYS, [
    ?ACL_KEY,
    ?MIMETYPE_KEY,
    ?TRANSFER_ENCODING_KEY,
    ?CDMI_COMPLETION_STATUS_KEY,
    ?JSON_METADATA_KEY,
    ?RDF_METADATA_KEY,
    ?XATTR_KEY
]).


-define(ALL_METADATA_SET_1, #{
    ?ACL_KEY => ?ACL_1,
    ?MIMETYPE_KEY => ?MIMETYPE_1,
    ?TRANSFER_ENCODING_KEY => ?TRANSFER_ENCODING_1,
    ?CDMI_COMPLETION_STATUS_KEY => ?CDMI_COMPLETION_STATUS_1,
    ?JSON_METADATA_KEY => ?JSON_METADATA_4,
    ?RDF_METADATA_KEY => ?RDF_METADATA_1,
    ?XATTR_KEY => ?XATTR_1_VALUE
}).
-define(ALL_METADATA_SET_2, #{
    ?ACL_KEY => ?ACL_2,
    ?MIMETYPE_KEY => ?MIMETYPE_2,
    ?TRANSFER_ENCODING_KEY => ?TRANSFER_ENCODING_2,
    ?CDMI_COMPLETION_STATUS_KEY => ?CDMI_COMPLETION_STATUS_2,
    ?JSON_METADATA_KEY => ?JSON_METADATA_5,
    ?RDF_METADATA_KEY => ?RDF_METADATA_2,
    ?XATTR_KEY => ?XATTR_2_VALUE
}).


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

    DirWithoutRdfMetadataPath = filename:join([RootDirPath, <<"dir_without_rdf_metadata">>]),
    {ok, DirWithoutRdfMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithoutRdfMetadataPath, 8#777),

    DirWithRdfMetadataPath = filename:join([RootDirPath, <<"dir_with_rdf_metadata">>]),
    {ok, DirWithRdfMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithRdfMetadataPath, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, DirWithRdfMetadataGuid}, rdf, ?RDF_METADATA_1, []),

    RegularFileWithoutRdfMetadataPath = filename:join([RootDirPath, <<"file_without_rdf_metadata">>]),
    {ok, RegularFileWithoutRdfMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithoutRdfMetadataPath, 8#777),

    RegularFileWithRdfMetadataPath = filename:join([RootDirPath, <<"file_with_rdf_metadata">>]),
    {ok, RegularFileWithRdfMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithRdfMetadataPath, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, RegularFileWithRdfMetadataGuid}, rdf, ?RDF_METADATA_1, []),

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
        ?assertEqual(?RDF_METADATA_1, Response)
    end,
    ValidateSuccessfulGsResultFun = fun(_, {ok, Result}) ->
        ?assertEqual(#{<<"metadata">> => ?RDF_METADATA_1}, Result)
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
                name = <<"Get rdf metadata from shared ", FileType/binary, " with rdf set using gs public api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, public),
                validate_result_fun = ValidateSuccessfulGsResultFun
            },
            #scenario_spec{
                name = <<"Get rdf metadata from shared ", FileType/binary, " with rdf set using private gs api">>,
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
            ?assertEqual(?RDF_METADATA_1, Response)
    end,
    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_1),
    lfm_proxy:set_metadata(Provider1, ?ROOT_SESS_ID, {guid, Space1Guid}, rdf, ?RDF_METADATA_1, []),
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
                    ?assertEqual(#{<<"metadata">> => ?RDF_METADATA_1}, Result)
            end
        }
    ])).


get_json_metadata_test(Config) ->
    [Provider2, Provider1] = Providers = ?config(op_worker_nodes, Config),
    Provider2DomainBin = ?GET_DOMAIN_BIN(Provider2),

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    UserSessId = GetSessionFun(Provider2),

    RootDirPath = filename:join(["/", ?SPACE_2, ?SCENARIO_NAME]),
    {ok, RootDirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, RootDirPath, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, RootDirGuid}, json, ?JSON_METADATA_1, []),

    DirLayer2Path = filename:join([RootDirPath, <<"dir_layer_2">>]),
    {ok, DirLayer2Guid} = lfm_proxy:mkdir(Provider2, UserSessId, DirLayer2Path, 8#717),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, DirLayer2Guid}, json, ?JSON_METADATA_2, []),

    DirLayer3Path = filename:join([DirLayer2Path, <<"dir_layer_3">>]),
    {ok, DirLayer3Guid} = lfm_proxy:mkdir(Provider2, UserSessId, DirLayer3Path, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, DirLayer3Guid}, json, ?JSON_METADATA_3, []),

    DirLayer4Path = filename:join([DirLayer3Path, <<"dir_layer_4">>]),
    {ok, DirLayer4Guid} = lfm_proxy:mkdir(Provider2, UserSessId, DirLayer4Path, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, DirLayer4Guid}, json, ?JSON_METADATA_4, []),
    {ok, ShareId} = lfm_proxy:create_share(Provider2, UserSessId, {guid, DirLayer4Guid}, <<"share">>),

    DirWithoutJsonMetadataPath = filename:join([DirLayer4Path, <<"dir_without_json_metadata">>]),
    {ok, DirWithoutJsonMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithoutJsonMetadataPath, 8#777),

    DirWithJsonMetadataPath = filename:join([DirLayer4Path, <<"dir_with_json_metadata">>]),
    {ok, DirWithJsonMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithJsonMetadataPath, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, DirWithJsonMetadataGuid}, json, ?JSON_METADATA_5, []),

    RegularFileWithoutJsonMetadataPath = filename:join([DirLayer4Path, <<"file_without_json_metadata">>]),
    {ok, RegularFileWithoutJsonMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithoutJsonMetadataPath, 8#777),

    RegularFileWithJsonMetadataPath = filename:join([DirLayer4Path, <<"file_with_json_metadata">>]),
    {ok, RegularFileWithJsonMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithJsonMetadataPath, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, RegularFileWithJsonMetadataGuid}, json, ?JSON_METADATA_5, []),

    % Wait for metadata sync between providers
    ?assertMatch(
        {ok, #file_attr{}},
        lfm_proxy:stat(Provider1, GetSessionFun(Provider1), {guid, RegularFileWithJsonMetadataGuid}),
        ?ATTEMPTS
    ),

    SupportedClientsPerNode = #{
        Provider1 => [?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        Provider2 => [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH]
    },

    ClientSpecForGetJsonInSpace2Scenarios = #client_spec{
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
        optional = [<<"inherited">>, <<"filter_type">>, <<"filter">>],
        correct_values = #{
            <<"inherited">> => [true, false],
            <<"filter_type">> => [<<"keypath">>],
            <<"filter">> => [<<"attr3.attr32">>, <<"attr2.attr22.[2]">>]
        },
        bad_values = [
            {<<"inherited">>, -100, ?ERROR_BAD_VALUE_BOOLEAN(<<"inherited">>)},
            {<<"inherited">>, <<"dummy">>, ?ERROR_BAD_VALUE_BOOLEAN(<<"inherited">>)},
            {<<"filter_type">>, <<"dummy">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filter_type">>, [<<"keypath">>])}
        ]
    },

    ConstructPrepareRestArgsFun = fun(FileId) -> fun(#api_test_ctx{data = Data}) ->
        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                <<"data/", FileId/binary, "/metadata/json">>,
                maps:with(
                    [<<"inherited">>, <<"filter_type">>, <<"filter">>],
                    utils:ensure_defined(Data, undefined, #{})
                )
            )
        }
    end end,
    ConstructPrepareDeprecatedFilePathRestArgsFun = fun(FilePath) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = get,
                path = http_utils:append_url_parameters(
                    <<"metadata/json/", FilePath/binary>>,
                    maps:with(
                        [<<"inherited">>, <<"filter_type">>, <<"filter">>],
                        utils:ensure_defined(Data, undefined, #{})
                    )
                )
            }
        end
    end,
    ConstructPrepareDeprecatedFileIdRestArgsFun = fun(Fileid) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = get,
                path = http_utils:append_url_parameters(
                    <<"metadata-id/json/", Fileid/binary>>,
                    maps:with(
                        [<<"inherited">>, <<"filter_type">>, <<"filter">>],
                        utils:ensure_defined(Data, undefined, #{})
                    )
                )
            }
        end
    end,
    ConstructPrepareGsArgsFun = fun(FileId, Scope) -> fun(#api_test_ctx{data = Data}) ->
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = FileId, aspect = json_metadata, scope = Scope},
            data = Data
        }
    end end,

    GetExpectedResultFun = fun(#api_test_ctx{client = Client, data = Data}, ShareId, DirectMetadataSet) ->
        try
            IncludeInherited = maps:get(<<"inherited">>, Data, false),
            FilterType = maps:get(<<"filter_type">>, Data, undefined),
            Filter = maps:get(<<"filter">>, Data, undefined),

            FilterList = case {FilterType, Filter} of
                {undefined, _} ->
                    [];
                {<<"keypath">>, undefined} ->
                    throw(?ERROR_MISSING_REQUIRED_VALUE(<<"filter">>));
                {<<"keypath">>, _} ->
                    binary:split(Filter, <<".">>, [global])
            end,

            ExpJsonMetadata = case {DirectMetadataSet, IncludeInherited} of
                {true, true} ->
                    case ShareId of
                        undefined when Client == ?USER_IN_SPACE_2_AUTH ->
                            % User belonging to the same space as owner of files
                            % shouldn't be able to get inherited metadata due to
                            % insufficient perms on DirLayer2
                            throw(?ERROR_POSIX(?EACCES));
                        undefined ->
                            json_metadata:merge([
                                ?JSON_METADATA_1,
                                ?JSON_METADATA_2,
                                ?JSON_METADATA_3,
                                ?JSON_METADATA_4,
                                ?JSON_METADATA_5
                            ]);
                        _ ->
                            json_metadata:merge([
                                ?JSON_METADATA_4,
                                ?JSON_METADATA_5
                            ])
                    end;
                {true, false} ->
                    ?JSON_METADATA_5;
                {false, true} ->
                    case ShareId of
                        undefined when Client == ?USER_IN_SPACE_2_AUTH ->
                            % User belonging to the same space as owner of files
                            % shouldn't be able to get inherited metadata due to
                            % insufficient perms on DirLayer2
                            throw(?ERROR_POSIX(?EACCES));
                        undefined ->
                            json_metadata:merge([
                                ?JSON_METADATA_1,
                                ?JSON_METADATA_2,
                                ?JSON_METADATA_3,
                                ?JSON_METADATA_4
                            ]);
                        _ ->
                            ?JSON_METADATA_4
                    end;
                {false, false} ->
                    throw(?ERROR_POSIX(?ENODATA))
            end,

            try
                {ok, json_metadata:find(ExpJsonMetadata, FilterList)}
            catch throw:{error, ?ENOATTR} ->
                ?ERROR_POSIX(?ENODATA)
            end
        catch throw:Error ->
            Error
        end
    end,

    ConstructValidateSuccessfulRestResultFun = fun(ShareId, DirectMetadataSet) ->
        fun(TestCtx, {ok, RespCode, RespBody}) ->
            {ExpCode, ExpBody} = case GetExpectedResultFun(TestCtx, ShareId, DirectMetadataSet) of
                {ok, ExpResult} ->
                    {?HTTP_200_OK, ExpResult};
                {error, _} = ExpError ->
                    {errors:to_http_code(ExpError), ?REST_ERROR(ExpError)}
            end,
            ?assertEqual({ExpCode, ExpBody}, {RespCode, RespBody})
        end
    end,
    ConstructValidateSuccessfulGsResultFun = fun(ShareId, DirectMetadataSet) ->
        fun(TestCtx, Result) ->
            case GetExpectedResultFun(TestCtx, ShareId, DirectMetadataSet) of
                {ok, ExpResult} ->
                    ?assertEqual({ok, #{<<"metadata">> => ExpResult}}, Result);
                {error, _} = ExpError ->
                    ?assertEqual(ExpError, Result)
            end
        end
    end,

    lists:foreach(fun({
        FileType,
        FileWithoutJsonMetadataPath, FileWithoutJsonMetadataGuid,
        FileWithJsonMetadataPath, FileWithJsonMetadataGuid
    }) ->
        {ok, FileWithoutJsonMetadataObjectId} = file_id:guid_to_objectid(FileWithoutJsonMetadataGuid),
        {ok, FileWithJsonMetadataObjectId} = file_id:guid_to_objectid(FileWithJsonMetadataGuid),

        ShareGuid = file_id:guid_to_share_guid(FileWithJsonMetadataGuid, ShareId),
        {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

        ?assert(api_test_utils:run_scenarios(Config, [

            %% TEST GET JSON METADATA FOR FILE WITH Json METADATA SET

            #scenario_spec{
                name = <<"Get json metadata from ", FileType/binary, " with json set using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(FileWithJsonMetadataObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get json metadata from ", FileType/binary, " with json set using /files/ rest endpoint">>,
                type = rest_with_file_path,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(FileWithJsonMetadataPath),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get json metadata from ", FileType/binary, " with json set using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(FileWithJsonMetadataObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get json metadata from ", FileType/binary, " with json set using gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(FileWithJsonMetadataGuid, private),
                validate_result_fun = ConstructValidateSuccessfulGsResultFun(undefined, true),
                data_spec = DataSpec
            },

            %% TEST GET JSON METADATA FOR SHARED FILE WITH JSON METADATA SET

            #scenario_spec{
                name = <<"Get json metadata from shared ", FileType/binary, " with json set using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(ShareObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(ShareId, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get json metadata from shared ", FileType/binary, " with json set using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(ShareObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(ShareId, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get json metadata from shared ", FileType/binary, " with json set using gs public api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, public),
                validate_result_fun = ConstructValidateSuccessfulGsResultFun(ShareId, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get json metadata from shared ", FileType/binary, " with json set using private gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, private),
                validate_result_fun = fun(_, Result) ->
                    ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                end,
                data_spec = DataSpec
            },

            %% TEST GET JSON METADATA FOR FILE WITHOUT JSON METADATA SET

            #scenario_spec{
                name = <<"Get json metadata from ", FileType/binary, " without json set using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(FileWithoutJsonMetadataObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, false),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get json metadata from ", FileType/binary, " without json set using /files/ rest endpoint">>,
                type = rest_with_file_path,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(FileWithoutJsonMetadataPath),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, false),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get json metadata from ", FileType/binary, " without json set using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(FileWithoutJsonMetadataObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, false),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get json metadata from ", FileType/binary, " without json set using gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(FileWithoutJsonMetadataGuid, private),
                validate_result_fun = ConstructValidateSuccessfulGsResultFun(undefined, false),
                data_spec = DataSpec
            }
        ]))
    end, [
        {
            <<"dir">>,
            DirWithoutJsonMetadataPath, DirWithoutJsonMetadataGuid,
            DirWithJsonMetadataPath, DirWithJsonMetadataGuid
        },
        {
            <<"file">>,
            RegularFileWithoutJsonMetadataPath, RegularFileWithoutJsonMetadataGuid,
            RegularFileWithJsonMetadataPath, RegularFileWithJsonMetadataGuid
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
            ?assertEqual(?JSON_METADATA_1, Response)
    end,
    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_1),
    lfm_proxy:set_metadata(Provider1, ?ROOT_SESS_ID, {guid, Space1Guid}, json, ?JSON_METADATA_1, []),
    {ok, Space1ObjectId} = file_id:guid_to_objectid(Space1Guid),

    ?assert(api_test_utils:run_scenarios(Config, [
        #scenario_spec{
            name = <<"Get json metadata from ", ?SPACE_1/binary, " with json set on provider not supporting user using /data/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForGetRdfInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareRestArgsFun(Space1ObjectId),
            validate_result_fun = ValidateRestGetRdfOnProvidersNotSupportingUserFun
        },
        #scenario_spec{
            name = <<"Get json metadata from ", ?SPACE_1/binary, " with json set on provider not supporting user using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = Providers,
            client_spec = ClientSpecForGetRdfInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(<<"/", ?SPACE_1/binary>>),
            validate_result_fun = ValidateRestGetRdfOnProvidersNotSupportingUserFun
        },
        #scenario_spec{
            name = <<"Get json metadata from ", ?SPACE_1/binary, " with json set on provider not supporting user using /files-id/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForGetRdfInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(Space1ObjectId),
            validate_result_fun = ValidateRestGetRdfOnProvidersNotSupportingUserFun
        },
        #scenario_spec{
            name = <<"Get json metadata from ", ?SPACE_1/binary, " with json set on provider not supporting user using gs api">>,
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
                    ?assertEqual(#{<<"metadata">> => ?JSON_METADATA_1}, Result)
            end
        }
    ])).


get_xattr_metadata_test(Config) ->
    [Provider2, Provider1] = Providers = ?config(op_worker_nodes, Config),

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    UserSessId = GetSessionFun(Provider2),

    Dir1Path = filename:join(["/", ?SPACE_2, ?SCENARIO_NAME]),
    {ok, Dir1Guid} = lfm_proxy:mkdir(Provider2, UserSessId, Dir1Path, 8#777),
    set_all_metadata_types(Provider2, UserSessId, Dir1Guid, set_1),

    Dir2Path = filename:join([Dir1Path, <<"dir_layer_4">>]),
    {ok, Dir2Guid} = lfm_proxy:mkdir(Provider2, UserSessId, Dir2Path, 8#717),
    {ok, ShareId} = lfm_proxy:create_share(Provider2, UserSessId, {guid, Dir2Guid}, <<"share">>),

    DirWithoutMetadataPath = filename:join([Dir2Path, <<"dir_without_metadata">>]),
    {ok, DirWithoutMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithoutMetadataPath, 8#777),

    DirWithMetadataPath = filename:join([Dir2Path, <<"dir_with_metadata">>]),
    {ok, DirWithMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithMetadataPath, 8#777),
    set_all_metadata_types(Provider2, UserSessId, DirWithMetadataGuid, set_2),

    RegularFileWithoutMetadataPath = filename:join([Dir2Path, <<"file_without_metadata">>]),
    {ok, RegularFileWithoutMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithoutMetadataPath, 8#777),

    RegularFileWithMetadataPath = filename:join([Dir2Path, <<"file_with_metadata">>]),
    {ok, RegularFileWithMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithMetadataPath, 8#777),
    set_all_metadata_types(Provider2, UserSessId, RegularFileWithMetadataGuid, set_2),

    % Wait for metadata sync between providers
    ?assertMatch(
        {ok, #file_attr{}},
        lfm_proxy:stat(Provider1, GetSessionFun(Provider1), {guid, RegularFileWithMetadataGuid}),
        ?ATTEMPTS
    ),

    SupportedClientsPerNode = #{
        Provider1 => [?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        Provider2 => [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH]
    },

    ClientSpecForGetJsonInSpace2Scenarios = #client_spec{
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

    NotSetXattrKey = <<"not_set_xattr">>,

    BadValues = [
        {<<"inherited">>, -100, ?ERROR_BAD_VALUE_BOOLEAN(<<"inherited">>)},
        {<<"inherited">>, <<"dummy">>, ?ERROR_BAD_VALUE_BOOLEAN(<<"inherited">>)},
        {<<"show_internal">>, -100, ?ERROR_BAD_VALUE_BOOLEAN(<<"show_internal">>)},
        {<<"show_internal">>, <<"dummy">>, ?ERROR_BAD_VALUE_BOOLEAN(<<"show_internal">>)}
    ],
    BadValuesWithForbiddenAttributes = [
        % Xattr name with prefixes 'cdmi_' and 'onedata_' should be forbidden
        % with exception of those listed in correct_values
        {<<"attribute">>, <<"cdmi_attr">>, ?ERROR_POSIX(?EPERM)},
        {<<"attribute">>, <<"onedata_attr">>, ?ERROR_POSIX(?EPERM)}
        | BadValues
    ],
    DataSpec = #data_spec{
        optional = [<<"attribute">>, <<"inherited">>, <<"show_internal">>],
        correct_values = #{
            <<"attribute">> => [NotSetXattrKey | ?ALL_XATTRS_KEYS],
            <<"inherited">> => [true, false],
            <<"show_internal">> => [true, false]
        },
        bad_values = BadValuesWithForbiddenAttributes
    },

    ConstructPrepareRestArgsFun = fun(FileId) -> fun(#api_test_ctx{data = Data}) ->
        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                <<"data/", FileId/binary, "/metadata/xattrs">>,
                maps:with(
                    [<<"inherited">>, <<"attribute">>, <<"show_internal">>],
                    utils:ensure_defined(Data, undefined, #{})
                )
            )
        }
    end end,
    ConstructPrepareDeprecatedFilePathRestArgsFun = fun(FilePath) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = get,
                path = http_utils:append_url_parameters(
                    <<"metadata/xattrs/", FilePath/binary>>,
                    maps:with(
                        [<<"inherited">>, <<"attribute">>, <<"show_internal">>],
                        utils:ensure_defined(Data, undefined, #{})
                    )
                )
            }
        end
    end,
    ConstructPrepareDeprecatedFileIdRestArgsFun = fun(Fileid) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = get,
                path = http_utils:append_url_parameters(
                    <<"metadata-id/xattrs/", Fileid/binary>>,
                    maps:with(
                        [<<"inherited">>, <<"attribute">>, <<"show_internal">>],
                        utils:ensure_defined(Data, undefined, #{})
                    )
                )
            }
        end
    end,
    ConstructPrepareGsArgsFun = fun(FileId, Scope) -> fun(#api_test_ctx{data = Data}) ->
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = FileId, aspect = xattrs, scope = Scope},
            data = Data
        }
    end end,

    GetExpectedResultFun = fun(#api_test_ctx{client = Client, data = Data}, ShareId, DirectMetadataSet) ->
        try
            Attribute = maps:get(<<"attribute">>, Data, undefined),
            IncludeInherited = maps:get(<<"inherited">>, Data, false),
            ShowInternal = maps:get(<<"show_internal">>, Data, false),

            XattrsToGet = case Attribute of
                undefined ->
                    case {DirectMetadataSet, IncludeInherited, ShareId, ShowInternal} of
                        {true, _, _, true} ->
                            ?ALL_XATTRS_KEYS;
                        {true, _, _, false} ->
                            [?XATTR_KEY];
                        {false, false, _, _} ->
                            [];
                        {false, true, undefined, true} ->
                            % Exclude cdmi attrs as those are not inherited
                            ?ALL_XATTRS_KEYS -- ?CDMI_XATTRS_KEY;
                        {false, true, undefined, false} ->
                            [?XATTR_KEY];
                        {false, true, _, _} ->
                            % No xattr could be inherited due to share root blocking further traverse
                            []
                    end;
                _ ->
                    [Attribute]
            end,

            AvailableXattrsMap = case {DirectMetadataSet, IncludeInherited} of
                {true, false} ->
                    ?ALL_METADATA_SET_2;
                {true, true} ->
                    case ShareId of
                        undefined when Client == ?USER_IN_SPACE_2_AUTH ->
                            % User belonging to the same space as owner of files
                            % shouldn't be able to get inherited json metadata or not set xattr
                            % due to insufficient perms on Dir1. But can get all other xattrs
                            % as the first found value is returned and ancestors aren't
                            % traversed further (json metadata is exceptional since it
                            % collects all ancestors jsons and merges them)
                            case lists:member(?JSON_METADATA_KEY, XattrsToGet) orelse lists:member(NotSetXattrKey, XattrsToGet) of
                                true ->
                                    throw(?ERROR_POSIX(?EACCES));
                                false ->
                                    ?ALL_METADATA_SET_2
                            end;
                        undefined ->
                            % When 'inherited' flag is set all ancestors json metadata
                            % are merged but for rest the first value found (which in
                            % this case is value directly set on file) is returned
                            ?ALL_METADATA_SET_2#{
                                ?JSON_METADATA_KEY => json_metadata:merge([
                                    ?JSON_METADATA_4,
                                    ?JSON_METADATA_5
                                ])
                            };
                        _ ->
                            % In share mode only metadata directly set on file is available
                            ?ALL_METADATA_SET_2
                    end;
                {false, false} ->
                    #{};
                {false, true} ->
                    case ShareId of
                        undefined when Client == ?USER_IN_SPACE_2_AUTH ->
                            case lists:any(fun(Xattr) -> lists:member(Xattr, ?CDMI_XATTRS_KEY) end, XattrsToGet) of
                                true ->
                                    % Cdmi attrs cannot be inherited, so trying to get them when
                                    % they are not directly set result in ?ENODATA no matter the
                                    % value of 'inherited' flag
                                    throw(?ERROR_POSIX(?ENODATA));
                                false ->
                                    % User belonging to the same space as owner of files
                                    % shouldn't be able to get any inherited metadata due to
                                    % insufficient perms on Dir1
                                    throw(?ERROR_POSIX(?EACCES))
                            end;
                        undefined ->
                            % User should fetch all metadata set on ancestor dirs
                            #{
                                ?JSON_METADATA_KEY => ?JSON_METADATA_4,
                                ?RDF_METADATA_KEY => ?RDF_METADATA_1,
                                ?XATTR_KEY => ?XATTR_1_VALUE
                            };
                        _ ->
                            #{}
                    end
            end,

            case XattrsToGet -- maps:keys(AvailableXattrsMap) of
                [] ->
                    {ok, maps:with(XattrsToGet, AvailableXattrsMap)};
                _ ->
                    ?ERROR_POSIX(?ENODATA)
            end
        catch throw:Error ->
            Error
        end
    end,

    ConstructValidateSuccessfulRestResultFun = fun(ShareId, DirectMetadataSet) ->
        fun(TestCtx, {ok, RespCode, RespBody}) ->
            {ExpCode, ExpBody} = case GetExpectedResultFun(TestCtx, ShareId, DirectMetadataSet) of
                {ok, ExpResult} ->
                    {?HTTP_200_OK, ExpResult};
                {error, _} = ExpError ->
                    {errors:to_http_code(ExpError), ?REST_ERROR(ExpError)}
            end,
            ?assertEqual({ExpCode, ExpBody}, {RespCode, RespBody})
        end
    end,
    ConstructValidateSuccessfulGsResultFun = fun(ShareId, DirectMetadataSet) ->
        fun(TestCtx, Result) ->
            case GetExpectedResultFun(TestCtx, ShareId, DirectMetadataSet) of
                {ok, ExpResult} ->
                    ?assertEqual({ok, #{<<"metadata">> => ExpResult}}, Result);
                {error, _} = ExpError ->
                    ?assertEqual(ExpError, Result)
            end
        end
    end,

    lists:foreach(fun({
        FileType,
        FileWithoutJsonMetadataPath, FileWithoutJsonMetadataGuid,
        FileWithJsonMetadataPath, FileWithJsonMetadataGuid
    }) ->
        {ok, FileWithoutJsonMetadataObjectId} = file_id:guid_to_objectid(FileWithoutJsonMetadataGuid),
        {ok, FileWithJsonMetadataObjectId} = file_id:guid_to_objectid(FileWithJsonMetadataGuid),

        ShareGuid = file_id:guid_to_share_guid(FileWithJsonMetadataGuid, ShareId),
        {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

        ?assert(api_test_utils:run_scenarios(Config, [

            %% TEST XATTR FOR FILE WITH METADATA SET

            #scenario_spec{
                name = <<"Get xattr from ", FileType/binary, " with xattr set using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(FileWithJsonMetadataObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get xattr from ", FileType/binary, " with xattr set using /files/ rest endpoint">>,
                type = rest_with_file_path,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(FileWithJsonMetadataPath),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get xattr from ", FileType/binary, " with xattr set using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(FileWithJsonMetadataObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get xattr from ", FileType/binary, " with xattr set using gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(FileWithJsonMetadataGuid, private),
                validate_result_fun = ConstructValidateSuccessfulGsResultFun(undefined, true),
                data_spec = DataSpec
            },

            %% TEST GET XATTR FOR SHARED FILE WITH METADATA SET

            #scenario_spec{
                name = <<"Get xattr from shared ", FileType/binary, " with xattr set using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(ShareObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(ShareId, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get xattr from shared ", FileType/binary, " with xattr set using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(ShareObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(ShareId, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get xattr from shared ", FileType/binary, " with xattr set using gs public api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, public),
                validate_result_fun = ConstructValidateSuccessfulGsResultFun(ShareId, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get xattr from shared ", FileType/binary, " with xattr set using private gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, private),
                validate_result_fun = fun(_, Result) ->
                    ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                end,
                data_spec = DataSpec#data_spec{
                    % Remove errors as request in this case is rejected much sooner
                    % before they can be thrown
                    bad_values = BadValues
                }
            },

            %% TEST GET XATTR FOR FILE WITHOUT METADATA SET

            #scenario_spec{
                name = <<"Get xattr from ", FileType/binary, " without xattr set using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(FileWithoutJsonMetadataObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, false),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get xattr from ", FileType/binary, " without xattr set using /files/ rest endpoint">>,
                type = rest_with_file_path,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(FileWithoutJsonMetadataPath),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, false),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get xattr from ", FileType/binary, " without xattr set using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(FileWithoutJsonMetadataObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, false),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get xattr from ", FileType/binary, " without xattr set using gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(FileWithoutJsonMetadataGuid, private),
                validate_result_fun = ConstructValidateSuccessfulGsResultFun(undefined, false),
                data_spec = DataSpec
            }
        ]))
    end, [
        {
            <<"dir">>,
            DirWithoutMetadataPath, DirWithoutMetadataGuid,
            DirWithMetadataPath, DirWithMetadataGuid
        },
        {
            <<"file">>,
            RegularFileWithoutMetadataPath, RegularFileWithoutMetadataGuid,
            RegularFileWithMetadataPath, RegularFileWithMetadataGuid
        }
    ]).


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
    ct:timetrap({minutes, 30}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


set_all_metadata_types(Node, SessionId, Guid, set_1) ->
    ?assertMatch(ok, lfm_proxy:set_metadata(Node, SessionId, {guid, Guid}, json, ?JSON_METADATA_4, [])),
    ?assertMatch(ok, lfm_proxy:set_metadata(Node, SessionId, {guid, Guid}, rdf, ?RDF_METADATA_1, [])),
    ?assertMatch(ok, lfm_proxy:set_mimetype(Node, SessionId, {guid, Guid}, ?MIMETYPE_1)),
    ?assertMatch(ok, lfm_proxy:set_transfer_encoding(Node, SessionId, {guid, Guid}, ?TRANSFER_ENCODING_1)),
    ?assertMatch(ok, lfm_proxy:set_cdmi_completion_status(Node, SessionId, {guid, Guid}, ?CDMI_COMPLETION_STATUS_1)),
    ?assertMatch(ok, lfm_proxy:set_xattr(Node, SessionId, {guid, Guid}, ?XATTR_1)),
    ?assertMatch(ok, lfm_proxy:set_acl(Node, SessionId, {guid, Guid}, acl:from_json(?ACL_1, cdmi)));
set_all_metadata_types(Node, SessionId, Guid, set_2) ->
    ?assertMatch(ok, lfm_proxy:set_metadata(Node, SessionId, {guid, Guid}, json, ?JSON_METADATA_5, [])),
    ?assertMatch(ok, lfm_proxy:set_metadata(Node, SessionId, {guid, Guid}, rdf, ?RDF_METADATA_2, [])),
    ?assertMatch(ok, lfm_proxy:set_mimetype(Node, SessionId, {guid, Guid}, ?MIMETYPE_2)),
    ?assertMatch(ok, lfm_proxy:set_transfer_encoding(Node, SessionId, {guid, Guid}, ?TRANSFER_ENCODING_2)),
    ?assertMatch(ok, lfm_proxy:set_cdmi_completion_status(Node, SessionId, {guid, Guid}, ?CDMI_COMPLETION_STATUS_2)),
    ?assertMatch(ok, lfm_proxy:set_xattr(Node, SessionId, {guid, Guid}, ?XATTR_2)),
    ?assertMatch(ok, lfm_proxy:set_acl(Node, SessionId, {guid, Guid}, acl:from_json(?ACL_2, cdmi))).
