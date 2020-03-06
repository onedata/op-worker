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
    [Provider2, Provider1] = Providers = ?config(op_worker_nodes, Config),
    Provider2DomainBin = ?GET_DOMAIN_BIN(Provider2),

    Space1 = <<"space1">>,
    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(Space1),
    {ok, Space1ObjectId} = file_id:guid_to_objectid(Space1Guid),

    Space2 = <<"space2">>,
    Space2Guid = fslogic_uuid:spaceid_to_space_dir_guid(Space2),

    Spaces = [
        {Space1Guid, Space1, <<"/", Space1/binary>>},
        {Space2Guid, Space2, <<"/", Space2/binary>>}
    ],

    UserInSpace1 = <<"user1">>,
    UserInSpace2 = <<"user3">>,
    UserInBothSpaces = <<"user2">>,

    GetSessionFun = fun(Node) ->
        ?config({session_id, {UserInBothSpaces, ?GET_DOMAIN(Node)}}, Config)
    end,

    UserInBothSpacesRootDirGuid = fslogic_uuid:user_root_dir_guid(UserInBothSpaces),
    {ok, UserInBothSpacesRootDirObjectId} = file_id:guid_to_objectid(UserInBothSpacesRootDirGuid),

    UserSessId = GetSessionFun(Provider2),

    DirName = ?SCENARIO_NAME,
    DirPath = filename:join(["/", Space2, DirName]),
    {ok, DirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirPath, 8#777),
    {ok, DirObjectId} = file_id:guid_to_objectid(DirGuid),

    {ok, ShareId} = lfm_proxy:create_share(Provider2, UserSessId, {guid, DirGuid}, <<"share">>),
    ShareDirGuid = file_id:guid_to_share_guid(DirGuid, ShareId),
    {ok, ShareDirObjectId} = file_id:guid_to_objectid(ShareDirGuid),

    Files = [{FileGuid1, _, FilePath1} | _] = lists:map(fun(Num) ->
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

    UserInSpace1Client = {user, UserInSpace1},
    UserInSpace2Client = {user, UserInSpace2},
    UserInBothSpacesClient = {user, UserInBothSpaces},

    SupportedClientsPerNode = #{
        Provider1 => [UserInSpace1Client, UserInSpace2Client, UserInBothSpacesClient],
        Provider2 => [UserInSpace2Client, UserInBothSpacesClient]
    },

    ClientSpecForSpace1Listing = #client_spec{
        correct = [UserInSpace1Client, UserInBothSpacesClient],
        unauthorized = [nobody],
        forbidden = [UserInSpace2Client],
        supported_clients_per_node = SupportedClientsPerNode
    },

    ClientSpecForSpace2Listing = #client_spec{
        correct = [UserInSpace2Client, UserInBothSpacesClient],
        unauthorized = [nobody],
        forbidden = [UserInSpace1Client],
        supported_clients_per_node = SupportedClientsPerNode
    },

    ClientSpecForUserInBothSpacesUserRootDirListing = #client_spec{
        correct = [UserInBothSpacesClient],
        unauthorized = [nobody],
        forbidden = [UserInSpace1Client, UserInSpace2Client],
        supported_clients_per_node = SupportedClientsPerNode
    },

    % Special case -> any user can make requests for shares but if request is
    % being made using credentials by user not supported on specific provider
    % ?ERROR_USER_NOT_SUPPORTED will be returned
    ClientSpecForShareListing = #client_spec{
        correct = [nobody, UserInSpace1Client, UserInSpace2Client, UserInBothSpacesClient],
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
            {<<"limit">>, -100, ?ERROR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},
            {<<"limit">>, 0, ?ERROR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},
            {<<"offset">>, <<"abc">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)}
        ]
    },

    ConstructPrepareRestArgsFun = fun(FileId) ->
        fun(_Env, Data) ->
            Qs = construct_list_files_qs(Data),
            #rest_args{
                method = get,
                path = <<"data/", FileId/binary, "/children", Qs/binary>>
            }
        end
    end,
    ConstructPrepareDeprecatedFilePathRestArgsFun = fun(FilePath) ->
        fun(_Env, Data) ->
            Qs = construct_list_files_qs(Data),
            #rest_args{
                method = get,
                path = <<"files", FilePath/binary, Qs/binary>>
            }
        end
    end,
    ConstructPrepareDeprecatedFileIdRestArgsFun = fun(Fileid) ->
        fun(_Env, Data) ->
            Qs = construct_list_files_qs(Data),
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

    ValidateRestListedFilesOnProvidersNotSupportingUser = fun(ExpSuccessResult) -> fun
        (Node, Client, {ok, ?HTTP_400_BAD_REQUEST, Response}, _Env, _Data) when
            Node == Provider2,
            Client == UserInBothSpacesClient
        ->
            assert_error_json(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin), Response);
        (_Node, _Client, {ok, ?HTTP_200_OK, Response}, _Env, _Data) ->
            ?assertEqual(ExpSuccessResult, Response)
    end end,

    ?assert(api_test_utils:run_scenarios(Config, [

        %% TEST LISTING NORMAL DIR

        #scenario_spec{
            name = <<"List normal dir using /data/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace2Listing,
            prepare_args_fun = ConstructPrepareRestArgsFun(DirObjectId),
            validate_result_fun = fun(_Node, _Client, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
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
            validate_result_fun = fun(_Node, _Client, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
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
            validate_result_fun = fun(_Node, _Client, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
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
            validate_result_fun = fun(_Node, _Client, {ok, Result}, _Env, Data) ->
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
            validate_result_fun = fun(_Node, _Client, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
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
            validate_result_fun = fun(_Node, _Client, {ok, ?HTTP_400_BAD_REQUEST, Response}, _Env, _Data) ->
                assert_error_json(?ERROR_NOT_SUPPORTED, Response)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List shared dir using gs api with public scope">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpecForShareListing,
            prepare_args_fun = ConstructPrepareGsArgsFun(ShareDirGuid, public),
            validate_result_fun = fun(_Node, _Client, {ok, Result}, _Env, Data) ->
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
            validate_result_fun = fun(_Node, _Client, Result, _Env, _Data) ->
                ?assertEqual(?ERROR_UNAUTHORIZED, Result)
            end,
            data_spec = ParamsSpec
        }

        %% TEST LISTING FILE

%%        % TODO fix
%%        #scenario_spec{
%%            name = <<"List file using /data/ rest endpoint">>,
%%            type = rest,
%%            target_nodes = Providers,
%%            client_spec = ClientSpecForSpace2Listing,
%%            prepare_args_fun = ConstructPrepareRestArgsFun(FileObjectId1),
%%            validate_result_fun = fun(_Node, _Client, {ok, ?HTTP_400_BAD_REQUEST, Response}, _Env, _Data) ->
%%                ExpError = #{<<"error">> => errors:to_json(?ERROR_POSIX(?ENOTDIR))},
%%                ?assertEqual(ExpError, Response)
%%            end,
%%            data_spec = ParamsSpec
%%        },
        #scenario_spec{
            name = <<"List file using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace2Listing,
            prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(FilePath1),
            validate_result_fun = fun(_Node, _Client, {ok, ?HTTP_200_OK, Response}, _Env, _Data) ->
                ?assertEqual([#{
                    <<"id">> => FileObjectId1,
                    <<"path">> => FilePath1
                }], Response)
            end,
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List file using /files-id/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace2Listing,
            prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(FileObjectId1),
            validate_result_fun = fun(_Node, _Client, {ok, ?HTTP_200_OK, Response}, _Env, _Data) ->
                ?assertEqual([#{
                    <<"id">> => FileObjectId1,
                    <<"path">> => FilePath1
                }], Response)
            end,
            data_spec = ParamsSpec
        },
%%        % TODO fix
%%        #scenario_spec{
%%            name = <<"List file using gs api">>,
%%            type = gs,
%%            target_nodes = Providers,
%%            client_spec = ClientSpecForSpace2Listing,
%%            prepare_args_fun = ConstructPrepareGsArgsFun(FileGuid1, private),
%%            validate_result_fun = fun(_Node, _Client, Result, _, _) ->
%%                ?assertEqual(?ERROR_POSIX(?ENOTDIR), Result)
%%            end,
%%            data_spec = ParamsSpec
%%        },

        % LISTING USER ROOT DIR SHOULD LIST ALL SPACES ALSO THOSE NOT SUPPORTED LOCALLY

        #scenario_spec{
            name = <<"List user root dir using /data/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForUserInBothSpacesUserRootDirListing,
            prepare_args_fun = ConstructPrepareRestArgsFun(UserInBothSpacesRootDirObjectId),
            validate_result_fun = fun(_Node, _Client, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
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
                correct = [UserInSpace1Client, UserInSpace2Client, UserInBothSpacesClient],
                unauthorized = [nobody],
                forbidden = [],
                supported_clients_per_node = SupportedClientsPerNode
            },
            prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(<<"/">>),
            validate_result_fun = fun(_Node, Client, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
                ClientSpaces = case Client of
                    UserInSpace1Client ->
                        [{Space1Guid, Space1, <<"/", Space1/binary>>}];
                    UserInSpace2Client ->
                        [{Space2Guid, Space2, <<"/", Space2/binary>>}];
                    UserInBothSpacesClient ->
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
            validate_result_fun = fun(_Node, _Client, {ok, ?HTTP_200_OK, Response}, _Env, Data) ->
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
            validate_result_fun = fun(_Node, _Client, {ok, Result}, _, Data) ->
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
            validate_result_fun = ValidateRestListedFilesOnProvidersNotSupportingUser(#{<<"children">> => []}),
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List dir on provider not supporting user using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace1Listing,
            prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(<<"/", Space1/binary>>),
            validate_result_fun = ValidateRestListedFilesOnProvidersNotSupportingUser([]),
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List dir on provider not supporting user using /files-id/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace1Listing,
            prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(Space1ObjectId),
            validate_result_fun = ValidateRestListedFilesOnProvidersNotSupportingUser([]),
            data_spec = ParamsSpec
        },
        #scenario_spec{
            name = <<"List dir on provider not supporting user using gs api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpecForSpace1Listing,
            prepare_args_fun = ConstructPrepareGsArgsFun(Space1Guid, private),
            validate_result_fun = fun
                (Node, Client, Result, _, _Data) when Node == Provider2 andalso Client == UserInBothSpacesClient ->
                    ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin), Result);
                (_Node, _Client, {ok, Result}, _, _Data) ->
                    ?assertEqual(#{<<"children">> => []}, Result)
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
-spec assert_error_json(errors:error(), Json :: map()) -> ok | no_return().
assert_error_json(ExpError, Json) ->
    ExpErrorJson = #{<<"error">> => errors:to_json(ExpError)},
    ?assertEqual(ExpErrorJson, Json).


%% @private
-spec construct_list_files_qs(Data :: map()) -> binary().
construct_list_files_qs(Data) ->
    QsParams = [QsParam || QsParam <- [
        construct_qs_param(<<"limit">>, Data),
        construct_qs_param(<<"offset">>, Data)
    ], QsParam /= <<>>],

    case str_utils:join_binary(QsParams, <<"&">>) of
        <<>> ->
            <<>>;
        Qs ->
            <<"?", Qs/binary>>
    end.


%% @private
-spec construct_qs_param(ParamName :: binary(), Data :: map()) -> binary().
construct_qs_param(ParamName, Data) ->
    case maps:get(ParamName, Data, undefined) of
        undefined ->
            <<>>;
        Value ->
            <<ParamName/binary, "=", (str_utils:to_binary(Value))/binary>>
    end.


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
