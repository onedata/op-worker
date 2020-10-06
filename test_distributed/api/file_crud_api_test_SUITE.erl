%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file crud API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(file_crud_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("file_api_test_utils.hrl").
-include("modules/fslogic/file_details.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    get_file_instance_test/1,
    get_shared_file_instance_test/1,
    get_file_instance_on_provider_not_supporting_space_test/1
]).

all() -> [
    get_file_instance_test,
    get_shared_file_instance_test,
    get_file_instance_on_provider_not_supporting_space_test
].


-define(ATTEMPTS, 30).


%%%===================================================================
%%% Get attrs test functions
%%%===================================================================


get_file_instance_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P1Node, P2Node],

    {FileType, _FilePath, FileGuid, #file_details{
        file_attr = #file_attr{
            guid = FileGuid
        }
    } = FileDetails} = api_test_utils:create_file_in_space2_with_additional_metadata(
        <<"/", ?SPACE_2/binary>>, false, ?RANDOM_FILE_NAME(), Config
    ),
    JsonFileDetails = file_details_to_gs_json(undefined, FileDetails),

    SpaceId = api_test_env:get_space_id(space2, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    SpaceDetails = get_space_dir_details(P2Node, SpaceGuid, ?SPACE_2),
    JsonSpaceDetails = file_details_to_gs_json(undefined, SpaceDetails),

    ClientSpec = #client_spec{
        correct = [
            user2,  % space owner - doesn't need any perms
            user3,  % files owner
            user4   % space member - should succeed as getting attrs doesn't require any perms
                    % TODO VFS-6766 revoke ?SPACE_VIEW priv and see that list of shares is empty
        ],
        unauthorized = [nobody],
        forbidden_not_in_space = [user1]
    },

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = str_utils:format("Get instance for ~s using gs private api", [FileType]),
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpec,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(FileGuid, private),
            validate_result_fun = build_get_instance_validate_gs_call_fun(JsonFileDetails),
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, undefined, undefined
            )
        },
        #scenario_spec{
            name = str_utils:format("Get instance for ~s using gs public api", [FileType]),
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(FileGuid, public),
            validate_result_fun = fun(#api_test_ctx{client = Client}, Result) ->
                case Client of
                    ?NOBODY -> ?assertEqual(?ERROR_UNAUTHORIZED, Result);
                    _ -> ?assertEqual(?ERROR_FORBIDDEN, Result)
                end
            end
        },
        #scenario_spec{
            name = str_utils:format("Get instance for ?SPACE_2 using gs private api"),
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpec,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(SpaceGuid, private),
            validate_result_fun = build_get_instance_validate_gs_call_fun(JsonSpaceDetails)
        }
    ])).


get_shared_file_instance_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    Providers = [P1Node, P2Node],

    {FileType, _FilePath, FileGuid, #file_details{
        file_attr = FileAttr = #file_attr{
            guid = FileGuid,
            shares = OriginalShares
        }
    } = FileDetails} = api_test_utils:create_file_in_space2_with_additional_metadata(
        <<"/", ?SPACE_2/binary>>, false, ?RANDOM_FILE_NAME(), Config
    ),

    SpaceOwnerSessId = api_test_env:get_user_session_id(user2, p1, Config),
    ShareId1 = api_test_utils:share_file_and_sync_file_attrs(P1Node, SpaceOwnerSessId, Providers, FileGuid),
    ShareId2 = api_test_utils:share_file_and_sync_file_attrs(P1Node, SpaceOwnerSessId, Providers, FileGuid),

    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId1),

    JsonFileDetails = file_details_to_gs_json(ShareId1, FileDetails#file_details{
        file_attr = FileAttr#file_attr{shares = [ShareId2, ShareId1 | OriginalShares]}
    }),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = str_utils:format("Get instance for shared ~s using gs public api", [FileType]),
            type = gs,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(ShareFileGuid, public),
            validate_result_fun = build_get_instance_validate_gs_call_fun(JsonFileDetails),
            data_spec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
                FileGuid, ShareId1, undefined
            )
        },
        #scenario_spec{
            name = str_utils:format("Get instance for shared ~s using gs private api", [FileType]),
            type = gs_with_shared_guid_and_aspect_private,
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC_FOR_SHARES,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(ShareFileGuid, private),
            validate_result_fun = fun(_, Result) ->
                ?assertEqual(?ERROR_UNAUTHORIZED, Result)
            end,
            data_spec = undefined
        }
    ])).


get_file_instance_on_provider_not_supporting_space_test(Config) ->
    P2Id = api_test_env:get_provider_id(p2, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),
    {FileType, _FilePath, FileGuid, _ShareId} = api_test_utils:create_shared_file_in_space1(Config),

    ValidateGsCallResultFun = fun(_, Result) ->
        ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(P2Id), Result)
    end,

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #scenario_spec{
            name = str_utils:format("Get instance for ~s on provider not supporting user using gs api", [
                FileType
            ]),
            type = gs,
            target_nodes = [P2Node],
            client_spec = ?CLIENT_SPEC_FOR_SPACE_1,
            prepare_args_fun = build_get_instance_prepare_gs_args_fun(FileGuid, private),
            validate_result_fun = ValidateGsCallResultFun,
            data_spec = undefined
        }
    ])).


%% @private
-spec file_details_to_gs_json(undefined | od_share:id(), #file_details{}) -> map().
file_details_to_gs_json(ShareId, #file_details{file_attr = #file_attr{
    guid = FileGuid
}} = FileDetails) ->

    JsonFileDetails = api_test_utils:file_details_to_gs_json(ShareId, FileDetails),
    JsonFileDetails#{
        <<"gri">> => gri:serialize(#gri{
            type = op_file,
            id = file_id:guid_to_share_guid(FileGuid, ShareId),
            aspect = instance,
            scope = case ShareId of
                undefined -> private;
                _ -> public
            end
        }),
        <<"revision">> => 1
    }.


%% @private
-spec build_get_instance_prepare_gs_args_fun(file_id:file_guid(), gri:scope()) ->
    onenv_api_test_runner:prepare_args_fun().
build_get_instance_prepare_gs_args_fun(FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data}) ->
        {GriId, _} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data),

        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = instance, scope = Scope}
        }
    end.


%% @private
-spec build_get_instance_validate_gs_call_fun(ExpJsonDetails :: map()) ->
    onenv_api_test_runner:validate_call_result_fun().
build_get_instance_validate_gs_call_fun(ExpJsonDetails) ->
    fun(_TestCtx, Result) ->
        ?assertEqual({ok, ExpJsonDetails}, Result)
    end.


%% @private
-spec get_space_dir_details(node(), file_id:file_guid(), od_space:name()) -> #file_details{}.
get_space_dir_details(Node, SpaceDirGuid, SpaceName) ->
    {ok, SpaceAttrs} = api_test_utils:get_file_attrs(Node, SpaceDirGuid),

    #file_details{
        file_attr = SpaceAttrs#file_attr{name = SpaceName},
        index_startid = file_id:guid_to_space_id(SpaceDirGuid),
        active_permissions_type = posix,
        has_metadata = false,
        has_direct_qos = false,
        has_eff_qos = false
    }.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    api_test_env:init_per_suite(Config, #onenv_test_config{envs = [
        {op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}
    ]}).


end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
