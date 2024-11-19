%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning handle basic API (gs).
%%% @end
%%%-------------------------------------------------------------------
-module(api_handle_test_SUITE).
-author("Katarzyna Such").

-include("api_test_runner.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
%%-include_lib("ctool/include/privileges.hrl").
-include("modules/logical_file_manager/lfm.hrl").


-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_handle_test/1,
    get_handle_test/1,
    update_handle_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_handle_test,
        get_handle_test,
        update_handle_test
    ]}
].

all() -> [
    {group, all_tests}
].

-define(CLIENT_SPEC, #client_spec{
    correct = [user1]
}).

-define(METADATA_STRING, <<
    "<?xml version=\"1.0\" encoding=\"utf-8\"?><metadata>",
    "    <dc:contributor>John Doe</dc:contributor>",
    "</metadata>"
>>).

-define(METADATA_PREFIX, <<"oai_dc">>).


%%%===================================================================
%%% Get file distribution test functions
%%%===================================================================


create_handle_test(_Config) ->
    Providers = [krakow],
    HServiceId = hd(ozw_test_rpc:list_handle_services()),

    MemRef = api_test_memory:init(),
    SetupFun = create_share_setup_fun(MemRef),
    TearDownFun = rm_share_teardown_fun(MemRef),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC,
            setup_fun = SetupFun,
            teardown_fun = TearDownFun,
            scenario_templates = [
                #scenario_template{
                    name = <<"Create handle for share using gs api">>,
                    type = gs,
                    prepare_args_fun = create_handle_prepare_gs_args_fun(MemRef),
                    validate_result_fun = fun(_, Result) ->
                        ?assertMatch({ok, _}, Result)
                    end
                }
            ],
            data_spec = #data_spec{
                required = [
                    <<"shareId">>, <<"handleServiceId">>,
                    <<"metadataPrefix">>, <<"metadataString">>
                ],
                correct_values = #{
                    <<"shareId">> => [shareId],
                    <<"handleServiceId">> => [HServiceId],
                    <<"metadataPrefix">> => [?METADATA_PREFIX],
                    <<"metadataString">> => [?METADATA_STRING]
                }
            }
        }
    ])).


%% @private
-spec create_handle_prepare_gs_args_fun(onenv_api_test_runner:api_test_ctx()) ->
    onenv_api_test_runner:gs_args().
create_handle_prepare_gs_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        ShareId = api_test_memory:get(MemRef, share_id),
        #gs_args{
            operation = create,
            gri = #gri{type = op_handle, aspect = instance, scope = private},
            data = maybe_inject_share_id(Data, ShareId)
        }
    end.


%% @private
-spec create_share_setup_fun(api_test_memory:mem_ref()) -> onenv_api_test_runner:setup_fun().
create_share_setup_fun(MemRef) ->
    fun() ->
        {_, FileSpec} = generate_random_file_spec([#share_spec{}]),
        Object = #object{guid = _, shares = [ShareId]} = onenv_file_test_utils:create_and_sync_file_tree(
            user1, space_krk, FileSpec
        ),
        api_test_memory:set(MemRef, file_tree_object, Object),
        api_test_memory:set(MemRef, share_id, ShareId)
    end.


%% @private
-spec rm_share_teardown_fun(api_test_memory:mem_ref()) -> onenv_api_test_runner:setup_fun().
rm_share_teardown_fun(MemRef) ->
    fun() ->
        #object{guid = Guid} = api_test_memory:get(MemRef, file_tree_object),
        onenv_file_test_utils:rm_and_sync_file(user1, Guid)
    end.


get_handle_test(_Config) ->
    Providers = [krakow],
    HServiceId = hd(ozw_test_rpc:list_handle_services()),
    {_, FileSpec} = generate_random_file_spec([#share_spec{}]),
    #object{guid = _, shares = [ShareId]} = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space_krk, FileSpec
    ),
    HandleId = create_handle(ShareId, HServiceId),

    ValidateResultFun = fun(_, {ok, #{
        <<"handleService">> := HServiceResult,
        <<"metadataPrefix">> := MetadataPrefixResult,
        <<"metadataString">> := MetadataStringResult,
        <<"url">> := PublicHandle
    }}) ->
        ExpectedHService = <<"op_handle_service.", HServiceId/binary, ".instance:public">>,
        ExpectedMetadataString = <<
            "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n",
            "<metadata>\n",
            "    <dc:identifier>", PublicHandle/binary, "</dc:identifier>"
            "    <dc:contributor>John Doe</dc:contributor>",
            "</metadata>"
        >>,
        ?assertEqual(
            {ExpectedHService, ExpectedMetadataString, ?METADATA_PREFIX},
            {HServiceResult, MetadataStringResult, MetadataPrefixResult}
        )
    end,
    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get handle using gs api">>,
                    type = gs,
                    prepare_args_fun = get_handle_prepare_gs_args_fun(HandleId),
                    validate_result_fun = ValidateResultFun
                }
            ]
        }
    ])).


%% @private
-spec get_handle_prepare_gs_args_fun(onenv_api_test_runner:api_test_ctx()) ->
    onenv_api_test_runner:gs_args().
get_handle_prepare_gs_args_fun(HandleId) ->
    fun(#api_test_ctx{data = Data}) ->
        #gs_args{
            operation = get,
            gri = #gri{type = op_handle, id = HandleId, aspect = instance, scope = private},
            data = Data
        }
    end.


update_handle_test(_Config) ->
    Providers = [krakow],
    HServiceId = hd(ozw_test_rpc:list_handle_services()),
    {_, FileSpec} = generate_random_file_spec([#share_spec{}]),
    #object{guid = FileGuid, shares = [ShareId]} = onenv_file_test_utils:create_and_sync_file_tree(
        user1, space_krk, FileSpec
    ),
    HandleId = create_handle(ShareId, HServiceId),

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = Providers,
            client_spec = ?CLIENT_SPEC,
            scenario_templates = [
                #scenario_template{
                    name = <<"Update handle using gs api">>,
                    type = gs,
                    prepare_args_fun = update_handle_prepare_gs_args_fun(HandleId),
                    validate_result_fun = fun(_, Result) ->
                        ?assertMatch(ok, Result)
                    end
                }
            ],
            data_spec = #data_spec{
                required = [<<"metadataString">>],
                correct_values = #{
                    <<"metadataString">> => [<<
                        "<?xml version=\"1.0\" encoding=\"utf-8\"?><metadata>",
                        "    <dc:contributor>Jane Doe</dc:contributor>",
                        "    <dc:description>Lorem ipsum</dc:description>",
                        "</metadata>"
                    >>]
                }
            }
        }
    ])),

    #document{value = #od_handle{
        public_handle = PublicHandle,
        metadata = MetadataStringResult
    }} = get_handle(?FILE_REF(FileGuid), HandleId),
    ExpectedMetadata = <<
        "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n<metadata>\n",
        "    <dc:identifier>", PublicHandle/binary, "</dc:identifier>"
        "    <dc:contributor>Jane Doe</dc:contributor>",
        "    <dc:description>Lorem ipsum</dc:description>",
        "</metadata>"
    >>,
    ?assertEqual(ExpectedMetadata, MetadataStringResult).


%% @private
-spec update_handle_prepare_gs_args_fun(onenv_api_test_runner:api_test_ctx()) ->
    onenv_api_test_runner:gs_args().
update_handle_prepare_gs_args_fun(HandleId) ->
    fun(#api_test_ctx{data = Data}) ->
        #gs_args{
            operation = update,
            gri = #gri{type = op_handle, id = HandleId, aspect = instance, scope = private},
            data = Data
        }
    end.


%%%===================================================================
%%% Common handle test utils
%%%===================================================================


%% @private
-spec generate_random_file_spec([onenv_file_test_utils:shares_spec()]) ->
    {binary(), onenv_file_test_utils:file_spec()}.
generate_random_file_spec(ShareSpecs) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    case FileType of
        <<"file">> -> {<<"REG">>, #file_spec{shares = ShareSpecs}};
        <<"dir">> -> {<<"DIR">>, #dir_spec{shares = ShareSpecs}}
    end.


%% @private
create_handle(ShareId, HServiceId) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
    MetadataPrefix = ?METADATA_PREFIX,
    MetadataString = ?METADATA_STRING,
   opw_test_rpc:insecure_call(P1Node, mi_handles, create,
        [SessId, ShareId, HServiceId, MetadataPrefix, MetadataString]
    ).


%% @private
get_handle(FileRef, HandleId) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    SessId = oct_background:get_user_session_id(user1, krakow),
   opw_test_rpc:insecure_call(P1Node, mi_handles, get,
        [SessId, FileRef, HandleId]
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec maybe_inject_share_id(map(), od_share:id()) -> map().
maybe_inject_share_id(Data, ShareId) ->
    case maps:get(<<"shareId">>, Data, undefined) of
        shareId -> Data#{<<"shareId">> => ShareId};
        _ -> Data
    end.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op-handle-proxy",
        posthook = fun(Config) ->
            UserId = oct_background:get_user_id(user1),
            HServiceId = hd(ozw_test_rpc:list_handle_services()),
            ok = ozw_test_rpc:add_user_to_handle_service(HServiceId, UserId),
            Config
        end
    }).


end_per_suite(_Config) ->
    UserId = oct_background:get_user_id(user1),
    HServiceId = hd(ozw_test_rpc:list_handle_services()),
    ok = ozw_test_rpc:remove_user_to_handle_service(HServiceId, UserId),
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    Config.


end_per_group(_Group, _Config) ->
    ok.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, Config) ->
    Config.
