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
-include("modules/logical_file_manager/lfm.hrl").


-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1
]).

-export([
    create_handle_test/1,
    get_public_handle_data_test/1,
    update_handle_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_handle_test,
        get_public_handle_data_test,
        update_handle_test
    ]}
].

all() -> [
    {group, all_tests}
].

-define(SPACE_OWNER_AND_HS_MEMBER, space_owner).
-define(NON_SPACE_MEMBER_AND_HS_ADMIN, handle_service_admin).
-define(SPACE_MEMBER_AND_NON_HS_MEMBER, space_member).
-define(NON_SPACE_MEMBER_AND_NON_HS_MEMBER, non_space_member).

-define(PROVIDER_SELECTOR, krakow).
-define(SPACE_SELECTOR, space_krk).
-define(METADATA_PREFIX, <<"oai_dc">>).


%%%===================================================================
%%% Get file distribution test functions
%%%===================================================================


create_handle_test(_Config) ->
    HServiceId = hd(ozt_handle_services:list_handle_services()),
    MemRef = api_test_memory:init(),
    Metadata = ozt_handles:example_metadata_variant(?METADATA_PREFIX, 1),

    ValidateResultFun = fun(_, {ok, #{
        <<"handleService">> := HServiceInDb,
        <<"metadataPrefix">> := MetadataPrefixInDb,
        <<"metadataString">> := MetadataInDb,
        <<"url">> := Url
    }}) ->
        ExpectedHService = gri:serialize(#gri{
            type = op_handle_service, id = HServiceId, aspect = instance, scope = public
        }),
        ExpectedMetadata = ozt_handles:expected_metadata_after_publication(Metadata, Url),

        ?assertMatch(
            {ExpectedHService, ?METADATA_PREFIX, ExpectedMetadata},
            {HServiceInDb, MetadataPrefixInDb, MetadataInDb}
        )
    end,
    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [?PROVIDER_SELECTOR],
            client_spec = #client_spec{
                correct = [?SPACE_OWNER_AND_HS_MEMBER],
                unauthorized = [nobody],
                forbidden_in_space = [?SPACE_MEMBER_AND_NON_HS_MEMBER],
                forbidden_not_in_space = [?NON_SPACE_MEMBER_AND_NON_HS_MEMBER, ?NON_SPACE_MEMBER_AND_HS_ADMIN]
            },
            setup_fun = build_create_handle_setup_fun(MemRef),
            teardown_fun = build_create_handle_teardown_fun(MemRef),
            scenario_templates = [
                #scenario_template{
                    name = <<"Create handle for share using gs api">>,
                    type = gs,
                    prepare_args_fun = create_handle_prepare_gs_args_fun(MemRef),
                    validate_result_fun = ValidateResultFun
                }
            ],
            data_spec = #data_spec{
                required = [
                    <<"shareId">>, <<"handleServiceId">>,
                    <<"metadataPrefix">>, <<"metadataString">>
                ],
                correct_values = #{
                    <<"shareId">> => [share_id],
                    <<"handleServiceId">> => [HServiceId],
                    <<"metadataPrefix">> => [?METADATA_PREFIX],
                    <<"metadataString">> => [Metadata]
                }
            }
        }
    ])).


%% @private
-spec build_create_handle_setup_fun(api_test_memory:mem_ref()) -> onenv_api_test_runner:setup_fun().
build_create_handle_setup_fun(MemRef) ->
    fun() ->
        #object{guid = Guid, shares = [ShareId]} = create_and_sync_shared_file_of_random_type(),
        api_test_memory:set(MemRef, guid, Guid),
        api_test_memory:set(MemRef, share_id, ShareId)
    end.


%% @private
-spec create_handle_prepare_gs_args_fun(onenv_api_test_runner:api_test_ctx()) ->
    onenv_api_test_runner:gs_args().
create_handle_prepare_gs_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        #gs_args{
            operation = create,
            gri = #gri{type = op_handle, aspect = instance, scope = private},
            data = case maps:get(<<"shareId">>, Data, undefined) of
                share_id -> Data#{<<"shareId">> => api_test_memory:get(MemRef, share_id)};
                _ -> Data
            end
        }
    end.


%% @private
-spec build_create_handle_teardown_fun(api_test_memory:mem_ref()) -> onenv_api_test_runner:setup_fun().
build_create_handle_teardown_fun(MemRef) ->
    fun() ->
        onenv_file_test_utils:rm_and_sync_file(?SPACE_OWNER_AND_HS_MEMBER, api_test_memory:get(MemRef, guid))
    end.


get_public_handle_data_test(_Config) ->
    HServiceId = hd(ozt_handle_services:list_handle_services()),
    #object{shares = [ShareId]} = create_and_sync_shared_file_of_random_type(),
    Metadata = ozt_handles:example_metadata_variant(?METADATA_PREFIX, 1),
    HandleId = ozt_handles:create(
        ?PROVIDER_SELECTOR, ?SPACE_OWNER_AND_HS_MEMBER, ShareId, HServiceId, ?METADATA_PREFIX, Metadata
    ),
    PublicHandle = opt_handles:get_public_handle_url(?PROVIDER_SELECTOR, ?SPACE_OWNER_AND_HS_MEMBER, HandleId),

    ValidateResultFun = fun(_, {ok, Result}) ->
        ExpectedHandleData =  #{
            <<"gri">> => gri:serialize(#gri{
                type = op_handle, id = HandleId, aspect = instance, scope = public
            }),
            <<"handleService">> => gri:serialize(#gri{
                type = op_handle_service, id = HServiceId, aspect = instance, scope = public
            }),
            <<"metadataPrefix">> => ?METADATA_PREFIX,
            <<"metadataString">> => ozt_handles:expected_metadata_after_publication(Metadata, PublicHandle),
            <<"revision">> => 1,
            <<"url">> => PublicHandle
        },

        ?assertEqual(ExpectedHandleData, Result)
    end,
    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [?PROVIDER_SELECTOR],
            client_spec = #client_spec{
                correct = [
                    ?SPACE_OWNER_AND_HS_MEMBER, ?NON_SPACE_MEMBER_AND_HS_ADMIN,
                    nobody, ?SPACE_MEMBER_AND_NON_HS_MEMBER, ?NON_SPACE_MEMBER_AND_NON_HS_MEMBER
                ]
            },
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
    fun(#api_test_ctx{}) ->
        #gs_args{
            operation = get,
            gri = #gri{type = op_handle, id = HandleId, aspect = instance, scope = public}
        }
    end.


update_handle_test(_Config) ->
    MemRef = api_test_memory:init(),
    MetadataAfterUpdate = ozt_handles:example_metadata_variant(?METADATA_PREFIX, 2),

    ValidateResultFun = fun(_, ok) ->
        HandleId = api_test_memory:get(MemRef, handle_id),
        PublicHandle = opt_handles:get_public_handle_url(?PROVIDER_SELECTOR, ?SPACE_OWNER_AND_HS_MEMBER, HandleId),
        #document{value = #od_handle{
            metadata = MetadataInDbAfterUpdate
        }} = opt_handles:get(?PROVIDER_SELECTOR, ?SPACE_OWNER_AND_HS_MEMBER, HandleId),
        ExpectedMetadata = ozt_handles:expected_metadata_after_publication(MetadataAfterUpdate, PublicHandle),
        ?assertEqual(ExpectedMetadata, MetadataInDbAfterUpdate)
    end,

    ?assert(onenv_api_test_runner:run_tests([
        #suite_spec{
            target_nodes = [?PROVIDER_SELECTOR],
            client_spec = #client_spec{
                correct = [?SPACE_OWNER_AND_HS_MEMBER, ?NON_SPACE_MEMBER_AND_HS_ADMIN],
                unauthorized = [nobody],
                forbidden_in_space = [?SPACE_MEMBER_AND_NON_HS_MEMBER],
                forbidden_not_in_space = [?NON_SPACE_MEMBER_AND_NON_HS_MEMBER]
            },
            setup_fun = build_update_handle_setup_fun(
                MemRef, ozt_handles:example_metadata_variant(?METADATA_PREFIX, 1)
            ),
            scenario_templates = [
                #scenario_template{
                    name = <<"Update handle using gs api">>,
                    type = gs,
                    prepare_args_fun = update_handle_prepare_gs_args_fun(MemRef),
                    validate_result_fun = ValidateResultFun
                }
            ],
            data_spec = #data_spec{
                required = [<<"metadataString">>],
                correct_values = #{
                    <<"metadataString">> => [MetadataAfterUpdate]
                }
            }
        }
    ])).


%% @private
-spec build_update_handle_setup_fun(api_test_memory:mem_ref(), od_handle:metadata()) ->
    onenv_api_test_runner:setup_fun().
build_update_handle_setup_fun(MemRef, Metadata) ->
    fun() ->
        HServiceId = hd(ozt_handle_services:list_handle_services()),
        #object{shares = [ShareId]} = create_and_sync_shared_file_of_random_type(),
        HandleId = ozt_handles:create(
            ?PROVIDER_SELECTOR, ?SPACE_OWNER_AND_HS_MEMBER, ShareId, HServiceId, ?METADATA_PREFIX, Metadata
        ),
        api_test_memory:set(MemRef, handle_id, HandleId)
    end.


%% @private
-spec update_handle_prepare_gs_args_fun(onenv_api_test_runner:api_test_ctx()) ->
    onenv_api_test_runner:gs_args().
update_handle_prepare_gs_args_fun(MemRef) ->
    fun(#api_test_ctx{data = Data}) ->
        #gs_args{
            operation = update,
            gri = #gri{
                type = op_handle, id = api_test_memory:get(MemRef, handle_id),
                aspect = instance, scope = private
            },
            data = Data
        }
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_and_sync_shared_file_of_random_type() -> onenv_file_test_utils:object().
create_and_sync_shared_file_of_random_type() ->
    ShareSpecs = [#share_spec{}],
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FileSpec = case FileType of
        <<"file">> -> #file_spec{shares = ShareSpecs};
        <<"dir">> ->  #dir_spec{shares = ShareSpecs}
    end,
    onenv_file_test_utils:create_and_sync_file_tree(
        ?SPACE_OWNER_AND_HS_MEMBER, ?SPACE_SELECTOR, FileSpec
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    LoadModules = [opt_handles, ozt_handles, ozt_handle_services],
    oct_background:init_per_suite([{?LOAD_MODULES, LoadModules} | Config], #onenv_test_config{
        onenv_scenario = "1op-handle-proxy",
        posthook = fun(NewConfig) ->
            % make sure there are no remnants from the previous test runs
            ozt_handle_services:remove_user_from_all_handle_services(
                ?SPACE_OWNER_AND_HS_MEMBER, ?PROVIDER_SELECTOR
            ),
            ozt_handle_services:remove_user_from_all_handle_services(
                ?NON_SPACE_MEMBER_AND_HS_ADMIN, ?PROVIDER_SELECTOR
            ),

            ozt_handle_services:add_user_to_all_handle_services(?SPACE_OWNER_AND_HS_MEMBER),
            ozt_handle_services:add_user_to_all_handle_services(
                ?NON_SPACE_MEMBER_AND_HS_ADMIN, privileges:handle_service_admin()
            ),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().