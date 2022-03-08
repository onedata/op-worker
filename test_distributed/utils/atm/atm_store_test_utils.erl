%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module with utility functions for automation store tests
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_test_utils).
-author("Michal Stanisz").

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([create_workflow_execution_auth/3]).
-export([
    build_store_schema/1, build_store_schema/2, build_store_schema/3,
    build_create_store_with_initial_content_fun/3,
    build_workflow_execution_env/3,
    example_data_spec/1,
    gen_valid_data/3,
    gen_invalid_data/3,
    compress_and_expand_data/4,
    randomly_remove_entity_referenced_by_item/4,
    split_into_chunks/3
]).


-define(ATTEMPTS, 60).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_workflow_execution_auth(
    oct_background:entity_selector(),
    oct_background:entity_selector(),
    oct_background:entity_selector()
) ->
    atm_workflow_execution_auth:record() | no_return().
create_workflow_execution_auth(ProviderSelector, UserSelector, SpaceSelector) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),

    AtmWorkflowExecutionId = ?RAND_STR(32),

    SessionId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    UserCtx = rpc:call(Node, user_ctx, new, [SessionId]),
    ok = rpc:call(Node, atm_workflow_execution_session, init, [AtmWorkflowExecutionId, UserCtx]),

    SpaceId = oct_background:get_space_id(SpaceSelector),
    rpc:call(Node, atm_workflow_execution_auth, build, [SpaceId, AtmWorkflowExecutionId, UserCtx]).


-spec build_store_schema(atm_store_config:record()) -> atm_store_schema:record().
build_store_schema(Config) ->
    build_store_schema(Config, false).


-spec build_store_schema(atm_store_config:record(), boolean()) ->
    atm_store_schema:record().
build_store_schema(Config, RequiresInitialContent) ->
    build_store_schema(Config, RequiresInitialContent, undefined).


-spec build_store_schema(atm_store_config:record(), boolean(), undefined | automation:item()) ->
    atm_store_schema:record().
build_store_schema(Config, RequiresInitialContent, DefaultInitialContent) ->
    #atm_store_schema{
        id = ?RAND_STR(16),
        name = ?RAND_STR(16),
        description = ?RAND_STR(16),
        type = infer_store_type(Config),
        config = Config,
        requires_initial_content = RequiresInitialContent,
        default_initial_content = DefaultInitialContent
    }.


-spec build_create_store_with_initial_content_fun(
    atm_workflow_execution_auth:record(),
    atm_store_config:record(),
    atm_value:expanded()
) ->
    fun((atm_value:expanded()) -> {ok, atm_store:doc()} | no_return()).
build_create_store_with_initial_content_fun(
    AtmWorkflowExecutionAuth,
    AtmStoreConfig,
    DefaultContentInitializer
) ->
    fun(ContentInitializer) ->
        case rand:uniform(3) of
            1 ->
                StoreSchema = atm_store_test_utils:build_store_schema(AtmStoreConfig, false),
                atm_store_api:create(AtmWorkflowExecutionAuth, ContentInitializer, StoreSchema);
            2 ->
                StoreSchema = atm_store_test_utils:build_store_schema(
                    AtmStoreConfig, false, ContentInitializer
                ),
                atm_store_api:create(AtmWorkflowExecutionAuth, undefined, StoreSchema);
            3 ->
                % Default content initializer (from schema) should be overridden
                % by one specified in args when creating store
                StoreSchema = atm_store_test_utils:build_store_schema(
                    AtmStoreConfig, false, DefaultContentInitializer
                ),
                atm_store_api:create(AtmWorkflowExecutionAuth, ContentInitializer, StoreSchema)
        end
    end.


-spec build_workflow_execution_env(
    atm_workflow_execution_auth:record(),
    atm_store_schema:record(),
    atm_store:id()
) ->
    atm_workflow_execution_env:record().
build_workflow_execution_env(AtmWorkflowExecutionAuth, AtmStoreSchema, AtmStoreId) ->
    atm_workflow_execution_env:build(
        atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
        atm_workflow_execution_auth:get_workflow_execution_id(AtmWorkflowExecutionAuth),
        0,
        #{AtmStoreSchema#atm_store_schema.id => AtmStoreId}
    ).


%% TODO VFS-8686 add array data spec generation after implementing compress/expand for array
-spec example_data_spec(atm_data_type:type()) -> atm_data_spec:record().
example_data_spec(atm_time_series_measurements_type) ->
    RandSpecs = atm_test_utils:example_time_series_measurements_specs(),

    #atm_data_spec{
        type = atm_time_series_measurements_type,
        value_constraints = #{specs => lists_utils:random_sublist(RandSpecs, 1, all)}
    };

example_data_spec(AtmDataType) when
    AtmDataType =:= atm_dataset_type;
    AtmDataType =:= atm_file_type;
    AtmDataType =:= atm_integer_type;
    AtmDataType =:= atm_object_type;
    AtmDataType =:= atm_onedatafs_credentials_type;
    AtmDataType =:= atm_range_type;
    AtmDataType =:= atm_string_type
->
    #atm_data_spec{type = AtmDataType}.


-spec gen_valid_data(
    oct_background:node_selector(),
    atm_workflow_execution_auth:record(),
    atm_data_spec:record()
) ->
    atm_value:expanded().
gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_array_type,
    value_constraints = #{item_data_spec := ItemDataSpec}
}) ->
    lists:map(
        fun(_) -> gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, ItemDataSpec) end,
        lists:seq(1, ?RAND_INT(5, 10))
    );

gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_dataset_type
}) ->
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),

    #file_attr{guid = FileGuid, type = FileType} = create_random_file_in_space_root_dir(
        ProviderSelector, AtmWorkflowExecutionAuth
    ),
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),

    #{
        <<"datasetId">> => ?rpc(ProviderSelector, mi_datasets:establish(
            SessionId, ?FILE_REF(FileGuid), ?no_flags_mask
        )),
        <<"rootFileId">> => ObjectId,
        <<"rootFileType">> => str_utils:to_binary(FileType)
    };

gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_file_type
}) ->
    #file_attr{guid = FileGuid, type = FileType} = create_random_file_in_space_root_dir(
        ProviderSelector, AtmWorkflowExecutionAuth
    ),
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),

    #{
        <<"file_id">> => ObjectId,
        <<"type">> => str_utils:to_binary(FileType)
    };

gen_valid_data(_ProviderSelector, _AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_integer_type
}) ->
    rand:uniform(1000000);

gen_valid_data(_ProviderSelector, _AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_object_type
}) ->
    lists:foldl(fun(_, Acc) ->
        Acc#{?RAND_STR(32) => lists_utils:random_element([?RAND_STR(32), rand:uniform(1000000)])}
    end, #{}, lists:seq(1, ?RAND_INT(3, 5)));

gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_onedatafs_credentials_type
}) ->
    #{
        <<"host">> => ?rpc(ProviderSelector, oneprovider:get_domain()),
        <<"accessToken">> => atm_workflow_execution_auth:get_access_token(AtmWorkflowExecutionAuth)
    };

gen_valid_data(_ProviderSelector, _AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_range_type
}) ->
    case rand:uniform(3) of
        1 ->
            #{<<"end">> => ?RAND_INT(10, 200)};
        2 ->
            #{
                <<"end">> => ?RAND_INT(10, 20),
                <<"start">> => - (?RAND_INT(0, 10)),
                <<"step">> => ?RAND_INT(1, 5)
            };
        3 ->
            #{
                <<"end">> => - (?RAND_INT(10, 20)),
                <<"start">> => ?RAND_INT(0, 10),
                <<"step">> => - (?RAND_INT(1, 5))
            }
    end;

gen_valid_data(_ProviderSelector, _AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_string_type
}) ->
    ?RAND_STR(32);

gen_valid_data(_ProviderSelector, _AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_time_series_measurements_type,
    value_constraints = #{specs := Specs}
}) ->
    lists:map(fun(_) ->
        #{
            <<"tsName">> => gen_ts_name(?RAND_ELEMENT(Specs)),
            <<"timestamp">> => ?RAND_INT(100000, 999999),
            <<"value">> => ?RAND_INT(1, 99)
        }
    end, lists:seq(1, ?RAND_INT(2, 5))).


%% @private
-spec gen_ts_name(atm_time_series_measurements_spec:record()) ->
    atm_time_series_attribute:name().
gen_ts_name(#atm_time_series_measurements_spec{
    name_matcher_type = exact,
    name_matcher = TsName
}) ->
    TsName;

gen_ts_name(#atm_time_series_measurements_spec{
    name_matcher_type = has_prefix,
    name_matcher = Pattern
}) ->
    binary:replace(Pattern, <<"*">>, <<"NIHAU">>).


-spec gen_invalid_data(
    oct_background:node_selector(),
    atm_workflow_execution_auth:record(),
    atm_data_spec:record()
) ->
    atm_value:expanded().
gen_invalid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_array_type,
    value_constraints = #{item_data_spec := #atm_data_spec{type = ItemDataType}}
}) ->
    InvalidItemDataSpec = example_data_spec(?RAND_ELEMENT(all_basic_data_types() -- [ItemDataType])),

    lists:map(
        fun(_) -> gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, InvalidItemDataSpec) end,
        lists:seq(1, ?RAND_INT(5, 10))
    );

gen_invalid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_object_type
}) ->
    gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
        type = lists_utils:random_element([atm_integer_type, atm_string_type])
    });

gen_invalid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
    type = AtmDataType
}) ->
    InvalidDataSpec = example_data_spec(?RAND_ELEMENT(all_basic_data_types() -- [AtmDataType])),
    gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, InvalidDataSpec).


-spec compress_and_expand_data(
    oct_background:node_selector(),
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_store:id()
) ->
    atm_value:expanded().
compress_and_expand_data(ProviderSelector, AtmWorkflowExecutionAuth, Data, AtmDataSpec) ->
    %% Some data types supported in atm are just references to entities in op.
    %% When retrieving items of such types from stores value returned may differ
    %% from the one given during adding to store (actual data about such entity
    %% is fetched using reference and returned)
    {ok, ExpandedData} = ?rpc(ProviderSelector, atm_value:expand(
        AtmWorkflowExecutionAuth,
        atm_value:compress(Data, AtmDataSpec),
        AtmDataSpec
    )),
    ExpandedData.


-spec randomly_remove_entity_referenced_by_item(
    oct_background:node_selector(),
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_data_spec:record()
) ->
    false | {true, errors:error()}.
randomly_remove_entity_referenced_by_item(ProviderSelector, AtmWorkflowExecutionAuth, Item, #atm_data_spec{
    type = atm_file_type
}) ->
    case rand:uniform(5) of
        1 ->
            SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),
            {ok, FileGuid} = file_id:objectid_to_guid(maps:get(<<"file_id">>, Item)),
            FileRef = ?FILE_REF(FileGuid),

            ?rpc(ProviderSelector, lfm:rm_recursive(SessionId, FileRef)),
            ?assertEqual({error, ?ENOENT}, ?rpc(ProviderSelector, lfm:stat(SessionId, FileRef)), ?ATTEMPTS),

            {true, ?ERROR_POSIX(?ENOENT)};
        _ ->
            false
    end;

randomly_remove_entity_referenced_by_item(ProviderSelector, AtmWorkflowExecutionAuth, Item, #atm_data_spec{
    type = atm_dataset_type
}) ->
    case rand:uniform(5) of
        1 ->
            SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),
            ?rpc(ProviderSelector, mi_datasets:remove(SessionId, maps:get(<<"datasetId">>, Item))),
            {true, ?ERROR_NOT_FOUND};
        _ ->
            false
    end;

randomly_remove_entity_referenced_by_item(_ProviderSelector, _AtmWorkflowExecutionAuth, _Item, _AtmDataSpec) ->
    false.


-spec split_into_chunks(pos_integer(), [[automation:item()]], [automation:item()]) ->
    [[automation:item()]].
split_into_chunks(_Size, Acc, []) ->
    lists:reverse(Acc);
split_into_chunks(Size, Acc, [_ | _] = Items) ->
    Chunk = lists:sublist(Items, 1, Size),
    split_into_chunks(Size, [Chunk | Acc], Items -- Chunk).


%%%===================================================================
%%% Helper functions
%%%===================================================================


%% @private
-spec infer_store_type(atm_store_config:record()) -> automation:store_type().
infer_store_type(#atm_audit_log_store_config{}) -> audit_log;
infer_store_type(#atm_list_store_config{}) -> list;
infer_store_type(#atm_range_store_config{}) -> range;
infer_store_type(#atm_single_value_store_config{}) -> single_value;
infer_store_type(#atm_tree_forest_store_config{}) -> tree_forest.


%% @private
-spec all_basic_data_types() -> [atm_data_type:type()].
all_basic_data_types() -> [
    atm_dataset_type,
    atm_file_type,
    atm_integer_type,
    atm_object_type,
    atm_onedatafs_credentials_type,
    atm_range_type,
    atm_string_type,
    atm_time_series_measurements_type
].


%% @private
-spec create_random_file_in_space_root_dir(
    oct_background:node_selector(),
    atm_workflow_execution_auth:record()
) ->
    ok.
create_random_file_in_space_root_dir(ProviderSelector, AtmWorkflowExecutionAuth) ->
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),
    SpaceId = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    case lists_utils:random_element([?REGULAR_FILE_TYPE, ?DIRECTORY_TYPE, ?SYMLINK_TYPE, ?LINK_TYPE]) of
        ?REGULAR_FILE_TYPE ->
            {ok, RegFileGuid} = ?rpc(ProviderSelector, lfm:create(
                SessionId, SpaceGuid, ?RAND_STR(24), undefined
            )),
            {ok, RegFileAttrs} = ?rpc(ProviderSelector, lfm:stat(SessionId, ?FILE_REF(RegFileGuid))),
            RegFileAttrs;
        ?DIRECTORY_TYPE ->
            {ok, DirGuid} = ?rpc(ProviderSelector, lfm:mkdir(
                SessionId, SpaceGuid, ?RAND_STR(24), undefined
            )),
            {ok, DirAttrs} = ?rpc(ProviderSelector, lfm:stat(SessionId, ?FILE_REF(DirGuid))),
            DirAttrs;
        ?SYMLINK_TYPE ->
            {ok, SymlinkAttrs} = ?rpc(ProviderSelector, lfm:make_symlink(
                SessionId, ?FILE_REF(SpaceGuid), ?RAND_STR(24), ?RAND_STR(24)
            )),
            SymlinkAttrs;
        ?LINK_TYPE ->
            {ok, RegFileGuid} = ?rpc(ProviderSelector, lfm:create(
                SessionId, SpaceGuid, ?RAND_STR(24), undefined
            )),
            {ok, HardlinkAttrs} = ?lfm_check(?rpc(ProviderSelector, lfm:make_link(
                SessionId, ?FILE_REF(RegFileGuid), ?FILE_REF(SpaceGuid), ?RAND_STR(24)
            ))),
            HardlinkAttrs
    end.
