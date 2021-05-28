%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions operating on datasets used in onenv ct tests.
%%% @end
%%%-------------------------------------------------------------------
-module(onenv_dataset_test_utils).
-author("Bartosz Walkowicz").

-include("onenv_test_utils.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([
    set_up_and_sync_dataset/2,
    set_up_and_sync_dataset/3,
    set_up_dataset/4,
    await_dataset_sync/4,

    get_exp_child_datasets/5,

    cleanup_all_datasets/1,
    cleanup_all_datasets/2
]).

-type dataset_spec() :: #dataset_spec{}.
-type dataset_object() :: #dataset_object{}.

-export_type([dataset_spec/0, dataset_object/0]).


-define(ATTEMPTS, 60).


%%%===================================================================
%%% API
%%%===================================================================


-spec set_up_and_sync_dataset(
    oct_background:entity_selector(),
    onenv_file_test_utils:object_selector()
) ->
    dataset_object().
set_up_and_sync_dataset(UserSelector, RootFileSelector) ->
    set_up_and_sync_dataset(UserSelector, RootFileSelector, #dataset_spec{}).


-spec set_up_and_sync_dataset(
    oct_background:entity_selector(),
    onenv_file_test_utils:object_selector(),
    dataset_spec()
) ->
    dataset_object().
set_up_and_sync_dataset(UserSelector, RootFileSelector, DatasetSpec) ->
    UserId = oct_background:get_user_id(UserSelector),
    {RootFileGuid, SpaceId} = onenv_file_test_utils:resolve_file(RootFileSelector),

    [CreationProvider | SyncProviders] = lists_utils:shuffle(oct_background:get_space_supporting_providers(
        SpaceId
    )),

    DatasetObj = set_up_dataset(CreationProvider, UserId, RootFileGuid, DatasetSpec),
    await_dataset_sync(CreationProvider, SyncProviders, UserId, DatasetObj),

    DatasetObj.


-spec set_up_dataset(
    oct_background:entity_selector(),
    od_user:id(),
    file_id:file_guid(),
    undefined | dataset_spec()
) ->
    undefined | dataset_object().
set_up_dataset(_CreationProvider, _UserId, _FileGuid, undefined) ->
    undefined;
set_up_dataset(CreationProvider, UserId, FileGuid, #dataset_spec{
    state = State,
    protection_flags = ProtectionFlagsJson,
    archives = Archives
}) ->
    CreationNode = ?OCT_RAND_OP_NODE(CreationProvider),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    Flags = file_meta:protection_flags_from_json(ProtectionFlagsJson),

    {ok, DatasetId} = ?assertMatch(
        {ok, _},
        lfm_proxy:establish_dataset(CreationNode, UserSessId, ?FILE_REF(FileGuid), Flags),
        ?ATTEMPTS
    ),
    case State of
        ?ATTACHED_DATASET ->
            ok;
        ?DETACHED_DATASET ->
            ?assertEqual(ok, lfm_proxy:update_dataset(
                CreationNode, UserSessId, DatasetId,
                ?DETACHED_DATASET, ?no_flags_mask, ?no_flags_mask
            ))
    end,

    ArchiveSpecs = case is_integer(Archives) of
        true -> [#archive_spec{} || _ <- lists:seq(1, Archives)];
        false -> Archives
    end,

    ArchiveObjs = lists:map(fun(ArchiveSpec) ->
        onenv_archive_test_utils:set_up_archive(CreationProvider, UserId, DatasetId, ArchiveSpec)
    end, ArchiveSpecs),

    #dataset_object{
        id = DatasetId,
        state = State,
        protection_flags = ProtectionFlagsJson,
        space_id = file_id:guid_to_space_id(FileGuid),
        archives = ArchiveObjs
    }.


-spec await_dataset_sync(
    oct_background:entity_selector(),
    [oct_background:entity_selector()],
    od_user:id(),
    undefined | dataset_object()
) ->
    ok | no_return().
await_dataset_sync(_CreationProvider, _SyncProviders, _UserId, undefined) ->
    ok;

await_dataset_sync(CreationProvider, SyncProviders, UserId, #dataset_object{
    id = DatasetId,
    state = State,
    protection_flags = ProtectionFlagsJson,
    archives = ArchiveObjs
}) ->
    CreationNode = ?OCT_RAND_OP_NODE(CreationProvider),
    CreationNodeSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    Flags = file_meta:protection_flags_from_json(ProtectionFlagsJson),

    {ok, DatasetInfo} = ?assertMatch(
        {ok, #dataset_info{state = State, id = DatasetId, protection_flags = Flags}},
        lfm_proxy:get_dataset_info(CreationNode, CreationNodeSessId, DatasetId)
    ),

    lists_utils:pforeach(fun(SyncProvider) ->
        SyncNode = ?OCT_RAND_OP_NODE(SyncProvider),
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),

        ?assertEqual(
            {ok, DatasetInfo},
            lfm_proxy:get_dataset_info(SyncNode, SessId, DatasetId),
            ?ATTEMPTS
        )
    end, SyncProviders),

    lists_utils:pforeach(fun(ArchiveObj) ->
        onenv_archive_test_utils:await_archive_sync(CreationProvider, SyncProviders, UserId, ArchiveObj, DatasetId)
    end, ArchiveObjs).



-spec get_exp_child_datasets(
    dataset:state(),
    file_meta:path(),
    dataset:id(),
    [binary()],
    onenv_file_test_utils:object()
) ->
    [{file_meta:name(), dataset:id(), lfm_datasets:info()}].
get_exp_child_datasets(State, ParentDirPath, ParentDatasetId, ParentEffProtectionFlagsJson,
    #object{dataset = #dataset_object{
    id = ParentDatasetId
}} = Object) ->
    get_exp_child_datasets(State, ParentDirPath, ParentDatasetId, ParentEffProtectionFlagsJson,
        Object#object{dataset = undefined});

get_exp_child_datasets(State, ParentDirPath, ParentDatasetId, ParentEffProtectionFlagsJson, Object) ->
    lists:keysort(1, lists:flatten(get_exp_child_datasets_internal(
        State, ParentDirPath, ParentDatasetId, ParentEffProtectionFlagsJson, Object
    ))).

-spec cleanup_all_datasets(oct_background:entity_selector()) -> ok.
cleanup_all_datasets(SpaceSelector) ->
    Providers = oct_background:get_space_supporting_providers(SpaceSelector),
    cleanup_all_datasets(Providers, SpaceSelector).


-spec cleanup_all_datasets(oct_background:entity_selector() | [oct_background:entity_selector()],
    oct_background:entity_selector()) -> ok.
cleanup_all_datasets(ProviderSelectors, SpaceSelector) ->
    ProviderSelectors2 = utils:ensure_list(ProviderSelectors),
    ProviderNodes = [oct_background:get_random_provider_node(ProviderSelector) || ProviderSelector <- ProviderSelectors2],
    SpaceId = oct_background:get_space_id(SpaceSelector),
    cleanup_and_verify_datasets(ProviderNodes, SpaceId, ?ATTACHED_DATASETS_STRUCTURE),
    cleanup_and_verify_datasets(ProviderNodes, SpaceId, ?DETACHED_DATASETS_STRUCTURE).

%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_exp_child_datasets_internal(
    dataset:state(),
    file_meta:path(),
    dataset:id(),
    [binary()],
    onenv_file_test_utils:object()
) ->
    [{file_meta:name(), dataset:id(), lfm_datasets:info()}].
get_exp_child_datasets_internal(State, ParentDirPath, ParentDatasetId, ParentEffProtectionFlagsJson, #object{
    type = ObjType,
    name = ObjName,
    guid = ObjGuid,
    dataset = #dataset_object{
        id = DatasetId,
        state = State,
        protection_flags = ProtectionFlagsJson
    }
}) ->
    ObjPath = filename:join(["/", ParentDirPath, ObjName]),
    CreationTime = time_test_utils:get_frozen_time_seconds(),
    EffProtectionFlags = case State of
        ?ATTACHED_DATASET -> lists:usort(ProtectionFlagsJson ++ ParentEffProtectionFlagsJson);
        ?DETACHED_DATASET -> []
    end,

    DatasetInfo = #dataset_info{
        id = DatasetId,
        state = State,
        root_file_guid = ObjGuid,
        root_file_path = ObjPath,
        root_file_type = ObjType,
        creation_time = CreationTime,
        protection_flags = file_meta:protection_flags_from_json(ProtectionFlagsJson),
        eff_protection_flags = file_meta:protection_flags_from_json(EffProtectionFlags),
        parent = ParentDatasetId,
        index = datasets_structure:pack_entry_index(ObjName, DatasetId)
    },
    {ObjName, DatasetId, DatasetInfo};

get_exp_child_datasets_internal(_State, _ParentDirPath, _ParentDatasetId, _ParentEffProtectionFlagsJson, #object{
    type = ?REGULAR_FILE_TYPE
}) ->
    [];

get_exp_child_datasets_internal(State, ParentDirPath, ParentDatasetId, ParentEffProtectionFlagsJson, #object{
    type = ?DIRECTORY_TYPE,
    name = DirName,
    children = Children
}) ->
    DirPath = filename:join(["/", ParentDirPath, DirName]),

    lists:map(fun(Child) ->
        get_exp_child_datasets_internal(State, DirPath, ParentDatasetId, ParentEffProtectionFlagsJson, Child)
    end, Children).


%% @private
-spec cleanup_and_verify_datasets([node()], od_space:id(), datasets_structure:forest_type()) -> ok.
cleanup_and_verify_datasets(Nodes, SpaceId, ForestType) ->
    cleanup_datasets(lists_utils:random_element(Nodes), SpaceId, ForestType),
    assert_all_dataset_entries_are_deleted_on_all_nodes(SpaceId, ForestType).


%% @private
-spec cleanup_datasets(node(), od_space:id(), datasets_structure:forest_type()) -> ok.
cleanup_datasets(Node, SpaceId, ForestType) ->
    {ok, Datasets} = rpc:call(Node, datasets_structure, list_all_unsafe, [SpaceId, ForestType]),
    lists:foreach(fun({_DatasetPath, {DatasetId, _DatasetName, _Index}}) ->
        cleanup_dataset(Node, DatasetId)
    end, Datasets).


%% @private
-spec cleanup_dataset(node(), dataset:id()) -> ok.
cleanup_dataset(Node, DatasetId) ->
    cleanup_dataset_archives(Node, DatasetId, 0),
    ?assertMatch(0, get_archive_count(Node, DatasetId), ?ATTEMPTS),
    lfm_proxy:remove_dataset(Node, ?ROOT_SESS_ID, DatasetId).


%% @private
-spec cleanup_dataset_archives(node(), dataset:id(), non_neg_integer()) -> ok.
cleanup_dataset_archives(Node, DatasetId, Offset) ->
    Limit = 1000,
    {ok, Archives, IsLast} =
        lfm_proxy:list_archives(Node, ?ROOT_SESS_ID, DatasetId, #{offset => Offset, limit => Limit}),
    lists:foreach(fun({_Index, ArchiveId}) ->
        rpc:call(Node, archive_api, remove_archive_recursive, [ArchiveId])
    end, Archives),
    case IsLast of
        true -> ok;
        false -> cleanup_dataset_archives(Node, DatasetId, Offset + length(Archives))
    end.


%% @private
-spec assert_all_dataset_entries_are_deleted_on_all_nodes(od_space:id(), datasets_structure:forest_type()) -> ok.
assert_all_dataset_entries_are_deleted_on_all_nodes(SpaceId, ForestType) ->
    lists:foreach(fun(N) ->
        ?assertMatch({ok, []}, rpc:call(N, datasets_structure, list_all_unsafe, [SpaceId, ForestType]), ?ATTEMPTS)
    end, oct_background:get_all_providers_nodes()).


%% @private
-spec get_archive_count(node(), dataset:id()) -> non_neg_integer().
get_archive_count(Node, DatasetId) ->
    case lfm_proxy:get_dataset_info(Node, ?ROOT_SESS_ID, DatasetId) of
        {ok, #dataset_info{archive_count = ArchiveCount}} -> ArchiveCount;
        {error, ?ENOENT} -> 0
    end.