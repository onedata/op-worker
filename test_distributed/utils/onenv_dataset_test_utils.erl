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
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([
    establish_and_sync_dataset/2,
    establish_and_sync_dataset/3,
    prepare_dataset/4,
    await_dataset_sync/4,

    get_exp_child_datasets/4
]).

-type dataset_spec() :: #dataset_spec{}.
-type dataset_obj() :: #dataset_obj{}.

-export_type([dataset_spec/0, dataset_obj/0]).


-define(ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


-spec establish_and_sync_dataset(
    oct_background:entity_selector(),
    onenv_file_test_utils:object_selector()
) ->
    dataset:id().
establish_and_sync_dataset(UserSelector, RootFileSelector) ->
    establish_and_sync_dataset(UserSelector, RootFileSelector, []).


-spec establish_and_sync_dataset(
    oct_background:entity_selector(),
    onenv_file_test_utils:object_selector(),
    [binary()]
) ->
    dataset:id().
establish_and_sync_dataset(UserSelector, RootFileSelector, ProtectionFlags) ->
    UserId = oct_background:get_user_id(UserSelector),
    {RootFileGuid, SpaceId} = onenv_file_test_utils:resolve_file(RootFileSelector),

    [CreationProvider | SyncProviders] = lists_utils:shuffle(oct_background:get_space_supporting_providers(
        SpaceId
    )),

    DatasetObj = prepare_dataset(CreationProvider, UserId, RootFileGuid, #dataset_spec{
        protection_flags = ProtectionFlags
    }),
    await_dataset_sync(CreationProvider, SyncProviders, UserId, DatasetObj),

    DatasetObj#dataset_obj.id.


-spec prepare_dataset(
    oct_background:entity_selector(),
    od_user:id(),
    file_id:file_guid(),
    undefined | dataset_spec()
) ->
    undefined | dataset_obj().
prepare_dataset(_CreationProvider, _UserId, _FileGuid, undefined) ->
    undefined;
prepare_dataset(CreationProvider, UserId, FileGuid, #dataset_spec{
    state = State,
    protection_flags = ProtectionFlagsJon
}) ->
    CreationNode = ?RAND_OP_NODE(CreationProvider),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    Flags = file_meta:protection_flags_from_json(ProtectionFlagsJon),

    {ok, DatasetId} = ?assertMatch(
        {ok, _},
        lfm_proxy:establish_dataset(CreationNode, UserSessId, {guid, FileGuid}, Flags),
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

    #dataset_obj{
        id = DatasetId,
        state = State,
        protection_flags = ProtectionFlagsJon
    }.


%% @private
-spec await_dataset_sync(
    oct_background:entity_selector(),
    [oct_background:entity_selector()],
    od_user:id(),
    undefined | dataset_obj()
) ->
    ok | no_return().
await_dataset_sync(_CreationProvider, _SyncProviders, _UserId, undefined) ->
    ok;

await_dataset_sync(CreationProvider, SyncProviders, UserId, #dataset_obj{
    id = DatasetId,
    state = State,
    protection_flags = ProtectionFlagsJon
}) ->
    CreationNode = ?RAND_OP_NODE(CreationProvider),
    CreationNodeSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    Flags = file_meta:protection_flags_from_json(ProtectionFlagsJon),

    {ok, DatasetInfo} = ?assertMatch(
        {ok, #dataset_info{state = State, id = DatasetId, protection_flags = Flags}},
        lfm_proxy:get_dataset_info(CreationNode, CreationNodeSessId, DatasetId)
    ),

    lists:foreach(fun(SyncProvider) ->
        SyncNode = ?RAND_OP_NODE(SyncProvider),
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),

        ?assertEqual(
            {ok, DatasetInfo},
            lfm_proxy:get_dataset_info(SyncNode, SessId, DatasetId),
            ?ATTEMPTS
        ),

        % TODO maybe await dataset links sync ?
        ok
    end, SyncProviders).


-spec get_exp_child_datasets(
    dataset:state(),
    file_meta:path(),
    dataset:id(),
    onenv_file_test_utils:object()
) ->
    [{file_meta:name(), dataset:id(), lfm_datasets:attrs()}].
get_exp_child_datasets(State, ParentDirPath, ParentDatasetId, #object{dataset = #dataset_obj{
    id = ParentDatasetId
}} = Object) ->
    get_exp_child_datasets(State, ParentDirPath, ParentDatasetId, Object#object{dataset = undefined});

get_exp_child_datasets(State, ParentDirPath, ParentDatasetId, Object) ->
    lists:keysort(1, lists:flatten(get_exp_child_datasets_internal(
        State, ParentDirPath, ParentDatasetId, Object
    ))).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_exp_child_datasets_internal(
    dataset:state(),
    file_meta:path(),
    dataset:id(),
    onenv_file_test_utils:object()
) ->
    [{file_meta:name(), dataset:id(), lfm_datasets:attrs()}].
get_exp_child_datasets_internal(State, ParentDirPath, ParentDatasetId, #object{
    type = ObjType,
    name = ObjName,
    guid = ObjGuid,
    dataset = #dataset_obj{
        id = DatasetId,
        state = State,
        protection_flags = ProtectionFlags
    }
}) ->
    ObjPath = filename:join(["/", ParentDirPath, ObjName]),
    CreationTime = time_test_utils:get_frozen_time_seconds(),

    DatasetDetails = #dataset_info{
        id = DatasetId,
        state = State,
        guid = ObjGuid,
        path = ObjPath,
        type = ObjType,
        creation_time = CreationTime,
        protection_flags = ProtectionFlags,
        parent = ParentDatasetId
    },
    {ObjName, DatasetId, DatasetDetails};

get_exp_child_datasets_internal(_State, _ParentDirPath, _ParentDatasetId, #object{
    type = ?REGULAR_FILE_TYPE
}) ->
    [];

get_exp_child_datasets_internal(State, ParentDirPath, ParentDatasetId, #object{
    type = ?DIRECTORY_TYPE,
    name = DirName,
    children = Children
}) ->
    DirPath = filename:join(["/", ParentDirPath, DirName]),

    lists:map(fun(Child) ->
        get_exp_child_datasets_internal(State, DirPath, ParentDatasetId, Child)
    end, Children).
