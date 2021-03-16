%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of dataset_links module.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_test_SUITE).
-author("Jakub Kudzia").

-include("modules/fslogic/file_details.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    establish_dataset_attached_to_space_dir/1,
    establish_dataset_attached_to_dir/1,
    establish_dataset_attached_to_file/1,
    detach_and_reattach_dataset/1,
    establish_dataset_on_not_existing_file_should_fail/1,
    establish_2nd_dataset_on_file_should_fail/1,
    establish_nested_datasets_structure/1
]).

all() -> ?ALL([
    establish_dataset_attached_to_space_dir,
    establish_dataset_attached_to_dir,
    establish_dataset_attached_to_file,
    detach_and_reattach_dataset,
    establish_dataset_on_not_existing_file_should_fail,
    establish_2nd_dataset_on_file_should_fail,
    establish_nested_datasets_structure
]).


-define(SEP, <<?DIRECTORY_SEPARATOR>>).

-define(ATTEMPTS, 30).

-define(FILE_NAME, <<"file_", (?RAND_NAME)/binary>>).
-define(DIR_NAME, <<"dir_", (?RAND_NAME)/binary>>).
-define(RAND_NAME,
    <<(str_utils:to_binary(?FUNCTION))/binary, "_", (integer_to_binary(rand:uniform(?RAND_RANGE)))/binary>>).
-define(RAND_RANGE, 1000000000).

% TODO test z movem jak zmieni sie guid

%%%===================================================================
%%% API functions
%%%===================================================================

% TODO VFS-7363 testy z konfliktami
% efektywne datasety
% reatachowanie datasetu o zlym id

establish_dataset_attached_to_space_dir(_Config) ->
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    establish_dataset_attached_to_generic_file_test_base(SpaceGuid).

establish_dataset_attached_to_dir(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    establish_dataset_attached_to_generic_file_test_base(Guid).

establish_dataset_attached_to_file(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    establish_dataset_attached_to_generic_file_test_base(Guid).

detach_and_reattach_dataset(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirName = ?DIR_NAME,
    {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, SpaceGuid, DirName, ?DEFAULT_DIR_PERMS),
    Uuid = file_id:guid_to_uuid(Guid),
    {ok, DatasetId} = lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid}),
    ?assertMatch({ok, #dataset_attrs{
        id = DatasetId,
        uuid = Uuid
    }}, lfm_proxy:get_dataset_attrs(P1Node, UserSessIdP1, DatasetId)),
    ?assertMatch({ok, #file_details{
        eff_datasets = [DatasetId],
        is_dataset_attached = true
    }}, lfm_proxy:get_details(P1Node, UserSessIdP1, {guid, Guid})),

    lfm_proxy:detach_dataset(P1Node, UserSessIdP1, DatasetId),

    ?assertMatch({ok, #dataset_attrs{
        id = DatasetId,
        uuid = Uuid
    }}, lfm_proxy:get_dataset_attrs(P1Node, UserSessIdP1, DatasetId)),

    ?assertMatch({ok, #file_details{
        eff_datasets = [],
        is_dataset_attached = false
    }}, lfm_proxy:get_details(P1Node, UserSessIdP1, {guid, Guid})),

    ?assertMatch(ok, lfm_proxy:reattach_dataset(P1Node, UserSessIdP1, DatasetId)),

    ?assertMatch({ok, #file_details{
        eff_datasets = [DatasetId],
        is_dataset_attached = true
    }}, lfm_proxy:get_details(P1Node, UserSessIdP1, {guid, Guid})).

establish_dataset_on_not_existing_file_should_fail(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    ok = lfm_proxy:unlink(P1Node, UserSessIdP1, {guid, Guid}),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})).


establish_2nd_dataset_on_file_should_fail(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(P1Node, UserSessIdP1, SpaceGuid, FileName, ?DEFAULT_DIR_PERMS),
    establish_dataset_attached_to_generic_file_test_base(Guid),
    ?assertMatch({error, ?EEXIST}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})).


establish_nested_datasets_structure(_Config) ->
    Depth = 10,
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    SpaceId = oct_background:get_space_id(space1),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    DirNamePrefix = ?DIR_NAME,

    GuidsReversed = lists:foldl(fun(N, AccIn = [ParentGuid | _]) ->
        DirName = str_utils:join_binary([DirNamePrefix, integer_to_binary(N)]),
        {ok, Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ParentGuid, DirName, ?DEFAULT_DIR_PERMS),
        [Guid | AccIn]
    end, [SpaceGuid], lists:seq(1, Depth)),

    DatasetIds = lists:map(fun(Guid) ->
        ct:pal("Guid: ~p", [Guid]),
        {ok, DatasetId} = lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid}),
        DatasetId
    end, GuidsReversed),


    lists:foreach(fun({Guid, DatasetId}) ->

        ct:pal("D: ~p", [lfm_proxy:get_dataset_attrs(P1Node, UserSessIdP1, DatasetId)]),
        ct:pal("F: ~p", [lfm_proxy:get_details(P1Node, UserSessIdP1, {guid, Guid})])

    end, lists:zip(GuidsReversed, DatasetIds)).




%===================================================================
% Test bases
%===================================================================

establish_dataset_attached_to_generic_file_test_base(Guid) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    Uuid = file_id:guid_to_uuid(Guid),
    {ok, DatasetId} = ?assertMatch({ok, _}, lfm_proxy:establish_dataset(P1Node, UserSessIdP1, {guid, Guid})),
    ?assertMatch({ok, #dataset_attrs{
        id = DatasetId,
        uuid = Uuid
    }}, lfm_proxy:get_dataset_attrs(P1Node, UserSessIdP1, DatasetId)),
    ?assertMatch({ok, #file_details{
        eff_datasets = [DatasetId],
        is_dataset_attached = true
    }}, lfm_proxy:get_details(P1Node, UserSessIdP1, {guid, Guid})).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    oct_background:init_per_suite(Config, #onenv_test_config{onenv_scenario = "2op"}).

end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().

init_per_testcase(_Case, Config) ->
    % update background config to update sessions
    Config2 = oct_background:update_background_config(Config),
    lfm_proxy:init(Config2).

end_per_testcase(_Case, Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    PNodes = oct_background:get_all_providers_nodes(),
    SpaceId = oct_background:get_space_id(space1),
    test_utils:mock_unload(PNodes, [file_meta]),
    cleanup_datasets(P1Node, SpaceId),
    assert_all_dataset_links_are_deleted(PNodes, SpaceId),
    lfm_test_utils:clean_space(P1Node, PNodes, SpaceId, ?ATTEMPTS),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================


cleanup_datasets(Node, SpaceId) ->
    {ok, Datasets} = rpc:call(Node, dataset_links, list_all_unsafe, [SpaceId]),
    lists:foreach(fun({DatasetId, _}) ->
        ok = rpc:call(Node, dataset_api, remove, [DatasetId])
    end, Datasets).

assert_all_dataset_links_are_deleted(Nodes, SpaceId) ->
    lists:foreach(fun(N) ->
        ?assertMatch({ok, []}, rpc:call(N, dataset_links, list_all_unsafe, [SpaceId]), ?ATTEMPTS)
    end, Nodes).