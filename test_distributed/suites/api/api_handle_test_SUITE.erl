%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning handle basic API.
%%% @end
%%%-------------------------------------------------------------------
-module(api_handle_test_SUITE).
-author("Katarzyna Such").

-include("api_file_test_utils.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/privileges.hrl").

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
        create_handle_test
%%        get_handle_test,
%%        update_handle_test
    ]}
].

all() -> [
    {group, all_tests}
].

-define(add_identifier_to_metadata(PublicHandle), <<
    "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n",
    "<metadata>\n",
    "    <dc:identifier>", PublicHandle/binary, "</dc:identifier>\n"
    "    <dc:contributor>John Doe</dc:contributor>\n",
    "</metadata>"
>>).

-define(get_handle_data(HServiceId, ShareId), #{
    <<"handleServiceId">> => HServiceId,
    <<"resourceType">> => <<"Share">>,
    <<"resourceId">> => ShareId,
    <<"metadataPrefix">> => <<"oai_dc">>,
    <<"metadata">> => <<
        "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n",
        "<metadata>\n",
        "    <dc:contributor>John Doe</dc:contributor>\n",
        "</metadata>"
    >>
}).


%%%===================================================================
%%% Get file distribution test functions
%%%===================================================================


create_handle_test(_Config) ->
    SpaceId = oct_background:get_space_id(space_krk),
    UserId = oct_background:get_user_id(user1),
    ok = ozw_test_rpc:update_oz_privileges(UserId, [?OZ_HANDLES_CREATE | privileges:oz_viewer()], []),

    ShareId = ozw_test_rpc:create_share(UserId, SpaceId),
    HServiceId = hd(ozw_test_rpc:list_handle_services()),
    Data = ?get_handle_data(HServiceId, ShareId),

    ?assert(is_binary(ozw_test_rpc:create_handle(UserId, Data))).


get_handle_test(_Config) ->
    SpaceId = oct_background:get_space_id(space_krk),
    UserId = oct_background:get_user_id(user1),
    ok = ozw_test_rpc:update_oz_privileges(UserId, [?OZ_HANDLES_CREATE, ?OZ_HANDLES_VIEW | privileges:oz_viewer()], []),

    ShareId = ozw_test_rpc:create_share(UserId, SpaceId),
    HServiceId = hd(ozw_test_rpc:list_handle_services()),
    Data = ?get_handle_data(HServiceId, ShareId),
    HandleId = ozw_test_rpc:create_handle(UserId, Data),
    {
        od_handle, PublicHandle, ResourceType, ActualMetadataPrefix, ActualMetadata,
        _, ActualResourceId, ActualHServiceId, _, _, _, _, _, _, _
    } = ozw_test_rpc:get_handle(UserId, HandleId),

    ExpectedMetadata = ?add_identifier_to_metadata(PublicHandle),
    ?assertEqual(
        {<<"Share">>, <<"oai_dc">>, ExpectedMetadata, ShareId, HServiceId},
        {ResourceType, ActualMetadataPrefix, ActualMetadata, ActualResourceId, ActualHServiceId}
    ).


update_handle_test(_Config) ->
    SpaceId = oct_background:get_space_id(space_krk),
    UserId = oct_background:get_user_id(user1),

    ShareId = ozw_test_rpc:create_share(UserId, SpaceId),
    HServiceId = hd(ozw_test_rpc:list_handle_services()),
    Data = ?get_handle_data(HServiceId, ShareId),
    HandleId = ozw_test_rpc:create_handle(Data),
    NewMetadata = <<
        "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n",
        "<metadata>\n",
        "    <dc:contributor>Jane Doe</dc:contributor>\n",
        "</metadata>"
    >>,
    ?assertEqual(ok, ozw_test_rpc:update_handle(HandleId, NewMetadata)),
    {
        od_handle, PublicHandle, _, _, ActualMetadata, _, _, _, _, _, _, _, _, _, _
    } = ozw_test_rpc:get_handle(HandleId),

    NewMetadataPublicHandle = <<
        "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n",
        "<metadata>\n",
        "    <dc:identifier>", PublicHandle/binary, "</dc:identifier>\n",
        "    <dc:contributor>Jane Doe</dc:contributor>\n",
        "</metadata>"
    >>,
    ct:pal("ActualMetadata ~p~n", [ActualMetadata]),
    ?assertEqual(NewMetadataPublicHandle, ActualMetadata).


%%%===================================================================
%%% Common handle test utils
%%%===================================================================



%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op-handle-proxy"
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    Config.


end_per_group(_Group, _Config) ->
    ok.


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, Config) ->
    Config.
