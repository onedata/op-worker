%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016, ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Replication tests.
%%% @end
%%%-------------------------------------------------------------------
-module(replication_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/dbsync/common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").


%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([dbsync_trigger_should_create_local_file_location/1]).


-performance({test_cases, []}).
all() ->
    [
        dbsync_trigger_should_create_local_file_location
    ].



%%%===================================================================
%%% Test functions
%%%===================================================================

dbsync_trigger_should_create_local_file_location(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    UserId = <<"user_id1">>,
    CTime = utils:time(),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    FileMeta = #file_meta{
        mode = 8#777,
        name = <<"file">>,
        type = ?REGULAR_FILE_TYPE,
        mtime = CTime,
        atime = CTime,
        ctime = CTime,
        uid = UserId
    },
    {ok, FileUUID} = ?assertMatch({ok, _}, rpc:call(W1, file_meta, create, [{uuid, SpaceDirUuid}, FileMeta])),

    rpc:call(W1, dbsync_events, change_replicated, [SpaceId, #change{model = file_meta, doc = #document{key = FileUUID, value = FileMeta}}]),

    ?assertMatch({ok, [_]}, rpc:call(W1, file_meta, get_locations, [{uuid, FileUUID}])).



%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    application:start(ssl2),
    hackney:start(),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    tracer:start(Workers),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces(Config),
    hackney:stop(),
    application:stop(ssl2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

