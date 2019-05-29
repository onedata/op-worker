%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module tests mechanism for harvesting metadata
%%% @end
%%%-------------------------------------------------------------------
-module(harvest_test_SUITE).
% todo rename to harvesting_test_SUITE, remember to rename job on bamboo !!!
-author("Jakub Kudzia").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    set_json_metadata_test/1,
    modify_json_metadata_test/1,
    delete_json_metadata_test/1,
    delete_file_with_json_metadata_test/1,
    modify_json_many_times/1,
    changes_should_be_submitted_to_all_harvesters_subscribed_for_the_space/1
%%    changes_should_be_submitted_to_all_indices_subscribed_for_the_space/1,
%%    changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester/1,
%%    each_provider_should_submit_only_local_changes_to_the_harvester/1,
%%    each_provider_should_submit_only_local_changes_to_the_harvester_deletion_test/1,
%%    submit_entry_failure_test/1,
%%    delete_entry_failure_test/1
]).

all() ->
    ?ALL([
        set_json_metadata_test,
        modify_json_metadata_test,
        delete_json_metadata_test,
        delete_file_with_json_metadata_test,
        modify_json_many_times,
        changes_should_be_submitted_to_all_harvesters_subscribed_for_the_space
%%        ,
%%        changes_should_be_submitted_to_all_indices_subscribed_for_the_space,
%%        changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester,
%%        each_provider_should_submit_only_local_changes_to_the_harvester,
%%        each_provider_should_submit_only_local_changes_to_the_harvester_deletion_test,
%%        submit_entry_failure_test,
%%        delete_entry_failure_test
    ]).

-define(SPACE_ID1, <<"space_id1">>).
-define(SPACE_ID2, <<"space_id2">>).
-define(SPACE_ID3, <<"space_id3">>).
-define(SPACE_ID4, <<"space_id4">>).
-define(SPACE_ID5, <<"space_id5">>).

-define(SPACE_NAME1, <<"space1">>).
-define(SPACE_NAME2, <<"space2">>).
-define(SPACE_NAME3, <<"space3">>).
-define(SPACE_NAME4, <<"space4">>).
-define(SPACE_NAME5, <<"space5">>).

-define(SPACE_NAMES, #{
    ?SPACE_ID1 => ?SPACE_NAME1
%%    ,
%%    ?SPACE_ID2 => ?SPACE_NAME2,
%%    ?SPACE_ID3 => ?SPACE_NAME3,
%%    ?SPACE_ID4 => ?SPACE_NAME4,
%%    ?SPACE_ID5 => ?SPACE_NAME5
}).

-define(SPACE_NAME(__SpaceId), maps:get(__SpaceId, ?SPACE_NAMES)).
-define(SPACE_IDS, maps:keys(?SPACE_NAMES)).

-define(PATH(FileName, SpaceId), filename:join(["/", ?SPACE_NAME(SpaceId), FileName])).

-define(HARVESTER1, <<"harvester1">>).
-define(HARVESTER2, <<"harvester2">>).
-define(HARVESTER3, <<"harvester3">>).

-define(USER_ID, <<"user1">>).
-define(SESS_ID(Worker),
    ?config({session_id, {?USER_ID, ?GET_DOMAIN(Worker)}}, Config)).

-define(XATTR_NAME, <<"xattr_name_", (?RAND_NAME)/binary>>).
-define(XATTR_VAL, <<"xattr_val_", (?RAND_NAME)/binary>>).

-define(FILE_NAME, <<"file_", (?RAND_NAME)/binary>>).

-define(RAND_NAME,
    <<(str_utils:to_binary(?FUNCTION))/binary, "_", (integer_to_binary(rand:uniform(?RAND_RANGE)))/binary>>).

-define(RAND_RANGE, 1000000000).
-define(ATTEMPTS, 60).
-define(TIMEOUT, timer:seconds(?ATTEMPTS)).

-define(PROVIDER_ID(Node), rpc:call(Node, oneprovider, get_id, [])).

%% Test config:
%% space_id1:
%%  * supported by: p1
%%  * harvesters: harvester1
%%  * indices: index1
%% space_id2:
%%  * supported by: p1
%%  * harvesters: harvester1, harvester2
%% space_id3:
%%  * supported by: p1
%%  * harvesters: harvester1
%% space_id4:
%%  * supported by: p1
%%  * harvesters: harvester1
%% space_id5:
%%  * supported by: p1, p2
%%  * harvesters: harvester3

-define(HARVEST_METADATA, harvest_metadata).
-define(HARVEST_METADATA(SpaceId, Destination, Batch, ExpProviderId),
    {?HARVEST_METADATA, SpaceId, Destination, Batch, ExpProviderId}).

%%% todo remove
%%-define(HARVEST_METADATA(SpaceId, Destination, Batch, MaxSeq, ExpProviderId),
%%    {?HARVEST_METADATA, SpaceId, Destination, Batch, ExpProviderId}).

-define(assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId),
    ?assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, ?TIMEOUT)).
-define(assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, Timeout),
    ?assertReceivedMatch(?HARVEST_METADATA(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId), Timeout)).


%todo remove
%%-define(assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpIndices, ExpProviderId),
%%    ?assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpIndices, ExpProviderId, ?TIMEOUT)).
%%-define(assertReceivedHarvestMetadata(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId, Timeout),
%%    ?assertReceivedEqual(?HARVEST_METADATA(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId), Timeout)).

-define(assertNotReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId),
    ?assertNotReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, ?TIMEOUT)).
-define(assertNotReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, Timeout),
    ?assertNotReceivedMatch(?HARVEST_METADATA(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId), Timeout)).


%%-define(assertNotReceivedHarvestMetadata(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId),
%%    ?assertNotReceivedHarvestMetadata(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId, ?TIMEOUT)).
%%-define(assertNotReceivedHarvestMetadata(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId, Timeout),
%%    ?assertNotReceivedMatch(?HARVEST_METADATA(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId), Timeout)).

-define(DELETE_ENTRY, delete_entry).
-define(DELETE_ENTRY(FileId, Harvester, ExpIndices, ExpProviderId),
    {?DELETE_ENTRY, FileId, Harvester, ExpIndices, ExpProviderId}).

-define(assertReceivedDeleteEntry(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId),
    ?assertReceivedDeleteEntry(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId, ?TIMEOUT)).
-define(assertReceivedDeleteEntry(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId, Timeout),
    ?assertReceivedEqual(?DELETE_ENTRY(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId), Timeout)).

-define(assertNotReceivedDeleteEntry(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId),
    ?assertNotReceivedDeleteEntry(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId, ?TIMEOUT)).
-define(assertNotReceivedDeleteEntry(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId, Timeout),
    ?assertNotReceivedEqual(?DELETE_ENTRY(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId), Timeout)).


-define(INDEX(N), <<"index", (integer_to_binary(N))/binary>>).
-define(INDEX11, <<"index11">>).
-define(INDEX21, <<"index21">>).
-define(INDEX22, <<"index22">>).
-define(INDEX23, <<"index23">>).
-define(INDEX31, <<"index31">>).

-define(DESTINATION1, #{
    ?HARVESTER1 => [?INDEX11]
}).
-define(DESTINATION2, #{
    ?HARVESTER1 => [?INDEX11],
    ?HARVESTER2 => [?INDEX21, ?INDEX22, ?INDEX23]
}).

-define(MOCK_HARVEST_METADATA_FAILURE, mock_harvester_logic_failure).

%%%====================================================================
%%% Test function
%%%====================================================================

set_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = ?DESTINATION1,
    EncodedJSON = json_utils:encode(JSON),
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> := FileId,
        <<"operation">> := <<"submit">>,
        <<"payload">> := #{
            <<"json">> := EncodedJSON
        }
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> := FileId,
        <<"operation">> := <<"submit">>,
        <<"payload">> := #{
            <<"json">> := EncodedJSON
        }
    }], ProviderId).

modify_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = ?DESTINATION1,
    EncodedJSON = json_utils:encode(JSON),
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> := FileId,
        <<"operation">> := <<"submit">>,
        <<"payload">> := #{
            <<"json">> := EncodedJSON
        }
    }], ProviderId),

    JSON2 = #{<<"color">> => <<"blue">>, <<"size">> => <<"big">>},
    EncodedJSON2 = json_utils:encode(JSON2),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON2, []),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> := FileId,
        <<"operation">> := <<"submit">>,
        <<"payload">> := #{
            <<"json">> := EncodedJSON2
        }
    }], ProviderId).

delete_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = ?DESTINATION1,
    EncodedJSON = json_utils:encode(JSON),
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> := FileId,
        <<"operation">> := <<"submit">>,
        <<"payload">> := #{
            <<"json">> := EncodedJSON
        }
    }], ProviderId),

    ok = lfm_proxy:remove_metadata(Worker, SessId, {guid, Guid}, json),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> := FileId,
        <<"operation">> := <<"delete">>
    }], ProviderId).


delete_file_with_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = ?DESTINATION1,
    EncodedJSON = json_utils:encode(JSON),
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> := FileId,
        <<"operation">> := <<"submit">>,
        <<"payload">> := #{
            <<"json">> := EncodedJSON
        }
    }], ProviderId),

    ok = lfm_proxy:unlink(Worker, SessId, {guid, Guid}),
    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> := FileId,
        <<"operation">> := <<"delete">>
    }], ProviderId).

modify_json_many_times(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    Modifications = 10000,
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ct:pal("Start"),

    ExpectedFinalJSON = lists:foldl(fun(I, _) ->
        Key = <<"key_", (integer_to_binary(I))/binary>>,
        Value = <<"value_", (integer_to_binary(I))/binary>>,
        JSON = #{Key => Value},
        ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
        JSON
    end, undefined, lists:seq(1, Modifications)),

    ct:pal("Finished"),

    Destination = ?DESTINATION1,
    EncodedJSON = json_utils:encode(ExpectedFinalJSON),
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> := FileId,
        <<"operation">> := <<"submit">>,
        <<"payload">> := #{
            <<"json">> := EncodedJSON
        }
    }], ProviderId).

changes_should_be_submitted_to_all_harvesters_subscribed_for_the_space(Config) ->
    % ?HARVESTER1 and ?HARVESTER2 are subscribed for ?SPACE_ID2
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,

    JSON1 = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID2), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = ?DESTINATION2,
    EncodedJSON = json_utils:encode(JSON1),
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> := FileId,
        <<"operation">> := <<"submit">>,
        <<"payload">> := #{
            <<"json">> := EncodedJSON
        }
    }], ProviderId).


%%changes_should_be_submitted_to_all_indices_subscribed_for_the_space(Config) ->
%%    % ?HARVESTER1 and ?HARVESTER2 are subscribed for ?SPACE_ID2
%%    [Worker | _] = ?config(op_worker_nodes, Config),
%%    SessId = ?SESS_ID(Worker),
%%    FileName = ?FILE_NAME,
%%
%%    JSON1 = #{<<"color">> => <<"blue">>},
%%
%%    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID2), 8#600),
%%    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),
%%    {ok, FileId} = file_id:guid_to_objectid(Guid),
%%
%%    ?assertReceivedHarvestMetadata(FileId, ?HARVESTER2, JSON1, [?INDEX21], ?PROVIDER_ID(Worker)),
%%    ?assertReceivedHarvestMetadata(FileId, ?HARVESTER2, JSON1, [?INDEX22], ?PROVIDER_ID(Worker)),
%%    ?assertReceivedHarvestMetadata(FileId, ?HARVESTER2, JSON1, [?INDEX23], ?PROVIDER_ID(Worker)).
%%
%%changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester(Config) ->
%%    % ?HARVESTER1 is subscribed for ?SPACE_ID3 and ?SPACE_ID4
%%    [Worker | _] = ?config(op_worker_nodes, Config),
%%    SessId = ?SESS_ID(Worker),
%%
%%    FileName = ?FILE_NAME,
%%    JSON1 = #{<<"color">> => <<"blue">>},
%%
%%    FileName2 = ?FILE_NAME,
%%    JSON2 = #{<<"color">> => <<"red">>},
%%
%%    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID3), 8#600),
%%    {ok, FileId} = file_id:guid_to_objectid(Guid),
%%    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),
%%
%%    {ok, Guid2} = lfm_proxy:create(Worker, SessId, ?PATH(FileName2, ?SPACE_ID4), 8#600),
%%    {ok, FileId2} = file_id:guid_to_objectid(Guid2),
%%    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid2}, json, JSON2, []),
%%
%%    ?assertReceivedHarvestMetadata(FileId, ?HARVESTER1, JSON1, [?INDEX11], ?PROVIDER_ID(Worker)),
%%    ?assertReceivedHarvestMetadata(FileId2, ?HARVESTER1, JSON2, [?INDEX11], ?PROVIDER_ID(Worker)).
%%
%%each_provider_should_submit_only_local_changes_to_the_harvester(Config) ->
%%    % ?HARVESTER3 is subscribed for ?SPACE_ID5 which is supported by both providers
%%    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
%%    SessId = ?SESS_ID(WorkerP1),
%%    SessId2 = ?SESS_ID(WorkerP2),
%%    ProviderId1 = ?PROVIDER_ID(WorkerP1),
%%    ProviderId2 = ?PROVIDER_ID(WorkerP2),
%%
%%    FileName = ?FILE_NAME,
%%    JSON1 = #{<<"color">> => <<"blue">>},
%%
%%    FileName2 = ?FILE_NAME,
%%    JSON2 = #{<<"color">> => <<"red">>},
%%
%%    {ok, Guid} = lfm_proxy:create(WorkerP1, SessId, ?PATH(FileName, ?SPACE_ID5), 8#600),
%%    {ok, FileId} = file_id:guid_to_objectid(Guid),
%%    ok = lfm_proxy:set_metadata(WorkerP1, SessId, {guid, Guid}, json, JSON1, []),
%%
%%    {ok, Guid2} = lfm_proxy:create(WorkerP2, SessId2, ?PATH(FileName2, ?SPACE_ID5), 8#600),
%%    {ok, FileId2} = file_id:guid_to_objectid(Guid2),
%%    ok = lfm_proxy:set_metadata(WorkerP2, SessId2, {guid, Guid2}, json, JSON2, []),
%%
%%    ?assertReceivedHarvestMetadata(FileId, ?HARVESTER3, JSON1, [?INDEX31], ProviderId1),
%%    ?assertReceivedHarvestMetadata(FileId2, ?HARVESTER3, JSON2, [?INDEX31], ProviderId2),
%%
%%    % calls to harvester_logic:create entry should not be duplicated
%%    ?assertNotReceivedHarvestMetadata(FileId, ?HARVESTER3, _, _, ProviderId1),
%%    ?assertNotReceivedHarvestMetadata(FileId2, ?HARVESTER3, _, _, ProviderId2).
%%
%%each_provider_should_submit_only_local_changes_to_the_harvester_deletion_test(Config) ->
%%    % ?HARVESTER3 is subscribed for ?SPACE_ID5 which is supported by both providers
%%    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
%%    SessId = ?SESS_ID(WorkerP1),
%%    SessId2 = ?SESS_ID(WorkerP2),
%%    ProviderId1 = ?PROVIDER_ID(WorkerP1),
%%    ProviderId2 = ?PROVIDER_ID(WorkerP2),
%%
%%    FileName = ?FILE_NAME,
%%    JSON1 = #{<<"color">> => <<"blue">>},
%%
%%    FileName2 = ?FILE_NAME,
%%    JSON2 = #{<<"color">> => <<"red">>},
%%
%%    {ok, Guid} = lfm_proxy:create(WorkerP1, SessId, ?PATH(FileName, ?SPACE_ID5), 8#600),
%%    {ok, FileId} = file_id:guid_to_objectid(Guid),
%%    ok = lfm_proxy:set_metadata(WorkerP1, SessId, {guid, Guid}, json, JSON1, []),
%%
%%    {ok, Guid2} = lfm_proxy:create(WorkerP2, SessId2, ?PATH(FileName2, ?SPACE_ID5), 8#600),
%%    {ok, FileId2} = file_id:guid_to_objectid(Guid2),
%%    ok = lfm_proxy:set_metadata(WorkerP2, SessId2, {guid, Guid2}, json, JSON2, []),
%%
%%    ?assertReceivedHarvestMetadata(FileId, ?HARVESTER3, JSON1, [?INDEX31], ProviderId1),
%%    ?assertReceivedHarvestMetadata(FileId2, ?HARVESTER3, JSON2, [?INDEX31], ProviderId2),
%%
%%    % calls to harvester_logic:create entry should not be duplicated
%%    ?assertNotReceivedHarvestMetadata(FileId, ?HARVESTER3, _, _, ProviderId1),
%%    ?assertNotReceivedHarvestMetadata(FileId2, ?HARVESTER3, _, _, ProviderId2),
%%
%%    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessId, {guid, Guid2}), ?ATTEMPTS),
%%    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP2, SessId2, {guid, Guid}), ?ATTEMPTS),
%%
%%    ok = lfm_proxy:unlink(WorkerP1, SessId, {guid, Guid2}),
%%    ok = lfm_proxy:unlink(WorkerP2, SessId2, {guid, Guid}),
%%
%%    ?assertReceivedDeleteEntry(FileId2, ?HARVESTER3, [?INDEX31], ProviderId1),
%%    ?assertReceivedDeleteEntry(FileId, ?HARVESTER3, [?INDEX31], ProviderId2),
%%
%%    % calls to harvester_logic:delete entry should not be duplicated
%%    ?assertNotReceivedDeleteEntry(FileId, ?HARVESTER3, [?INDEX31], ProviderId1),
%%    ?assertNotReceivedDeleteEntry(FileId2, ?HARVESTER3, [?INDEX31], ProviderId2).
%%
%%submit_entry_failure_test(Config) ->
%%    [Worker | _] = ?config(op_worker_nodes, Config),
%%    SessId = ?SESS_ID(Worker),
%%
%%    FileName = ?FILE_NAME,
%%    FileName2 = ?FILE_NAME,
%%    JSON1 = #{<<"color">> => <<"blue">>},
%%    ProviderId1 = ?PROVIDER_ID(Worker),
%%
%%    HSPid1 = get_harvesting_stream_pid(Worker, ?HARVESTER1, ?SPACE_ID1, ?INDEX11),
%%
%%    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
%%    {ok, FileId} = file_id:guid_to_objectid(Guid),
%%    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),
%%
%%    ?assertReceivedHarvestMetadata(FileId, ?HARVESTER1, JSON1, [?INDEX11], ProviderId1),
%%
%%    set_mock_harvest_metadata_failure(Worker, true),
%%
%%    JSON2 = #{<<"color">> => <<"red">>},
%%    JSON3 = #{<<"color">> => <<"green">>},
%%
%%    {ok, Guid2} = lfm_proxy:create(Worker, SessId, ?PATH(FileName2, ?SPACE_ID1), 8#600),
%%    {ok, FileId2} = file_id:guid_to_objectid(Guid2),
%%
%%    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON2, []),
%%    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid2}, json, JSON3, []),
%%
%%    % changes should not be submitted as connection to onezone failed
%%    ?assertNotReceivedHarvestMetadata(FileId, ?HARVESTER1, JSON2, _, ProviderId1),
%%    ?assertNotReceivedHarvestMetadata(FileId2, ?HARVESTER1, JSON3, _, ProviderId1),
%%
%%    set_mock_harvest_metadata_failure(Worker, false),
%%
%%    % harvesting_stream should not have been restarted
%%    ?assertEqual(HSPid1,
%%        get_harvesting_stream_pid(Worker, ?HARVESTER1, ?SPACE_ID1, ?INDEX11)),
%%
%%    % previously sent change should not be submitted
%%    ?assertNotReceivedHarvestMetadata(FileId, ?HARVESTER1, JSON1, _, ProviderId1),
%%
%%    % missing changes should be submitted
%%    ?assertReceivedHarvestMetadata(FileId, ?HARVESTER1, JSON2, [?INDEX11], ProviderId1),
%%    ?assertReceivedHarvestMetadata(FileId2, ?HARVESTER1, JSON3, [?INDEX11], ProviderId1).
%%
%%delete_entry_failure_test(Config) ->
%%    [Worker | _] = ?config(op_worker_nodes, Config),
%%    SessId = ?SESS_ID(Worker),
%%
%%    FileName = ?FILE_NAME,
%%    JSON1 = #{<<"color">> => <<"blue">>},
%%    ProviderId1 = ?PROVIDER_ID(Worker),
%%
%%    HSPid1 = get_harvesting_stream_pid(Worker, ?HARVESTER1, ?SPACE_ID1, ?INDEX11),
%%
%%    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
%%    {ok, FileId} = file_id:guid_to_objectid(Guid),
%%    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),
%%
%%    ?assertReceivedHarvestMetadata(FileId, ?HARVESTER1, JSON1, [?INDEX11], ProviderId1),
%%
%%    set_mock_harvest_metadata_failure(Worker, true),
%%
%%    ok = lfm_proxy:unlink(Worker, SessId, {guid, Guid}),
%%
%%    % change should not be submitted as connection to onezone failed
%%    ?assertNotReceivedDeleteEntry(FileId, ?HARVESTER1, [?INDEX11], ProviderId1),
%%
%%    set_mock_harvest_metadata_failure(Worker, false),
%%
%%    % harvesting_stream should not have been restarted
%%    ?assertEqual(HSPid1,
%%        get_harvesting_stream_pid(Worker, ?HARVESTER1, ?SPACE_ID1, ?INDEX11)),
%%
%%    % previously sent change should not be submitted
%%    ?assertNotReceivedHarvestMetadata(FileId, ?HARVESTER1, JSON1, _, ProviderId1),
%%
%%    % missing changes should be submitted
%%    ?assertReceivedDeleteEntry(FileId, ?HARVESTER1, [?INDEX11], ProviderId1).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(_Case, Config) ->
    Config2 = sort_workers(Config),
    Workers = ?config(op_worker_nodes, Config2),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config2, "env_desc.json"), Config2),

    lists:foreach(fun(W) ->
        lists:foreach(fun(SpaceId) ->
            {ok, D} = rpc:call(W, space_logic, get, [?ROOT_SESS_ID, SpaceId]),
            {ok, Harvesters} = space_logic:get_harvesters(D),
            lists:foreach(fun(HarvesterId) ->
                {ok, HD} = rpc:call(W, harvester_logic, get, [HarvesterId]),
                rpc:call(W, od_harvester, save_to_cache, [HD])
            end, Harvesters),
            % trigger od_provider posthooks
            rpc:call(W, od_space, save_to_cache, [D])
            end, ?SPACE_IDS)
        end, Workers),
    set_mock_harvest_metadata_failure(Workers, false),
    mock_space_logic_harvest_metadata(Workers),
%%    mock_harvester_logic_delete_entry(Workers),
    mock_space_quota_checks(Workers),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, space_logic),
    lists:foreach(fun(W) ->
        SupervisorPid = whereis(W, harvesting_stream_sup),
        exit(SupervisorPid, kill)
    end, Workers),
    cleanup_harvesting_model(Workers),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

whereis(Node, Name) ->
    rpc:call(Node, erlang, whereis, [Name]).

mock_space_logic_harvest_metadata(Node) ->
    Self = self(),
    ok = test_utils:mock_expect(Node, space_logic, harvest_metadata,
        fun(SpaceId, Destination, Batch, _MaxStreamSeq, _MaxSeq) ->
            case application:get_env(op_worker, ?MOCK_HARVEST_METADATA_FAILURE, false) of
                true ->
                    {error, test_error};
                false ->
                    Self ! ?HARVEST_METADATA(SpaceId, Destination, Batch, oneprovider:get_id()),
                    {ok, #{}}
            end
        end
    ).

set_mock_harvest_metadata_failure(Nodes, Boolean) ->
    test_utils:set_env(Nodes, op_worker, ?MOCK_HARVEST_METADATA_FAILURE, Boolean).

mock_space_quota_checks(Node) ->
    % mock space_quota to mock error logs due to some test environment issues
    ok = test_utils:mock_new(Node, space_quota),
    ok = test_utils:mock_expect(Node, space_quota, get_disabled_spaces, fun() -> {ok, []} end).

cleanup_harvesting_model(Nodes) ->
    lists:foreach(fun(Node) ->
        lists:foreach(fun(SpaceId) ->
            rpc:call(Node, harvesting, delete, [SpaceId])
        end, ?SPACE_IDS)
    end, Nodes).

sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).

which_children(Node, SupRef) ->
    rpc:call(Node, supervisor, which_children, [SupRef]).

get_harvesting_stream_pid(Node, HarvesterId, SpaceId, IndexId) ->
    {_, HSPid, _, _} = ?assertMatch({_, _, _, _},
        lists:keyfind({HarvesterId, SpaceId, IndexId}, 1, which_children(Node, harvesting_stream_sup)),
        ?ATTEMPTS),
    HSPid.