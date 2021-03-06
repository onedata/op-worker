%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module tests mechanism of harvesting metadata in multi-provider
%%% environment.
%%% NOTE !!!
%%% Test cases in this suite are not independent as it is impossible to
%%% reset couchbase_changes_stream.
%%% `harvesting model` is not cleaned between test cases.
%%% Because of that, harvesting in next testcase starts from the sequence
%%% on which harvesting in previous test case stopped.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_test_SUITE).
-author("Jakub Kudzia").

-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/harvesting/harvesting.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/http/codes.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    harvest_space_doc_and_do_not_harvest_trash_doc/1,
    create_file/1,
    create_file_and_hardlink/1,
    create_dir_and_symlink/1,
    create_file_with_dataset/1,
    rename_file/1,
    delete_file/1,
    set_json_metadata/1,
    modify_json_metadata/1,
    delete_json_metadata/1,
    delete_file_with_json_metadata/1,
    modify_json_many_times/1,
    set_rdf_metadata/1,
    modify_rdf_metadata/1,
    delete_rdf_metadata/1,
    delete_file_with_rdf_metadata/1,
    modify_rdf_many_times/1,
    set_xattr_metadata/1,
    cdmi_xattr_should_not_be_harvested/1,
    modify_xattr_metadata/1,
    delete_xattr_metadata/1,
    delete_file_with_xattr_metadata/1,
    modify_xattr_many_times/1,
    modify_metadata_and_rename_file/1,
    changes_should_be_submitted_to_all_harvesters_and_indices_subscribed_for_the_space/1,
    changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester/1,
    each_provider_should_submit_only_local_changes_to_the_harvester/1,
    each_provider_should_submit_only_local_changes_to_the_harvester2/1,
    submit_entry_failure/1,
    delete_entry_failure/1
]).

all() ->
    ?ALL([
        harvest_space_doc_and_do_not_harvest_trash_doc,
        create_file,
        create_file_and_hardlink,
        create_dir_and_symlink,
        create_file_with_dataset,
        rename_file,
        delete_file,
        set_json_metadata,
        modify_json_metadata,
        delete_json_metadata,
        delete_file_with_json_metadata,
        modify_json_many_times,
        set_rdf_metadata,
        modify_rdf_metadata,
        delete_rdf_metadata,
        delete_file_with_rdf_metadata,
        modify_rdf_many_times,
        set_xattr_metadata,
        cdmi_xattr_should_not_be_harvested,
        modify_xattr_metadata,
        delete_xattr_metadata,
        delete_file_with_xattr_metadata,
        modify_xattr_many_times,
        modify_metadata_and_rename_file,
        changes_should_be_submitted_to_all_harvesters_and_indices_subscribed_for_the_space,
        changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester,
        each_provider_should_submit_only_local_changes_to_the_harvester,
        each_provider_should_submit_only_local_changes_to_the_harvester2,
        submit_entry_failure,
        delete_entry_failure
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
    ?SPACE_ID1 => ?SPACE_NAME1,
    ?SPACE_ID2 => ?SPACE_NAME2,
    ?SPACE_ID3 => ?SPACE_NAME3,
    ?SPACE_ID4 => ?SPACE_NAME4,
    ?SPACE_ID5 => ?SPACE_NAME5
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

-define(DIR_NAME, <<"dir", (?RAND_NAME)/binary>>).
-define(FILE_NAME, <<"file_", (?RAND_NAME)/binary>>).

-define(RAND_NAME,
    <<(str_utils:to_binary(?FUNCTION))/binary, "_", (integer_to_binary(rand:uniform(?RAND_RANGE)))/binary>>).

-define(RAND_RANGE, 1000000000).
-define(ATTEMPTS, 30).

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

%% NOTE!!!
%% This assert assumes that list ExpBatch is sorted in the order of increasing sequences
-define(assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId),
    ?assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, ?ATTEMPTS)).
-define(assertReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, Timeout),
    (
        (fun
            AssertFun(__SpaceId, __Destination, [], __Unexpected, __ProviderId, __Timeout) ->
                % all expected changes has been received
                % resend these entries that were unexpected
                self() ! ?HARVEST_METADATA(__SpaceId, __Destination, __Unexpected, __ProviderId),
                ok;
            AssertFun(__SpaceId, __Destination, __Batch, __Unexpected, __ProviderId, __Timeout) ->
                __TimeoutInMillis = timer:seconds(__Timeout),
                receive
                    ?HARVEST_METADATA(
                        __SpaceId,
                        __Destination,
                        __ReceivedBatch,
                        __ProviderId
                    ) ->
                        {__ExpBatchLeft, __NewUnexpected} = subtract_batches(__Batch, __ReceivedBatch),
                        AssertFun(__SpaceId, __Destination, __ExpBatchLeft, __Unexpected ++ __NewUnexpected,
                            __ProviderId, __Timeout)
                after
                    __TimeoutInMillis ->
                        __Args = [{module, ?MODULE},
                            {line, ?LINE},
                            {expected, {__SpaceId, __Destination, __Batch, __ProviderId, __Timeout}},
                            {value, timeout}],
                        ct:print("assertReceivedHarvestMetadata_failed: ~p~n", [__Args]),
                        erlang:error({assertReceivedHarvestMetadata_failed, __Args})
                end
        end)(ExpSpaceId, ExpDestination, ExpBatch, [], ExpProviderId, Timeout)
    )).

-define(assertNotReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId),
    ?assertNotReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, ?ATTEMPTS)).
-define(assertNotReceivedHarvestMetadata(ExpSpaceId, ExpDestination, ExpBatch, ExpProviderId, Timeout),
    (
        (fun AssertFun(__SpaceId, __Destination, __Batch, __Unexpected, __ProviderId, __Timeout) ->
            Stopwatch = stopwatch:start(),
            __TimeoutInMillis = timer:seconds(__Timeout),
            receive
                __HM = ?HARVEST_METADATA(
                    __SpaceId,
                    __Destination,
                    __ReceivedBatch,
                    __ProviderId
                ) ->
                    ElapsedTime = stopwatch:read_seconds(Stopwatch),
                    {__Batch2, __NewUnexpected} = subtract_batches(__Batch, __ReceivedBatch),
                    case length(__Batch2) < length(__Batch) of
                        false ->
                            AssertFun(__SpaceId, __Destination, __Batch, __Unexpected ++ __NewUnexpected,
                                __ProviderId, max(__Timeout - ElapsedTime, 0));
                        true ->
                            % __Batch2 is smaller than __Batch which means that one of changes, which was
                            % expected not to occur, actually occurred
                            __Args = [
                                {module, ?MODULE},
                                {line, ?LINE}
                            ],
                            ct:print("assertNotReceivedHarvestMetadata_failed: ~lp~n"
                                "Unexpectedly received: ~p~n", [__Args, __HM]),
                            erlang:error({assertNotReceivedHarvestMetadata_failed, __Args})
                    end
            after
                __TimeoutInMillis ->
                    % resend these entries that were unexpected
                    self() ! ?HARVEST_METADATA(__SpaceId, __Destination, __Unexpected, __ProviderId),
                    ok
            end
        end)(ExpSpaceId, ExpDestination, ExpBatch, [], ExpProviderId, Timeout)
    )).

-define(INDEX(N), <<"index", (integer_to_binary(N))/binary>>).
-define(INDEX11, <<"index11">>).
-define(INDEX21, <<"index21">>).
-define(INDEX22, <<"index22">>).
-define(INDEX23, <<"index23">>).
-define(INDEX31, <<"index31">>).

-define(DUMMY_RDF, <<"dummy rdf">>).
-define(DUMMY_RDF(Content),
    <<(?DUMMY_RDF)/binary, "_", (str_utils:to_binary(Content))/binary>>).

-define(MOCK_HARVEST_METADATA_FAILURE, mock_harvest_metadata_failure).

%%%====================================================================
%%% Test function
%%%====================================================================

harvest_space_doc_and_do_not_harvest_trash_doc(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),

    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID1),
    TrashGuid = fslogic_uuid:spaceid_to_trash_dir_guid(?SPACE_ID1),
    {ok, SpaceFileId} = file_id:guid_to_objectid(SpaceGuid),
    {ok, TrashFileId} = file_id:guid_to_objectid(TrashGuid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => SpaceFileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => ?SPACE_NAME1,
        <<"fileType">> => str_utils:to_binary(?DIRECTORY_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId),
    
    % trash doc should not be harvested
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => TrashFileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => ?TRASH_DIR_NAME,
        <<"fileType">> => str_utils:to_binary(?DIRECTORY_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => SpaceFileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => ?SPACE_NAME1,
        <<"fileType">> => str_utils:to_binary(?DIRECTORY_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId2).

create_file(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId2).


create_file_with_dataset(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    {ok, DatasetId} = lfm_proxy:establish_dataset(Worker, SessId, ?FILE_REF(Guid)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{},
        <<"datasetId">> => DatasetId
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{},
        <<"datasetId">> => DatasetId
    }], ProviderId2),

    ok = lfm_proxy:update_dataset(Worker, SessId, DatasetId, ?DETACHED_DATASET, ?no_flags_mask, ?no_flags_mask),

    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{},
        <<"datasetId">> => DatasetId
    }], ProviderId),

    % detaching dataset should result in entry without datasetId
    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId),

    % reattach dataset
    ok = lfm_proxy:update_dataset(Worker, SessId, DatasetId, ?ATTACHED_DATASET, ?no_flags_mask, ?no_flags_mask),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{},
        <<"datasetId">> => DatasetId
    }], ProviderId),

    % remove dataset
    ok = lfm_proxy:remove_dataset(Worker, SessId, DatasetId),

    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{},
        <<"datasetId">> => DatasetId
    }], ProviderId),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId).


create_file_and_hardlink(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    HardlinkName = <<"hardlink">>,

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    {ok, #file_attr{guid = HardlinkGuid}} =
        lfm_proxy:make_link(Worker, SessId, ?PATH(HardlinkName, ?SPACE_ID1), Guid),
    {ok, FileId} = file_id:guid_to_objectid(Guid),
    {ok, LinkFileId} = file_id:guid_to_objectid(HardlinkGuid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => LinkFileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => HardlinkName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId2),

    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => LinkFileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => HardlinkName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId2).

create_dir_and_symlink(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    DirName = ?DIR_NAME,
    SymlinkName = <<"symlink">>,

    {ok, Guid} = lfm_proxy:mkdir(Worker, SessId, ?PATH(DirName, ?SPACE_ID1)),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(?SPACE_ID1),
    LinkTarget = filename:join([SpaceIdPrefix, DirName]),
    {ok, #file_attr{guid = SymlinkGuid}} =
        lfm_proxy:make_symlink(Worker, SessId, ?PATH(SymlinkName, ?SPACE_ID1), LinkTarget),
    {ok, FileId} = file_id:guid_to_objectid(Guid),
    {ok, LinkFileId} = file_id:guid_to_objectid(SymlinkGuid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => DirName,
        <<"fileType">> => str_utils:to_binary(?DIRECTORY_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => LinkFileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => SymlinkName,
        <<"fileType">> => str_utils:to_binary(?SYMLINK_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => DirName,
        <<"fileType">> => str_utils:to_binary(?DIRECTORY_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId2),

    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => LinkFileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => SymlinkName,
        <<"fileType">> => str_utils:to_binary(?SYMLINK_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId2).

rename_file(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    FileName2 = ?FILE_NAME,

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId),

    {ok, Guid} = lfm_proxy:mv(Worker, SessId, ?FILE_REF(Guid), ?PATH(FileName2, ?SPACE_ID1)),

    % check whether operation of rename was harvested
    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName2,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId).

delete_file(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId),

    ok = lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Guid)),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"delete">>
    }], ProviderId),

    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"delete">>
    }], ProviderId2).

set_json_metadata(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON
        }
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON
        }
    }], ProviderId2).

modify_json_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON
        }
    }], ProviderId),

    JSON2 = #{<<"color">> => <<"blue">>, <<"size">> => <<"big">>},
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON2, []),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON2
        }
    }], ProviderId).

delete_json_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON
        }
    }], ProviderId),

    ok = lfm_proxy:remove_metadata(Worker, SessId, ?FILE_REF(Guid), json),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId).

delete_file_with_json_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON
        }
    }], ProviderId),

    ok = lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Guid)),
    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"delete">>
    }], ProviderId).

modify_json_many_times(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    Modifications = 10000,
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ExpectedFinalJSON = lists:foldl(fun(I, _) ->
        Key = <<"key_", (integer_to_binary(I))/binary>>,
        Value = <<"value_", (integer_to_binary(I))/binary>>,
        JSON = #{Key => Value},
        ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON, []),
        JSON
    end, undefined, lists:seq(1, Modifications)),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => ExpectedFinalJSON
        }
    }], ProviderId).

set_rdf_metadata(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    RDF = ?DUMMY_RDF,

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, RDF, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"rdf">> => RDF
        }
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => RDF
        }
    }], ProviderId2).

modify_rdf_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    RDF = ?DUMMY_RDF,

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, RDF, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"rdf">> => RDF
        }
    }], ProviderId),

    RDF2 = ?DUMMY_RDF(2),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, RDF2, []),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"rdf">> => RDF2
        }
    }], ProviderId).

delete_rdf_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    RDF = ?DUMMY_RDF,

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, RDF, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"rdf">> => RDF
        }
    }], ProviderId),

    ok = lfm_proxy:remove_metadata(Worker, SessId, ?FILE_REF(Guid), rdf),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId).

delete_file_with_rdf_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    RDF = ?DUMMY_RDF,

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, RDF, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"rdf">> => RDF
        }
    }], ProviderId),

    ok = lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Guid)),
    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"delete">>
    }], ProviderId).

modify_rdf_many_times(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    Modifications = 10000,
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ExpectedFinalRDF = lists:foldl(fun(I, _) ->
        RDF = ?DUMMY_RDF(I),
        ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, RDF, []),
        RDF
    end, undefined, lists:seq(1, Modifications)),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"rdf">> => ExpectedFinalRDF
        }
    }], ProviderId).

set_xattr_metadata(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    XattrName = <<"name">>,
    XattrValue = <<"value">>,
    Xattr = #xattr{name = XattrName, value = XattrValue},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"xattrs">> => #{
                XattrName => XattrValue
            }
        }
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"xattrs">> => #{
                XattrName => XattrValue
            }
        }
    }], ProviderId2).

cdmi_xattr_should_not_be_harvested(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),
    FileName = ?FILE_NAME,
    FileContent = <<"file content">>,

    ObjectContentTypeHeader = {?HDR_CONTENT_TYPE, <<"application/cdmi-object">>},
    CDMIVersionHeader = {<<"X-CDMI-Specification-Version">>, <<"1.1.1">>},
    UserTokenHeader = rest_test_utils:user_token_header(Config, ?USER_ID),

    RequestHeaders1 = [ObjectContentTypeHeader, CDMIVersionHeader, UserTokenHeader],
    RequestBody1 = #{<<"value">> => FileContent},
    RawRequestBody1 = json_utils:encode((RequestBody1)),
    {ok, _, _, Response1} = ?assertMatch({ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(Worker, filename:join(?SPACE_NAME1, FileName), put, RequestHeaders1, RawRequestBody1)),
    CdmiResponse1 = json_utils:decode(Response1),
    FileId = maps:get(<<"objectID">>, CdmiResponse1),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId2).


modify_xattr_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    XattrName = <<"name">>,
    XattrValue = <<"value">>,
    Xattr = #xattr{name = XattrName, value = XattrValue},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"xattrs">> => #{
                XattrName => XattrValue
            }
        }
    }], ProviderId),

    XattrName2 = <<"name2">>,
    XattrValue2 = <<"value2">>,
    Xattr2 = #xattr{name = XattrName2, value = XattrValue2},

    ok = lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"xattrs">> => #{
                XattrName => XattrValue,
                XattrName2 => XattrValue2
            }
        }
    }], ProviderId).

delete_xattr_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    XattrName = <<"name">>,
    XattrValue = <<"value">>,
    Xattr = #xattr{name = XattrName, value = XattrValue},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"xattrs">> => #{
                XattrName => XattrValue
            }
        }
    }], ProviderId),

    ok = lfm_proxy:remove_xattr(Worker, SessId, ?FILE_REF(Guid), XattrName),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId).

delete_file_with_xattr_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    XattrName = <<"name">>,
    XattrValue = <<"value">>,
    Xattr = #xattr{name = XattrName, value = XattrValue},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"submit">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"payload">> => #{
            <<"xattrs">> => #{
                XattrName => XattrValue
            }
        }
    }], ProviderId),

    ok = lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Guid)),
    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"delete">>
    }], ProviderId).

modify_xattr_many_times(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    Modifications = 10000,
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ExpectedFinalXattrs = lists:foldl(fun(I, XattrsIn) ->
        XattrName = <<"name", (integer_to_binary(I))/binary>>,
        XattrValue = <<"value", (integer_to_binary(I))/binary>>,
        Xattr = #xattr{name = XattrName, value = XattrValue},
        ok = lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr),
        XattrsIn#{XattrName => XattrValue}
    end, #{}, lists:seq(1, Modifications)),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"xattrs">> => ExpectedFinalXattrs
        }
    }], ProviderId).

modify_metadata_and_rename_file(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    FileName2 = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON
        }
    }], ProviderId),

    JSON2 = #{<<"color">> => <<"blue">>, <<"size">> => <<"big">>},
    {ok, Guid} = lfm_proxy:mv(Worker, SessId, ?FILE_REF(Guid), ?PATH(FileName2, ?SPACE_ID1)),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON2, []),

    % both updates should be aggregated and sent in one batch
    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName2,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON2
        }
    }], ProviderId).


changes_should_be_submitted_to_all_harvesters_and_indices_subscribed_for_the_space(Config) ->
    % ?HARVESTER1 and ?HARVESTER2 are subscribed for ?SPACE_ID2
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,

    JSON1 = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID2)),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON1, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{
        ?HARVESTER1 => [?INDEX11],
        ?HARVESTER2 => [?INDEX21, ?INDEX22, ?INDEX23]
    },
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID2, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID2,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON1
        }
    }], ProviderId).

changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester(Config) ->
    % ?HARVESTER1 is subscribed for ?SPACE_ID3 and ?SPACE_ID4
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},

    FileName2 = ?FILE_NAME,
    JSON2 = #{<<"color">> => <<"red">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID3)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON1, []),

    {ok, Guid2} = lfm_proxy:create(Worker, SessId, ?PATH(FileName2, ?SPACE_ID4)),
    {ok, FileId2} = file_id:guid_to_objectid(Guid2),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid2), json, JSON2, []),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID3, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID3,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON1
        }
    }], ProviderId),

    ?assertReceivedHarvestMetadata(?SPACE_ID4, Destination, [#{
        <<"fileId">> => FileId2,
        <<"spaceId">> => ?SPACE_ID4,
        <<"fileName">> => FileName2,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON2
        }
    }], ProviderId).

each_provider_should_submit_only_local_changes_to_the_harvester(Config) ->
    % ?HARVESTER3 is subscribed for ?SPACE_ID5 which is supported by both providers
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(WorkerP1),
    SessId2 = ?SESS_ID(WorkerP2),
    ProviderId1 = ?PROVIDER_ID(WorkerP1),
    ProviderId2 = ?PROVIDER_ID(WorkerP2),

    FileName = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},

    FileName2 = ?FILE_NAME,
    JSON2 = #{<<"color">> => <<"red">>},

    {ok, Guid} = lfm_proxy:create(WorkerP1, SessId, ?PATH(FileName, ?SPACE_ID5)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(WorkerP1, SessId, ?FILE_REF(Guid), json, JSON1, []),

    {ok, Guid2} = lfm_proxy:create(WorkerP2, SessId2, ?PATH(FileName2, ?SPACE_ID5)),
    {ok, FileId2} = file_id:guid_to_objectid(Guid2),
    ok = lfm_proxy:set_metadata(WorkerP2, SessId2, ?FILE_REF(Guid2), json, JSON2, []),

    Destination = #{?HARVESTER3 => [?INDEX31]},

    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessId, ?FILE_REF(Guid2)), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP2, SessId2, ?FILE_REF(Guid)), ?ATTEMPTS),

    ?assertReceivedHarvestMetadata(?SPACE_ID5, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID5,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON1
        }
    }], ProviderId1),

    ?assertReceivedHarvestMetadata(?SPACE_ID5, Destination, [#{
        <<"fileId">> => FileId2,
        <<"spaceId">> => ?SPACE_ID5,
        <<"fileName">> => FileName2,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON2
        }
    }], ProviderId2),

    ?assertNotReceivedHarvestMetadata(?SPACE_ID5, Destination, [#{
        <<"fileId">> => FileId2,
        <<"spaceId">> => ?SPACE_ID5,
        <<"fileName">> => FileName2,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON2
        }
    }], ProviderId1),

    ?assertNotReceivedHarvestMetadata(?SPACE_ID5, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID5,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON1
        }
    }], ProviderId2).

each_provider_should_submit_only_local_changes_to_the_harvester2(Config) ->
    % ?HARVESTER3 is subscribed for ?SPACE_ID5 which is supported by both providers
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(WorkerP1),
    SessId2 = ?SESS_ID(WorkerP2),
    ProviderId1 = ?PROVIDER_ID(WorkerP1),
    ProviderId2 = ?PROVIDER_ID(WorkerP2),

    FileName = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},

    FileName2 = ?FILE_NAME,
    JSON2 = #{<<"color">> => <<"red">>},

    {ok, Guid} = lfm_proxy:create(WorkerP1, SessId, ?PATH(FileName, ?SPACE_ID5)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(WorkerP1, SessId, ?FILE_REF(Guid), json, JSON1, []),

    {ok, Guid2} = lfm_proxy:create(WorkerP2, SessId2, ?PATH(FileName2, ?SPACE_ID5)),
    {ok, FileId2} = file_id:guid_to_objectid(Guid2),
    ok = lfm_proxy:set_metadata(WorkerP2, SessId2, ?FILE_REF(Guid2), json, JSON2, []),

    Destination = #{?HARVESTER3 => [?INDEX31]},

    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessId, ?FILE_REF(Guid2)), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP2, SessId2, ?FILE_REF(Guid)), ?ATTEMPTS),

    ?assertReceivedHarvestMetadata(?SPACE_ID5, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID5,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON1
        }
    }], ProviderId1),

    ?assertReceivedHarvestMetadata(?SPACE_ID5, Destination, [#{
        <<"fileId">> => FileId2,
        <<"spaceId">> => ?SPACE_ID5,
        <<"fileName">> => FileName2,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON2
        }
    }], ProviderId2),

    ?assertNotReceivedHarvestMetadata(?SPACE_ID5, Destination, [#{
        <<"fileId">> => FileId2,
        <<"spaceId">> => ?SPACE_ID5,
        <<"fileName">> => FileName2,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON2
        }
    }], ProviderId1),

    ?assertNotReceivedHarvestMetadata(?SPACE_ID5, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID5,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON1
        }
    }], ProviderId2),

    ok = lfm_proxy:unlink(WorkerP1, SessId, ?FILE_REF(Guid2)),
    ok = lfm_proxy:unlink(WorkerP2, SessId2, ?FILE_REF(Guid)),

    ?assertReceivedHarvestMetadata(?SPACE_ID5, Destination, [#{
        <<"fileId">> => FileId2,
        <<"operation">> => <<"delete">>
    }], ProviderId1),

    ?assertReceivedHarvestMetadata(?SPACE_ID5, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"delete">>
    }], ProviderId2),

    ?assertNotReceivedHarvestMetadata(?SPACE_ID5, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"delete">>
    }], ProviderId1),

    ?assertNotReceivedHarvestMetadata(?SPACE_ID5, Destination, [#{
        <<"fileId">> => FileId2,
        <<"operation">> => <<"delete">>
    }], ProviderId2).

submit_entry_failure(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    FileName2 = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},

    HSPid1 = get_main_harvesting_stream_pid(Worker, ?SPACE_ID1),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON1, []),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON1
        }
    }], ProviderId),

    set_mock_harvest_metadata_failure(Worker, true),

    JSON2 = #{<<"color">> => <<"red">>},
    JSON3 = #{<<"color">> => <<"green">>},

    {ok, Guid2} = lfm_proxy:create(Worker, SessId, ?PATH(FileName2, ?SPACE_ID1)),
    {ok, FileId2} = file_id:guid_to_objectid(Guid2),

    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON2, []),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid2), json, JSON3, []),

    % changes should not be submitted as connection to onezone failed
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON2
        }
    }, #{
        <<"fileId">> => FileId2,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName2,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON3
        }
    }], ProviderId),

    set_mock_harvest_metadata_failure(Worker, false),

    % harvesting_stream should not have been restarted
    ?assertEqual(HSPid1, get_main_harvesting_stream_pid(Worker, ?SPACE_ID1)),

    % missing changes should be submitted
    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON2
        }
    }, #{
        <<"fileId">> => FileId2,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName2,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON3
        }
    }], ProviderId, 60),

    % previously sent change should not be submitted
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON1
        }
    }], ProviderId).

delete_entry_failure(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},

    HSPid1 = get_main_harvesting_stream_pid(Worker, ?SPACE_ID1),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON1, []),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON1
        }
    }], ProviderId),

    set_mock_harvest_metadata_failure(Worker, true),

    ok = lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Guid)),

    % change should not be submitted as connection to onezone failed
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"delete">>
    }], ProviderId),

    set_mock_harvest_metadata_failure(Worker, false),

    % harvesting_stream should not have been restarted
    ?assertEqual(HSPid1, get_main_harvesting_stream_pid(Worker, ?SPACE_ID1)),

    % missing changes should be submitted
    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"delete">>
    }], ProviderId, 60),

    % previously sent change should not be submitted
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON1
        }
    }], ProviderId).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        application:ensure_all_started(hackney),
        initializer:setup_storage(NewConfig)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    application:stop(hackney),
    ssl:stop().

init_per_testcase(_Case, Config) ->
    Config2 = sort_workers(Config),
    Workers = ?config(op_worker_nodes, Config2),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config2, "env_desc.json"), Config2),

    lists:foreach(fun(W) ->
        lists:foreach(fun(SpaceId) ->
            {ok, SpaceDoc} = rpc:call(W, space_logic, get, [?ROOT_SESS_ID, SpaceId]),
            {ok, Harvesters} = space_logic:get_harvesters(SpaceDoc),
            lists:foreach(fun(HarvesterId) ->
                {ok, HarvesterDoc} = rpc:call(W, harvester_logic, get, [HarvesterId]),
                % trigger od_harvester posthooks
                rpc:call(W, initializer, put_into_cache, [HarvesterDoc])
            end, Harvesters),
            % trigger od_space posthooks
            rpc:call(W, initializer, put_into_cache, [SpaceDoc])
        end, ?SPACE_IDS)
    end, Workers),
    set_mock_harvest_metadata_failure(Workers, false),
    mock_space_logic_harvest_metadata(Workers),
    mock_space_quota_checks(Workers),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, space_logic),
    lists:foreach(fun(W) ->
        SupervisorPid = whereis(W, harvesting_stream_sup),
        exit(SupervisorPid, normal)
    end, Workers),
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
            case op_worker:get_env(?MOCK_HARVEST_METADATA_FAILURE, false) of
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
    ok = test_utils:mock_expect(Node, space_quota, get_disabled_spaces, fun() ->
        {ok, []} end).

sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).

get_main_harvesting_stream_pid(Node, SpaceId) ->
    rpc:call(Node, global, whereis_name, [?MAIN_HARVESTING_STREAM(SpaceId)]).


subtract_batches(ExpectedBatch, ReceivedBatch) ->
    % Entries in ExpectedBatch and ReceivedBatch may not
    % be in the same order.
    % This may happen due to race when saving changes to DB.
    % Changes may be persisted in different order than they were performed on memory.
    subtract_sorted_batches(sort_batch(ExpectedBatch), sort_batch(ReceivedBatch)).

sort_batch(Batch) ->
    lists:sort(fun
        (#{<<"fileId">> := FileId1}, #{<<"fileId">> := FileId2}) -> FileId1 =< FileId2
    end, Batch).

%% @doc this function returns a 2-element tuple in which:
%%  * the 1st element is a list of expected batch entries that have not been received yet
%%  * the 2nd element is a list of unexpected batch entries that have been received
subtract_sorted_batches(ExpectedBatchSorted, ReceivedBatchSorted) ->
    subtract_sorted_batches(ExpectedBatchSorted, ReceivedBatchSorted, []).


subtract_sorted_batches(B1, [], UnexpectedReversed) ->
    {B1, lists:reverse(UnexpectedReversed)};
subtract_sorted_batches([], B2, RestReversed) ->
    {[], lists:reverse(RestReversed) ++ B2};
subtract_sorted_batches(
    B1 = [ExpectedEntry = #{<<"operation">> := <<"submit">>} | T],
    [ReceivedEntry = #{<<"operation">> := <<"submit">>} | T2],
    UnexpectedReversed
) ->
    case are_submit_entries_equal(ExpectedEntry, ReceivedEntry) of
        true ->
            subtract_sorted_batches(T, T2, UnexpectedReversed);
        false ->
            subtract_sorted_batches(B1, T2, [ReceivedEntry | UnexpectedReversed])
    end;
subtract_sorted_batches([#{
    <<"fileId">> := FileId,
    <<"operation">> := <<"delete">>
} | T], [#{
    <<"fileId">> := FileId,
    <<"operation">> := <<"delete">>
} | T2], UnexpectedReversed) ->
    subtract_sorted_batches(T, T2, UnexpectedReversed);
subtract_sorted_batches(B1, [H | T2], UnexpectedReversed) ->
    subtract_sorted_batches(B1, T2, [H | UnexpectedReversed]).


are_submit_entries_equal(ExpectedEntry = #{
    <<"fileId">> := FileId,
    <<"operation">> := <<"submit">>,
    <<"payload">> := Payload,
    <<"spaceId">> := SpaceId,
    <<"fileName">> := FileName,
    <<"fileType">> := FileType
}, ReceivedEntry) ->
    % mandatory fields
    (FileId == maps:get(<<"fileId">>, ReceivedEntry)) andalso
    (Payload == maps:get(<<"payload">>, ReceivedEntry)) andalso
    (SpaceId == maps:get(<<"spaceId">>, ReceivedEntry)) andalso
    (FileName == maps:get(<<"fileName">>, ReceivedEntry)) andalso
    (FileType == maps:get(<<"fileType">>, ReceivedEntry)) andalso
    % optional fields
    (maps:get(<<"datasetId">>, ExpectedEntry, undefined) == maps:get(<<"datasetId">>, ReceivedEntry, undefined)).
