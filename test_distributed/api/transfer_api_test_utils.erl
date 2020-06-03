%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning transfer create API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_api_test_utils).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("../transfers_test_mechanism.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    create_file/3,

    create_setup_file_replication_env_fun/5,
    create_setup_view_transfer_env_fun/5,

    create_verify_transfer_env_fun/6
]).


-define(BYTES_NUM, 20).


%%%===================================================================
%%% API
%%%===================================================================


create_file(Node, SessId, DirPath) ->
    FilePath = filename:join([DirPath, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(<<"file">>, Node, SessId, FilePath, 8#777),
    api_test_utils:fill_file_with_dummy_data(Node, SessId, FileGuid, ?BYTES_NUM),
    FileGuid.


%%--------------------------------------------------------------------
%% @doc
%% Creates either single file or directory with 5 files (random select) and
%% awaits metadata synchronization between nodes. In case of 'eviction' also
%% copies data so that it would exist on all nodes.
%% FileGuid to transfer and expected transfer stats are saved in returned map.
%% @end
%%--------------------------------------------------------------------
create_setup_file_replication_env_fun(TransferType, SrcNode, DstNode, UserId, Config) ->
    fun() ->
        SessId1 = ?SESS_ID(UserId, SrcNode, Config),

        RootFileType = api_test_utils:randomly_choose_file_type_for_test(false),
        RootFilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
        {ok, RootFileGuid} = api_test_utils:create_file(
            RootFileType, SrcNode, SessId1, RootFilePath, 8#777
        ),
        {ok, RootFileObjectId} = file_id:guid_to_objectid(RootFileGuid),

        FilesToTransfer = case RootFileType of
            <<"file">> ->
                api_test_utils:fill_file_with_dummy_data(SrcNode, SessId1, RootFileGuid, ?BYTES_NUM),
                [RootFileGuid];
            <<"dir">> ->
                lists:map(fun(_) ->
                    create_file(SrcNode, SessId1, RootFilePath)
                end, lists:seq(1, 5))
        end,
        OtherFiles = [create_file(SrcNode, SessId1, filename:join(["/", ?SPACE_2]))],

        sync_files_between_nodes(TransferType, SrcNode, DstNode, OtherFiles ++ FilesToTransfer),

        ExpTransfer = get_exp_transfer_stats(
            TransferType, RootFileType, SrcNode, DstNode, length(FilesToTransfer)
        ),

        #{
            root_file_guid => RootFileGuid,
            root_file_path => RootFilePath,
            root_file_cdmi_id => RootFileObjectId,
            exp_transfer => ExpTransfer#{
                space_id => ?SPACE_2,
                file_uuid => file_id:guid_to_uuid(RootFileGuid),
                path => RootFilePath
            },
            files_to_transfer => FilesToTransfer,
            other_files => OtherFiles
        }
    end.


create_setup_view_transfer_env_fun(TransferType, SrcNode, DstNode, UserId, Config) ->
    fun() ->
        SessId1 = ?SESS_ID(UserId, SrcNode, Config),

        FilesToTransferNum = rand:uniform(5),
        RootDirPath = filename:join(["/", ?SPACE_2]),

        XattrName = transfers_test_utils:random_job_name(?FUNCTION_NAME),
        XattrValue = 1,
        Xattr = #xattr{name = XattrName, value = XattrValue},

        ViewName = create_view(TransferType, ?SPACE_2, XattrName, SrcNode, DstNode),

        FilesToTransfer = lists:map(fun(_) ->
            FileGuid = create_file(SrcNode, SessId1, RootDirPath),
            ?assertMatch(ok, lfm_proxy:set_xattr(SrcNode, SessId1, {guid, FileGuid}, Xattr)),
            FileGuid
        end, lists:seq(1, FilesToTransferNum)),

        OtherFiles = [create_file(SrcNode, SessId1, RootDirPath)],

        sync_files_between_nodes(TransferType, SrcNode, DstNode, OtherFiles ++ FilesToTransfer),

        ObjectIds = api_test_utils:guids_to_object_ids(FilesToTransfer),
        QueryViewParams = [{key, XattrValue}],

        case TransferType of
            replication ->
                % Wait until all FilesToTransfer are indexed by view on DstNode.
                % Otherwise some of them could be omitted from replication.
                ?assertViewQuery(ObjectIds, DstNode, ?SPACE_2, ViewName,  QueryViewParams);
            eviction ->
                % Wait until all FilesToTransfer are indexed by view on SrcNode.
                % Otherwise some of them could be omitted from eviction.
                ?assertViewQuery(ObjectIds, SrcNode, ?SPACE_2, ViewName,  QueryViewParams);
            migration ->
                % Wait until FilesToTransfer are all indexed by view on SrcNode and DstNode.
                % Otherwise some of them could be omitted from replication/eviction.
                ?assertViewQuery(ObjectIds, SrcNode, ?SPACE_2, ViewName,  QueryViewParams),
                ?assertViewQuery(ObjectIds, DstNode, ?SPACE_2, ViewName,  QueryViewParams)
        end,
        ExpTransfer = get_exp_transfer_stats(
            TransferType, <<"dir">>, SrcNode, DstNode, FilesToTransferNum
        ),

        #{
            view_name => ViewName,
            exp_transfer => ExpTransfer#{
                space_id => ?SPACE_2,
                file_uuid => fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_2),
                path => RootDirPath
            },
            files_to_transfer => FilesToTransfer,
            other_files => OtherFiles
        }
    end.


create_verify_transfer_env_fun(replication, Node, UserId, SrcProvider, DstProvider, Config) ->
    create_verify_replication_env_fun(Node, UserId, SrcProvider, DstProvider, Config);
create_verify_transfer_env_fun(eviction, Node, UserId, SrcProvider, DstProvider, Config) ->
    create_verify_eviction_env_fun(Node, UserId, SrcProvider, DstProvider, Config);
create_verify_transfer_env_fun(migration, Node, UserId, SrcProvider, DstProvider, Config) ->
    create_verify_migration_env_fun(Node, UserId, SrcProvider, DstProvider, Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
create_view(TransferType, SpaceId, XattrName, SrcNode, DstNode) ->
    ViewName = transfers_test_utils:random_view_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName),

    Providers = case TransferType of
        replication ->
            [transfers_test_utils:provider_id(DstNode)];
        eviction ->
            [transfers_test_utils:provider_id(SrcNode)];
        migration ->
            [transfers_test_utils:provider_id(SrcNode), transfers_test_utils:provider_id(DstNode)]
    end,

    transfers_test_utils:create_view(DstNode, SpaceId, ViewName, MapFunction, [], Providers),

    ViewName.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Wait for file file_meta sync between providers. Id transfer is 'eviction'
%% also copy files content to other provider and awaits file_location sync
%% (eviction doesn't work if data replicas don't exist on other providers).
%% @end
%%--------------------------------------------------------------------
sync_files_between_nodes(eviction, SrcNode, DstNode, Files) ->
    lists:foreach(fun(Guid) ->
        % Read file on DstNode to force rtransfer
        api_test_utils:wait_for_file_sync(DstNode, ?ROOT_SESS_ID, Guid),
        ExpContent = api_test_utils:read_file(SrcNode, ?ROOT_SESS_ID, Guid, ?BYTES_NUM),
        ?assertMatch(
            ExpContent,
            api_test_utils:read_file(DstNode, ?ROOT_SESS_ID, Guid, ?BYTES_NUM),
            ?ATTEMPTS
        )
    end, Files),
    % Wait until file_distribution contains entries for both nodes
    % Otherwise some of them could be omitted from eviction (if data
    % replicas don't exist on other providers eviction for file is skipped).
    assert_distribution(SrcNode, ?ROOT_SESS_ID, Files, [{SrcNode, ?BYTES_NUM}, {DstNode, ?BYTES_NUM}]);

sync_files_between_nodes(_TransferType, _SrcNode, DstNode, Files) ->
    lists:foreach(fun(Guid) ->
        api_test_utils:wait_for_file_sync(DstNode, ?ROOT_SESS_ID, Guid)
    end, Files).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns expected transfer statistics depending on transfer type and
%% data source type. In case of 'dir' it is assumed that it contains only files
%% and not any subdirectories.
%% @end
%%--------------------------------------------------------------------
get_exp_transfer_stats(replication, <<"file">>, _SrcNode, DstNode, _FilesToTransferNum) ->
    #{
        replication_status => completed,
        eviction_status => skipped,
        replicating_provider => transfers_test_utils:provider_id(DstNode),
        evicting_provider => undefined,
        files_to_process => 1,
        files_processed => 1,
        files_replicated => 1,
        bytes_replicated => ?BYTES_NUM,
        files_evicted => 0
    };
get_exp_transfer_stats(eviction, <<"file">>, SrcNode, _DstNode, _FilesToTransferNum) ->
    #{
        replication_status => skipped,
        eviction_status => completed,
        replicating_provider => undefined,
        evicting_provider => transfers_test_utils:provider_id(SrcNode),
        files_to_process => 1,
        files_processed => 1,
        files_replicated => 0,
        bytes_replicated => 0,
        files_evicted => 1
    };
get_exp_transfer_stats(migration, <<"file">>, SrcNode, DstNode, _FilesToTransferNum) ->
    #{
        replication_status => completed,
        eviction_status => completed,
        replicating_provider => transfers_test_utils:provider_id(DstNode),
        evicting_provider => transfers_test_utils:provider_id(SrcNode),
        files_to_process => 2,
        files_processed => 2,
        files_replicated => 1,
        bytes_replicated => ?BYTES_NUM,
        files_evicted => 1
    };
get_exp_transfer_stats(replication, <<"dir">>, _SrcNode, DstNode, FilesToTransferNum) ->
    #{
        replication_status => completed,
        eviction_status => skipped,
        replicating_provider => transfers_test_utils:provider_id(DstNode),
        evicting_provider => undefined,
        files_to_process => 1 + FilesToTransferNum,
        files_processed => 1 + FilesToTransferNum,
        files_replicated => FilesToTransferNum,
        bytes_replicated => FilesToTransferNum * ?BYTES_NUM,
        files_evicted => 0
    };
get_exp_transfer_stats(eviction, <<"dir">>, SrcNode, _DstNode, FilesToTransferNum) ->
    #{
        replication_status => skipped,
        eviction_status => completed,
        replicating_provider => undefined,
        evicting_provider => transfers_test_utils:provider_id(SrcNode),
        files_to_process => 1 + FilesToTransferNum,
        files_processed => 1 + FilesToTransferNum,
        files_replicated => 0,
        bytes_replicated => 0,
        files_evicted => FilesToTransferNum
    };
get_exp_transfer_stats(migration, <<"dir">>, SrcNode, DstNode, FilesToTransferNum) ->
    #{
        replication_status => completed,
        eviction_status => completed,
        replicating_provider => transfers_test_utils:provider_id(DstNode),
        evicting_provider => transfers_test_utils:provider_id(SrcNode),
        files_to_process => 2 * (1 + FilesToTransferNum),
        files_processed => 2 * (1 + FilesToTransferNum),
        files_replicated => FilesToTransferNum,
        bytes_replicated => FilesToTransferNum * ?BYTES_NUM,
        files_evicted => FilesToTransferNum
    }.


%% @private
create_verify_replication_env_fun(Node, UserId, SrcProvider, DstProvider, Config) ->
    SessId = ?SESS_ID(UserId, Node, Config),

    fun
        (expected_failure, #api_test_ctx{env = #{files_to_transfer := FilesToTransfer, other_files := OtherFiles}}) ->
            assert_distribution(
                Node, SessId, _AllFiles = OtherFiles ++ FilesToTransfer,
                [{SrcProvider, ?BYTES_NUM}]
            ),
            true;
        (expected_success, #api_test_ctx{env = #{files_to_transfer := FilesToTransfer, other_files := OtherFiles}}) ->
            assert_distribution(
                Node, SessId, OtherFiles,
                [{SrcProvider, ?BYTES_NUM}]
            ),
            assert_distribution(
                Node, SessId, FilesToTransfer,
                [{SrcProvider, ?BYTES_NUM}, {DstProvider, ?BYTES_NUM}]
            ),
            true
    end.


%% @private
create_verify_eviction_env_fun(Node, UserId, SrcProvider, DstProvider, Config) ->
    SessId = ?SESS_ID(UserId, Node, Config),

    fun
        (expected_failure, #api_test_ctx{env = #{files_to_transfer := FilesToTransfer, other_files := OtherFiles}}) ->
            assert_distribution(
                Node, SessId, _AllFiles = OtherFiles ++ FilesToTransfer,
                [{SrcProvider, ?BYTES_NUM}, {DstProvider, ?BYTES_NUM}]
            ),
            true;
        (expected_success, #api_test_ctx{env = #{files_to_transfer := FilesToTransfer, other_files := OtherFiles}}) ->
            assert_distribution(
                Node, SessId, OtherFiles,
                [{SrcProvider, ?BYTES_NUM}, {DstProvider, ?BYTES_NUM}]
            ),
            assert_distribution(
                Node, SessId, FilesToTransfer,
                [{SrcProvider, 0}, {DstProvider, ?BYTES_NUM}]
            ),
            true
    end.


%% @private
create_verify_migration_env_fun(Node, UserId, SrcProvider, DstProvider, Config) ->
    SessId = ?SESS_ID(UserId, Node, Config),

    fun
        (expected_failure, #api_test_ctx{env = #{files_to_transfer := FilesToTransfer, other_files := OtherFiles}}) ->
            assert_distribution(
                Node, SessId, _AllFiles = FilesToTransfer ++ OtherFiles,
                [{SrcProvider, ?BYTES_NUM}]
            ),
            true;
        (expected_success, #api_test_ctx{env = #{files_to_transfer := FilesToTransfer, other_files := OtherFiles}}) ->
            assert_distribution(
                Node, SessId, OtherFiles,
                [{SrcProvider, ?BYTES_NUM}]
            ),
            assert_distribution(
                Node, SessId, FilesToTransfer,
                [{SrcProvider, 0}, {DstProvider, ?BYTES_NUM}]
            ),
            true
    end.


%% @private
assert_distribution(Node, SessId, Files, ExpSizePerProvider) ->
    ExpDistribution = lists:sort(lists:map(fun({Provider, ExpSize}) ->
        #{
            <<"blocks">> => case ExpSize of
                0 -> [];
                _ -> [[0, ExpSize]]
            end,
            <<"providerId">> => transfers_test_utils:provider_id(Provider),
            <<"totalBlocksSize">> => ExpSize
        }
    end, ExpSizePerProvider)),

    FetchDistributionFun = fun(Guid) ->
        {ok, Distribution} = lfm_proxy:get_file_distribution(Node, SessId, {guid, Guid}),
        lists:sort(Distribution)
    end,

    lists:foreach(fun(FileGuid) ->
        ?assertEqual(ExpDistribution, FetchDistributionFun(FileGuid), ?ATTEMPTS)
    end, Files).
