%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains utils concerning transfer creation in api tests.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_api_test_utils).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("middleware/middleware.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("../transfers_test_mechanism.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    create_file/3,

    build_env_with_started_transfer_setup_fun/7,
    build_create_file_transfer_setup_fun/6,
    build_create_view_transfer_setup_fun/6,

    await_transfer_active/3,
    await_transfer_end/3,
    rerun_transfer/2,

    build_create_transfer_verify_fun/7
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


build_env_with_started_transfer_setup_fun(TransferType, MemRef, DataSourceType, P1, P2, UserId, Config) ->
    SetupEnvFun = build_create_transfer_setup_fun(
        TransferType, MemRef, DataSourceType, P1, P2, UserId, Config
    ),
    fun() ->
        SetupEnvFun(),
        TransferDetails = api_test_memory:get(MemRef, transfer_details),

        CreationTime = clock:timestamp_millis() div 1000,
        QueryViewParams = #{<<"descending">> => true},
        Callback = <<"callback">>,

        TransferId = create_transfer(
            TransferType, DataSourceType, P1, P2, UserId,
            QueryViewParams, Callback, TransferDetails, Config
        ),
        api_test_memory:set(MemRef, transfer_details, TransferDetails#{
            transfer_id => TransferId,
            user_id => UserId,
            creation_time => CreationTime,
            query_view_params => QueryViewParams,
            callback => Callback
        })
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates either single file or directory with 5 files (random select) and
%% awaits metadata synchronization between nodes. In case of 'eviction' also
%% copies data so that it would exist on all nodes.
%% FileGuid to transfer and expected transfer stats are saved in env.
%% @end
%%--------------------------------------------------------------------
build_create_file_transfer_setup_fun(TransferType, MemRef, SrcNode, DstNode, UserId, Config) ->
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

        api_test_memory:set(MemRef, transfer_details, #{
            src_node => SrcNode,
            dst_node => DstNode,
            root_file_guid => RootFileGuid,
            root_file_path => RootFilePath,
            root_file_type => RootFileType,
            root_file_cdmi_id => RootFileObjectId,
            exp_transfer => ExpTransfer#{
                space_id => ?SPACE_2,
                file_uuid => file_id:guid_to_uuid(RootFileGuid),
                path => RootFilePath
            },
            files_to_transfer => FilesToTransfer,
            other_files => OtherFiles
        })
    end.


build_create_view_transfer_setup_fun(TransferType, MemRef, SrcNode, DstNode, UserId, Config) ->
    fun() ->
        SessId1 = ?SESS_ID(UserId, SrcNode, Config),

        FilesToTransferNum = rand:uniform(5),
        RootDirPath = filename:join(["/", ?SPACE_2]),

        XattrName = transfers_test_utils:random_job_name(?FUNCTION_NAME),
        XattrValue = 1,
        Xattr = #xattr{name = XattrName, value = XattrValue},

        {ViewName, ViewId} = create_view(TransferType, ?SPACE_2, XattrName, SrcNode, DstNode),

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

        api_test_memory:set(MemRef, transfer_details, #{
            src_node => SrcNode,
            dst_node => DstNode,
            view_name => ViewName,
            view_id => ViewId,
            exp_transfer => ExpTransfer#{
                space_id => ?SPACE_2,
                file_uuid => fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_2),
                path => RootDirPath
            },
            files_to_transfer => FilesToTransfer,
            other_files => OtherFiles
        })
    end.


await_transfer_active(Nodes, TransferId, TransferType) ->
    ExpTransferStatus = case TransferType of
        replication -> #{replication_status => active};
        eviction -> #{eviction_status => active};
        migration -> #{replication_status => active}
    end,
    lists:foreach(fun(Node) ->
        transfers_test_utils:assert_transfer_state(Node, TransferId, ExpTransferStatus, ?ATTEMPTS)
    end, Nodes).


await_transfer_end(Nodes, TransferId, TransferType) ->
    ExpTransferStatus = case TransferType of
        replication ->
            #{replication_status => completed};
        eviction ->
            #{eviction_status => completed};
        migration ->
            #{
                replication_status => completed,
                eviction_status => completed
            }
    end,
    lists:foreach(fun(Node) ->
        transfers_test_utils:assert_transfer_state(Node, TransferId, ExpTransferStatus, ?ATTEMPTS)
    end, Nodes).


rerun_transfer(Node, TransferId) ->
    {ok, RerunId} = ?assertMatch(
        {ok, _},
        rpc:call(Node, transfer, rerun_ended, [undefined, TransferId])
    ),
    RerunId.


build_create_transfer_verify_fun(replication, MemRef, Node, _UserId, SrcProvider, DstProvider, _Config) ->
    build_crate_replication_verify_fun(MemRef, Node, SrcProvider, DstProvider);
build_create_transfer_verify_fun(eviction, MemRef, Node, _UserId, SrcProvider, DstProvider, _Config) ->
    build_crate_eviction_verify_fun(MemRef, Node, SrcProvider, DstProvider);
build_create_transfer_verify_fun(migration, MemRef, Node, _UserId, SrcProvider, DstProvider, _Config) ->
    build_crate_migration_verify_fun(MemRef, Node, SrcProvider, DstProvider).


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
    {ok, ViewId} = ?assertMatch({ok, _}, rpc:call(DstNode, view_links, get_view_id, [ViewName, SpaceId])),
    {ViewName, ViewId}.


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
    api_test_utils:assert_distribution([SrcNode], Files, [{SrcNode, ?BYTES_NUM}, {DstNode, ?BYTES_NUM}]);

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
build_crate_replication_verify_fun(MemRef, Node, SrcProvider, DstProvider) ->
    fun
        (expected_failure, _) ->
            #{
                files_to_transfer := FilesToTransfer,
                other_files := OtherFiles
            } = api_test_memory:get(MemRef, transfer_details),

            api_test_utils:assert_distribution(
                [Node], _AllFiles = OtherFiles ++ FilesToTransfer,
                [{SrcProvider, ?BYTES_NUM}]
            ),
            true;
        (expected_success, _) ->
            #{
                files_to_transfer := FilesToTransfer,
                other_files := OtherFiles
            } = api_test_memory:get(MemRef, transfer_details),

            api_test_utils:assert_distribution(
                [Node], OtherFiles,
                [{SrcProvider, ?BYTES_NUM}]
            ),
            api_test_utils:assert_distribution(
                [Node], FilesToTransfer,
                [{SrcProvider, ?BYTES_NUM}, {DstProvider, ?BYTES_NUM}]
            ),
            true
    end.


%% @private
build_crate_eviction_verify_fun(MemRef, Node, SrcProvider, DstProvider) ->
    fun
        (expected_failure, _) ->
            #{
                files_to_transfer := FilesToTransfer,
                other_files := OtherFiles
            } = api_test_memory:get(MemRef, transfer_details),

            api_test_utils:assert_distribution(
                [Node], _AllFiles = OtherFiles ++ FilesToTransfer,
                [{SrcProvider, ?BYTES_NUM}, {DstProvider, ?BYTES_NUM}]
            ),
            true;
        (expected_success, _) ->
            #{
                files_to_transfer := FilesToTransfer,
                other_files := OtherFiles
            } = api_test_memory:get(MemRef, transfer_details),

            api_test_utils:assert_distribution(
                [Node], OtherFiles,
                [{SrcProvider, ?BYTES_NUM}, {DstProvider, ?BYTES_NUM}]
            ),
            api_test_utils:assert_distribution(
                [Node], FilesToTransfer,
                [{SrcProvider, 0}, {DstProvider, ?BYTES_NUM}]
            ),
            true
    end.


%% @private
build_crate_migration_verify_fun(MemRef, Node, SrcProvider, DstProvider) ->
    fun
        (expected_failure, _) ->
            #{
                files_to_transfer := FilesToTransfer,
                other_files := OtherFiles
            } = api_test_memory:get(MemRef, transfer_details),

            api_test_utils:assert_distribution(
                [Node], _AllFiles = FilesToTransfer ++ OtherFiles,
                [{SrcProvider, ?BYTES_NUM}]
            ),
            true;
        (expected_success, _) ->
            #{
                files_to_transfer := FilesToTransfer,
                other_files := OtherFiles
            } = api_test_memory:get(MemRef, transfer_details),

            api_test_utils:assert_distribution(
                [Node], OtherFiles,
                [{SrcProvider, ?BYTES_NUM}]
            ),
            api_test_utils:assert_distribution(
                [Node], FilesToTransfer,
                [{SrcProvider, 0}, {DstProvider, ?BYTES_NUM}]
            ),
            true
    end.


%% @private
build_create_transfer_setup_fun(TransferType, MemRef, file, P1, P2, UserId, Config) ->
    build_create_file_transfer_setup_fun(
        TransferType, MemRef, P1, P2, UserId, Config
    );
build_create_transfer_setup_fun(TransferType, MemRef, view, P1, P2, UserId, Config) ->
    build_create_view_transfer_setup_fun(
        TransferType, MemRef, P1, P2, UserId, Config
    ).


%% @private
create_transfer(Type, DataSourceType, SrcNode, DstNode, UserId, QueryViewParams, Callback, TransferDetails, Config) ->
    DataSourceDependentData = case DataSourceType of
        file ->
            #{<<"fileId">> => maps:get(root_file_cdmi_id, TransferDetails)};
        view ->
            #{
                <<"spaceId">> => ?SPACE_2,
                <<"viewName">> => maps:get(view_name, TransferDetails),
                <<"queryViewParams">> => QueryViewParams
            }
    end,
    Data = DataSourceDependentData#{
        <<"type">> => atom_to_binary(Type, utf8),
        <<"evictingProviderId">> => transfers_test_utils:provider_id(SrcNode),
        <<"replicatingProviderId">> => transfers_test_utils:provider_id(DstNode),
        <<"dataSourceType">> => atom_to_binary(DataSourceType, utf8),
        <<"spaceId">> => ?SPACE_2,
        <<"callback">> => Callback
    },
    Req = #op_req{
        auth = ?USER(UserId, ?SESS_ID(UserId, SrcNode, Config)),
        gri = #gri{type = op_transfer, aspect = instance},
        operation = create,
        data = Data
    },
    {ok, value, TransferId} = ?assertMatch(
        {ok, _, _},
        rpc:call(SrcNode, middleware, handle, [Req])
    ),
    % Wait for transfer doc sync with DstProvider
    ?assertMatch({ok, _}, rpc:call(DstNode, transfer, get, [TransferId]), ?ATTEMPTS),

    TransferId.
