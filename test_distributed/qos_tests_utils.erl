%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains utils functions for QoS tests.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_tests_utils).
-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([create_dir_structure/4, create_file/4, assert_qos_entry_document/6,
    assert_file_qos_document/4, create_directory/3, assert_effective_qos/4,
    get_guid/3]).

-define(req(W, SessId, FuseRequest), element(2, rpc:call(W, worker_proxy, call,
    [fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = FuseRequest}}]))).


assert_qos_entry_document(Worker, QosId, FileUuid, Expression, ReplicasNum, Status) ->
    ExpectedQosEntry = #qos_entry{
        file_uuid = FileUuid,
        expression = Expression,
        replicas_num = ReplicasNum,
        status = Status
    },
    {ok, #document{value = QosEntry}} = ?assertMatch({ok, _Doc}, rpc:call(Worker, qos_entry, get, [QosId])),
    ?assertMatch(ExpectedQosEntry, QosEntry).


assert_file_qos_document(Worker, FileUuid, QosList, TargetStorages) ->
    ExpectedFileQos = #file_qos{
        qos_list = QosList,
        target_storages = TargetStorages
    },
    ExpectedFileQosSorted = sort_file_qos(ExpectedFileQos),

    {ok, #document{value = FileQos}} = ?assertMatch({ok, _Doc}, rpc:call(Worker, file_qos, get, [FileUuid])),
    FileQosSorted = sort_file_qos(FileQos),
    ?assertMatch(ExpectedFileQosSorted, FileQosSorted).


assert_effective_qos(Worker, FileUuid, QosList, TargetStorages) ->
    ExpectedFileQos = #file_qos{
        qos_list = QosList,
        target_storages = TargetStorages
    },
    ExpectedFileQosSorted = sort_file_qos(ExpectedFileQos),

    EffQos = ?assertMatch(_EffQos, rpc:call(Worker, file_qos, get_effective, [FileUuid])),
    EffQosSorted = sort_file_qos(EffQos),
    ?assertMatch(ExpectedFileQosSorted, EffQosSorted).


sort_file_qos(FileQos) ->
    FileQos#file_qos{
        qos_list = lists:sort(FileQos#file_qos.qos_list),
        target_storages = maps:map(fun(_, QosListForStorage) ->
            lists:sort(QosListForStorage)
        end, FileQos#file_qos.target_storages)
    }.


create_dir_structure(Worker, SessionId, {DirName, DirContent}, Path) when is_list(DirContent) ->
    try
        create_directory(Worker, SessionId, filename:join(Path, DirName))
    catch
        error:eexist ->
            ok
    end,
    DirPath = filename:join(Path, DirName),
    lists:foreach(fun(Child) ->
        create_dir_structure(Worker, SessionId, Child, DirPath)
    end, DirContent);
create_dir_structure(Worker, SessionId, {FileName, FileContent}, Path) ->
    create_file(Worker, SessionId, filename:join(Path, FileName), FileContent).


create_directory(Worker, SessionId, DirPath) ->
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, SessionId, DirPath),
    DirGuid.


create_file(Worker, SessionId, FilePath, FileContent) ->
    {ok, FileGuid} = lfm_proxy:create(Worker, SessionId, FilePath, 8#700),
    {ok, Handle} = lfm_proxy:open(Worker, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(Worker, Handle, 0, FileContent),
    lfm_proxy:fsync(Worker, Handle),
    lfm_proxy:close(Worker, Handle),
    FileGuid.


get_guid(Worker, SessId, Path) ->
    #fuse_response{fuse_response = #guid{guid = Guid}} =
        ?assertMatch(
            #fuse_response{status = #status{code = ?OK}},
            ?req(Worker, SessId, #resolve_guid{path = Path}),
            30
        ),
    Guid.