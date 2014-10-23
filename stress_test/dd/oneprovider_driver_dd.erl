%% ===================================================================
%% @author Michał Wrzeszcz
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This test checks correctness and amount of write operations
%% using FUSE.
%% @end
%% ===================================================================

-module(oneprovider_driver_dd).
-export([setup/0, new/1, run/4]).

-include("basho_bench.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("oneprovider_modules/dao/dao_vfs.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").

%% ====================================================================
%% Test driver callbacks
%% ====================================================================

%% setup/0
%% ====================================================================
%% @doc Runs once per each test node at begging of a test (before any new/1 is called)
-spec setup() -> Result when
    Result :: ok | {error, Reason :: term()}.
%% ====================================================================
setup() ->
    try
        ?INFO("Test setup~n", []),

        case basho_bench_config:get(client_id) of
            1 -> setup_storages(); %% If its the first test node, initialize cluster
            _ -> timer:sleep(2000) %% Otherwise wait for main node to finish
        %% TODO: implement better, more deterministic way of synchronising test nodes (e.g. via ready-ping)
        end,
        ok
    catch
        E1:E2 ->
            ?ERROR("Setup error: ~p~n", [{E1, E2}]),
            {error, {E1, E2}}
    end.


%% new/1
%% ====================================================================
%% @doc Creates new worker with integer id
-spec new(Id :: integer()) -> Result when
    Result :: {ok, term()} | {error, Reason :: term()}.
%% ====================================================================
new(Id) ->
    try
        ?INFO("Initializing worker with id: ~p~n", [Id]),
        VFSRoot = basho_bench_config:get(onedata_root),
        Dir = VFSRoot ++ "/stress_test_" ++ basho_bench_config:get(build_id),
        File = Dir ++ "/file_" ++ integer_to_list(Id),

        case st_utils:make_dir(Dir) of
            ok ->
                %% Open test file (each test process gets different file name like "file_Id")
                Device = case open_helper(File, {error, first_try}, 20) of
                             {error, Reason} ->
                                 ?ERROR("Can not open file: ~p: ~p", [File, Reason]),
                                 Reason;
                             IO -> IO
                         end,
                BlockSize = basho_bench_config:get(block_size),
                Data = [0 || _X <- lists:seq(1, 1024 * BlockSize)],
                Args = {Device, 0, Data},
                ?INFO("Worker with id: ~p initialized successfully with arguments: ~p", [Id, Args]),
                {ok, {Device, 0, Data}};
            {error, Reason} ->
                ?ERROR("Can not create directory: ~p: ~p~n", [Dir, Reason]),
                {error, Reason}
        end
    catch
        E1:E2 ->
            ?ERROR("Initialization error for worker with id: ~p: ~p", [Id, {E1, E2}]),
            {error, {E1, E2}}
    end.


%% run/4
%% ====================================================================
%% @doc Runs an operation using one of workers
%% Currently only 'write' operation is implemented
-spec run(Operation :: atom(), KeyGen :: fun(), ValueGen :: fun(), State :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term(), NewState :: term()}.
%% ====================================================================
run(write, _KeyGen, _ValueGen, {Dev, _Offset, _Data} = State) when is_atom(Dev) ->
    timer:sleep(1000), %% Dont generate more then one error per sec when open/2 is failing
    {error, {open, Dev}, State};
run(write, _KeyGen, _ValueGen, {Dev, Offset, Data}) ->
    NewState = {Dev, (Offset + length(Data)) rem (basho_bench_config:get(max_filesize) * 1024 * 1024), Data},
    case file:pwrite(Dev, 0, Data) of
        ok -> {ok, NewState};
        {error, Reason} ->
            ?ERROR("Run error: (file: ~p, offset: ~p): ~p", [Dev, Offset, Reason]),
            {error, Reason, NewState}
    end;
run(Operation, _KeyGen, _ValueGen, State) ->
    ?ERROR("Unknown operation ~p in state ~p", [Operation, State]),
    {error, unknown_operation, State}.


%% ====================================================================
%% Helper functions
%% ====================================================================

%% setup_storages/0
%% ====================================================================
%% @doc Register test user and configure storages on cluster
-spec setup_storages() -> Result when
    Result :: ok | no_return().
%% ====================================================================
setup_storages() ->
    ?INFO("Storage setup~n", []),
    CertFile = basho_bench_config:get(cert_file),
    [Host | _] = basho_bench_config:get(cluster_hosts),
    Groups = #fuse_group_info{name = ?CLUSTER_FUSE_ID, storage_helper = #storage_helper_info{name = "DirectIO", init_args = ["/mnt/gluster"]}},
    Node = st_utils:host_to_node(Host),
    {ok, DN} = st_utils:get_dn(CertFile),

    %% Init net kernet in order to connect to cluster
    case net_kernel:start([list_to_atom("tester@" ++ net_adm:localhost()), longnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        NetError -> throw(io_lib:fwrite("Can not start net_kernel: ~p", [NetError]))
    end,

    erlang:set_cookie(node(), oneprovider_node),

    case rpc:call(Node, os, cmd, ["rm -rf /mnt/gluster/*"]) of
        "" -> ok;
        RmError -> throw(io_lib:fwrite("Can not remove files from storage: ~p", [RmError]))
    end,

    case rpc:call(Node, fslogic_storage, insert_storage, ["ClusterProxy", [], [Groups]]) of
        {ok, _} -> ok;
        InsertError -> throw(io_lib:fwrite("Can not insert storage: ~p", [InsertError]))
    end,

    case rpc:call(Node, user_logic, create_user, ["onedatatestuser", "Test Name", [], "test@test.com", [DN]]) of
        {ok, _} -> ok;
        CreateError -> throw(io_lib:fwrite("Can not add test user: ~p", [CreateError]))
    end,

    ok.


%% open_helper/3
%% ====================================================================
%% @doc Opens requested file retring when error occurs
-spec open_helper(File :: string(), {ok, IO :: pid()} | {error, Error :: term}, Retry :: integer()) -> Result when
    Result :: pid() | {error | open_error}.
%% ====================================================================
open_helper(_, {ok, IO}, _) ->
    IO;
open_helper(File, {error, _Error}, Retry) when Retry > 0 ->
    {_, _, M} = now(),
    timer:sleep(M rem 100),
    ?ERROR("Open helper error ~p (~p), retring in ~p", [_Error, Retry, M rem 100]),
    open_helper(File, file:open(File, [write, raw]), Retry - 1);
open_helper(_, _, _) ->
    {error, open_failed}.