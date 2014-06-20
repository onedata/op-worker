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

-module(veilcluster_driver_dd).
-export([setup/0, new/1, run/4]).

-include("basho_bench.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").

%% ====================================================================
%% Test driver callbacks
%% ====================================================================

%% setup/0
%% ====================================================================
%% @doc Runs once per each test node at begging of a test (before any new/1 is called)
-spec setup() -> no_return().
%% ====================================================================
setup() ->
    try
        ?INFO("Test setup~n", []),

        case basho_bench_config:get(client_id) of
            1 -> setup_storages(); %% If its the first test node, initialize cluster
            _ -> timer:sleep(2000) %% Otherwise wait for main node to finish
        %% TODO: implement better, more deterministic way of synchronising test nodes (e.g. via ready-ping)
        end
    catch
        E1:E2 -> ?ERROR("Setup error: ~p:~p~n", [E1, E2])
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
        VFSRoot = basho_bench_config:get(veilfs_root),
        Dir = VFSRoot ++ "/stress_test_" ++ basho_bench_config:get(build_id),
        File = Dir ++ "/file_" ++ integer_to_list(Id),

        case make_dir(Dir) of
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
-spec setup_storages() -> no_return().
%% ====================================================================
setup_storages() ->
    ?INFO("Storage setup~n", []),
    CertFile = basho_bench_config:get(cert_file),
    [Host | _] = basho_bench_config:get(cluster_hosts),
    Groups = #fuse_group_info{name = ?CLUSTER_FUSE_ID, storage_helper = #storage_helper_info{name = "DirectIO", init_args = ["/mnt/gluster"]}},
    Worker = map_hostname(Host),
    {ok, DN} = get_dn(CertFile),

    %% Init net kernet in order to connect to cluster
    case net_kernel:start([list_to_atom("tester@" ++ net_adm:localhost()), longnames]) of
        {ok, _} -> ok;
        NetError -> throw(io_lib:fwrite("Can not start net_kernel: ~p", [NetError]))
    end,

    erlang:set_cookie(node(), veil_cluster_node),

    case rpc:call(Worker, os, cmd, ["rm -rf /mnt/gluster/*"]) of
        "" -> ok;
        RmError -> throw(io_lib:fwrite("Can not remove files from storage: ~p", [RmError]))
    end,

    case rpc:call(Worker, fslogic_storage, insert_storage, ["ClusterProxy", [], [Groups]]) of
        {ok, _} -> ok;
        InsertError -> throw(io_lib:fwrite("Can not insert storage: ~p", [InsertError]))
    end,

    case rpc:call(Worker, user_logic, create_user, ["veilfstestuser", "Test Name", [], "test@test.com", [DN]]) of
        {ok, _} -> ok;
        CreateError -> throw(io_lib:fwrite("Can not add test user: ~p", [CreateError]))
    end.


%% get_dn/1
%% ====================================================================
%% @doc Gets rDN list compatibile user_logic:create_user from PEM file
-spec get_dn(PEMFile :: string()) -> Result when
    Result :: {ok, DN :: string()}.
%% ====================================================================
get_dn(PEMFile) ->
    try
        {ok, PemBin} = file:read_file(PEMFile),
        Cert = public_key:pem_decode(PemBin),
        [Leaf | Chain] = [public_key:pkix_decode_cert(DerCert, otp) || {'Certificate', DerCert, _} <- Cert],
        {ok, EEC} = gsi_handler:find_eec_cert(Leaf, Chain, gsi_handler:is_proxy_certificate(Leaf)),
        {rdnSequence, Rdn} = gsi_handler:proxy_subject(EEC),
        user_logic:rdn_sequence_to_dn_string(Rdn)
    catch
        _:Error -> throw(io_lib:format("Can not get dn: ~p", [Error]))
    end.


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


%% map_hostname/1
%% ====================================================================
%% @doc Maps machine hostname to worker name
-spec map_hostname(Hostname :: string()) -> Result when
    Result :: atom() | no_return().
%% ====================================================================
map_hostname("149.156.10.162") -> 'worker@veil-d01.grid.cyf-kr.edu.pl';
map_hostname("149.156.10.163") -> 'worker@veil-d02.grid.cyf-kr.edu.pl';
map_hostname("149.156.10.164") -> 'worker@veil-d03.grid.cyf-kr.edu.pl';
map_hostname("149.156.10.165") -> 'worker@veil-d04.grid.cyf-kr.edu.pl';
map_hostname(Other) -> throw(io_lib:fwrite("Unknown hostname: ~p", [Other])).


%% make_dir/1
%% ====================================================================
%% @doc Creates directory with parent directories
-spec make_dir(Dir :: string()) -> ok | {error, Reason :: term()}.
%% ====================================================================
make_dir(Dir) ->
    [Root | Leafs] = filename:split(Dir),
    case make_dir(Root, Leafs) of
        ok -> ok;
        {error, eexist} -> ok;
        Other -> Other
    end.


%% make_dir/1
%% ====================================================================
%% @doc Creates directory with parent directories.
%% Should not be used directly, use make_dir/1 instead.
-spec make_dir(Root :: string(), Leafs :: [string()]) -> ok | {error, Reason :: term()}.
%% ====================================================================
make_dir(Root, []) ->
    file:make_dir(Root);
make_dir(Root, [Leaf | Leafs]) ->
    file:make_dir(Root),
    make_dir(filename:join(Root, Leaf), Leafs).