-module(veilcluster_driver_dd).
-export([new/1, run/4, setup/0]).

-include("basho_bench.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TEST DRIVER CALLBACKS %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Runs once per each test node at begging of a test (before any new/1 is called)
setup() ->
    ?DEBUG("Test setup~n", []),
    try
        %% Init net kernet in order to connect to cluster
        NetKernel = net_kernel:start([list_to_atom("tester@" ++ net_adm:localhost()), longnames]),
        erlang:set_cookie(node(), veil_cluster_node),
        InitRes =
            case basho_bench_config:get(client_id) of
                1 -> catch setup_storages(); %% If its the first test node, initialize cluster
                _ -> timer:sleep(2000) %% Otherwise wait for main node to finish
            %% TODO: implement better, more deterministic way of synchronising test nodes (e.g. via ready-ping)
            end
    catch
        E1:E2 -> ?DEBUG("Setup error: ~p:~p~n", [E1, E2])
    end.

%% Test initialization
new(Id) ->
    ?DEBUG("Test initialization~n", []),
    VFSRoot = basho_bench_config:get(veilfs_root),
    Dir = VFSRoot ++ "/stress_test_" ++ basho_bench_config:get(build_id),
    make_dir(filename:split(Dir)),
    File = Dir ++ "/file_" ++ integer_to_list(Id),

    %% Open test file (each test process gets different file name like "file_Id")
    Device = case open_helper(File, {error, first_try}, 20) of
                 {error, Reason} ->
                     ?ERROR("Initialization error: ~p", [Reason]),
                     Reason;
                 IO -> IO
             end,
    BlockSize = basho_bench_config:get(block_size),
    Data = [0 || _X <- lists:seq(1, 1024 * BlockSize)],
    {ok, {Device, 0, Data}}.


%% Only 'write' action is implemented right now
run(write, _KeyGen, _ValueGen, {Dev, _Offset, _Data} = State) when is_atom(Dev) ->
    timer:sleep(1000), %% Dont generate more then one error per sec when open/2 is failing
    {error, {open, Dev}, State};
run(write, _KeyGen, _ValueGen, {Dev, Offset, Data}) ->
    NewState = {Dev, (Offset + length(Data)) rem (basho_bench_config:get(max_filesize) * 1024 * 1024), Data},
    case file:pwrite(Dev, 0, Data) of
        ok -> {ok, NewState};
        {error, Reason} ->
            ?DEBUG("Run error: (file: ~p, offset: ~p): ~p", [Dev, Offset, Reason]),
            {error, Reason, NewState}
    end;
run(Action, _KeyGen, _ValueGen, State) ->
    ?ERROR("Unknown action ~p with state ~p", [Action, State]),
    {error, unknown_action}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% PRIVETE HELPER METHODS %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Register test user and configure storages on cluster
setup_storages() ->
    ?DEBUG("Storage setup~n", []),
    Cert = basho_bench_config:get(cert_file),
    [Host | _] = basho_bench_config:get(cluster_hosts),
    Groups = #fuse_group_info{name = ?CLUSTER_FUSE_ID, storage_helper = #storage_helper_info{name = "DirectIO", init_args = ["/mnt/gluster"]}},
    rpc:call(map_hostname(Host), os, cmd, ["rm -rf /mnt/gluster/*"]),
    rpc:call(map_hostname(Host), fslogic_storage, insert_storage, ["ClusterProxy", [], [Groups]]),
    rpc:call(map_hostname(Host), user_logic, create_user, ["veilfstestuser", "Test Name", [], "test@test.com", [get_dn(Cert)]]).

%% Gets rDN list compatibile user_logic:create_user from PEM file
get_dn(PEMFile) ->
    {ok, PemBin} = file:read_file(PEMFile),
    Cert = public_key:pem_decode(PemBin),
    [Leaf | Chain] = [public_key:pkix_decode_cert(DerCert, otp) || {'Certificate', DerCert, _} <- Cert],
    {ok, EEC} = gsi_handler:find_eec_cert(Leaf, Chain, gsi_handler:is_proxy_certificate(Leaf)),
    {rdnSequence, Rdn} = gsi_handler:proxy_subject(EEC),
    {ok, DnString} = user_logic:rdn_sequence_to_dn_string(Rdn),
    DnString.

%% Opens requested file retring when error occurs
open_helper(_, {ok, IO}, _) ->
    IO;
open_helper(File, {error, _Error}, Retry) when Retry > 0 ->
    {_, _, M} = now(),
    timer:sleep(M rem 100),
    ?DEBUG("Open helper error ~p (~p), retring in ~p", [_Error, Retry, M rem 100]),
    open_helper(File, file:open(File, [write, raw]), Retry - 1);
open_helper(_, _, _) ->
    {error, open_failed}.

%% Maps ip address to node name
map_hostname("149.156.10.162") -> 'worker@veil-d01.grid.cyf-kr.edu.pl';
map_hostname("149.156.10.163") -> 'worker@veil-d02.grid.cyf-kr.edu.pl';
map_hostname("149.156.10.164") -> 'worker@veil-d03.grid.cyf-kr.edu.pl';
map_hostname("149.156.10.165") -> 'worker@veil-d04.grid.cyf-kr.edu.pl'.

%% Create directory with parent directories
make_dir([Root | Leafs]) ->
    make_dir(Root, Leafs).

make_dir(Root, []) ->
    file:make_dir(Root);
make_dir(Root, [Leaf | Leafs]) ->
    file:make_dir(Root),
    make_dir(filename:join(Root, Leaf), Leafs).