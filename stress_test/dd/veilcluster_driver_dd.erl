-module(veilcluster_driver_dd).
-export([new/1, run/4, setup/0]).

-include("basho_bench.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").

-define(LogLoop, 100).
-define(LogLoopTime, 60000000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TEST DRIVER CALLBACKS %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Runs once per each test node at begging of a test (before any new/1 is called)
setup() ->
    try 
        %% Init net kernet in order to connect to cluster
        NetKernel = net_kernel:start([list_to_atom("tester@" ++ net_adm:localhost()), longnames]),
        ?DEBUG("net_kernel ans: ~p~n", [NetKernel]),
        erlang:set_cookie(node(), veil_cluster_node),
        ?INFO("SETUP: ~p", [basho_bench_config:get(client_id)]),
        InitRes = 
            case basho_bench_config:get(client_id) of 
                1 -> catch setup_storages(); %% If its the first test node, initialize cluster 
                _ -> timer:sleep(2000) %% Otherwise wait for main node to finish
                                       %% TODO: implement better, more deterministic way of synchronising test nodes (e.g. via ready-ping)
            end,
        ?DEBUG("Setup: ~p~n", [InitRes])
    catch
      E1:E2 -> ?DEBUG("setup error: ~p:~p~n", [E1, E2])
    end.

new(Id) -> 
    VFSRoot = basho_bench_config:get(veilfs_root),
    Dir = VFSRoot ++ "/stress_test_" ++ basho_bench_config:get(build_id),
    file:make_dir(Dir),
    File = Dir ++ "/file_" ++ integer_to_list(Id), 

    %% Open test file (each test process gets different file name like "file_Id")
    Device = 
        case open_helper(File, {error, first_try}, 20) of 
            {error, Reason} ->
                ?ERROR("new/1 error: ~p", [Reason]),
                Reason;
            IO -> IO
    end,
    ?DEBUG("dd file: ~p~n", [File]),
    BlockSize = basho_bench_config:get(block_size),
    Data = [0 || _X <- lists:seq(1, 1024*BlockSize)],
    {ok, {Device, 0, Data, {?LogLoop, os:timestamp()}}}.


%% Only 'write' action is implemented right now
run(write, _KeyGen, _ValueGen, {Dev, _Offset, _Data} = State) when is_atom(Dev) ->
    timer:sleep(1000), %% Dont generate more then one error per sec when open/2 is failing
    {error, {open, Dev}, State};
run(write, _KeyGen, _ValueGen, {Dev, Offset, Data, {LogLoop, LastTime}}) ->
  NewLoopValue = case LogLoop of
                   0 ->
                     Now = os:timestamp(),
                     case timer:now_diff(Now, LastTime) > ?LogLoopTime of
                       true ->
                         ?DEBUG("Loop log~n", []),
                         {?LogLoop, Now};
                       false ->
                         {?LogLoop, LastTime}
                     end;
                   _ ->
                     {LogLoop -1, LastTime}
                 end,

    NewState = {Dev, (Offset + length(Data)) rem (basho_bench_config:get(max_filesize) * 1024*1024), Data, NewLoopValue},
    case file:pwrite(Dev, 0, Data) of 
        ok -> {ok, NewState};
        {error, Reason} -> 
            ?DEBUG("Error (file: ~p, offset: ~p): ~p", [Dev, Offset, Reason]),
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
    Cert = basho_bench_config:get(cert_file),
    [Host | _] = basho_bench_config:get(cluster_hosts),
    rpc:call(list_to_atom("worker@" ++ Host), user_logic, create_user, ["test_user", "Test Name", [], "test@test.com", [get_dn(Cert)]]),
    Groups = #fuse_group_info{name = ?CLUSTER_FUSE_ID, storage_helper = #storage_helper_info{name = "DirectIO", init_args = ["/mnt/gluster"]}},
    rpc:call(list_to_atom("worker@" ++ Host), fslogic_storage, insert_storage, ["ClusterProxy", [], [Groups]]),
    rpc:call(list_to_atom("worker@" ++ Host), os, cmd, ["rm -rf /mnt/gluster/*"]).

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
    ?DEBUG("open error ~p (~p), retring in ~p", [_Error, Retry, M rem 100]),
    open_helper(File, file:open(File, [write, raw]), Retry - 1);
open_helper(_, _, _) ->
    {error, open_failed}.
