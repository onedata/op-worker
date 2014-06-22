%% ===================================================================
%% @author Rafał Słota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc:This test checks how many simple messages may be sent and received
%% in worst case (open/close socket before/after each operation).
%% @end
%% ===================================================================

-module(veilcluster_driver_ping).
-export([new/1, run/4]).

-include("basho_bench.hrl").
-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").

%% ====================================================================
%% Test driver callbacks
%% ====================================================================

%% new/1
%% ====================================================================
%% @doc Creates new worker with integer id
-spec new(Id :: integer()) -> Result when
    Result :: {ok, term()} | {error, Reason :: term()}.
%% ====================================================================
new(Id) ->
    try
        ?INFO("Initializing worker with id: ~p~n", [Id]),
        Hosts = basho_bench_config:get(cluster_hosts),
        CertFile = basho_bench_config:get(cert_file),

        Pong = #atom{value = "pong"},
        PongBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Pong)),
        PongAns = #answer{answer_status = "ok", worker_answer = PongBytes},
        PongAnsBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_answer(PongAns)),

        Args = {Hosts, CertFile, PongAnsBytes},
        ?INFO("Worker with id: ~p initialized successfully with arguments: ~p", [Id, Args]),
        {ok, Args}
    catch
        E1:E2 ->
            ?ERROR("Initialization error for worker with id: ~p: ~p", [Id, {E1, E2}]),
            {error, {E1, E2}}
    end.


%% run/4
%% ====================================================================
%% @doc Runs an operation using one of workers
-spec run(Operation :: atom(), KeyGen :: fun(), ValueGen :: fun(), State :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term(), NewState :: term()}.
%% ====================================================================
run(Operation, KeyGen, _ValueGen, {Hosts, CertFile, PongAnsBytes}) ->
    NewState = {Hosts, CertFile, PongAnsBytes},
    try
        Host = lists:nth((KeyGen() rem length(Hosts)) + 1, Hosts),

        case wss:connect(Host, 5555, [{certfile, CertFile}, {cacertfile, CertFile}, auto_handshake]) of
            {ok, Socket} ->
                Res = try ping(Operation, Socket, PongAnsBytes) of
                          ok -> {ok, NewState}
                      catch
                          R1:R2 -> {error, {R1, R2}, NewState}
                      end,
                wss:close(Socket),
                Res;
            {error, Error} -> {error, {connect, Error}, NewState};
            Other -> {error, {unknown_error, Other}, NewState}
        end
    catch
        E1:E2 ->
            {error, {error_thrown, E1, E2}, NewState}
    end.


%% ====================================================================
%% Helper functions
%% ====================================================================

%% ping/3
%% ====================================================================
%% @doc Pings given module using socket
-spec ping(Module :: module(), Socket :: port(), PongAnsBytes :: binary()) -> Result when
    Result :: ok | no_return().
%% ====================================================================
ping(Module, Socket, PongAnsBytes) ->
    Ping = #atom{value = "ping"},
    PingBytes = erlang:iolist_to_binary(communication_protocol_pb:encode_atom(Ping)),

    Message = #clustermsg{module_name = atom_to_list(Module), message_type = "atom",
        message_decoder_name = "communication_protocol", answer_type = "atom",
        answer_decoder_name = "communication_protocol", synch = true, protocol_version = 1, input = PingBytes},
    Msg = erlang:iolist_to_binary(communication_protocol_pb:encode_clustermsg(Message)),

    wss:send(Socket, Msg),
    Ans =
        case wss:recv(Socket, 5000) of
            {ok, Ans1} -> Ans1;
            {error, Reason} -> throw({recv, Reason})
        end,

    case Ans of
        PongAnsBytes -> ok;
        Other -> throw({invalid_answer, Other})
    end.