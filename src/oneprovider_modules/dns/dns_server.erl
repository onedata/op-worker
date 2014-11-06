%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module handles DNS protocol requests (TCP and UDP) and allows using
%% custom query handlers.
%% @end
%% ===================================================================
-module(dns_server).

-include("dns.hrl").
-include_lib("ctool/include/logging.hrl").


% Maximum size of UDP DNS reply (without EDNS) as in RFC1035.
-define(NOEDNS_UDP_SIZE, 512).

%% Local name of the process waiting for dns udp messages
-define(DNS_UDP_LISTENER, dns_udp).

%% Module name of dns tcp ranch listener
-define(DNS_TCP_LISTENER, dns_tcp).

%% Timeout to wait for DNS listeners to start. After it, they are assumed to have failed to start.
-define(LISTENERS_START_TIMEOUT, 10000).


%% ====================================================================
%% API
%% ====================================================================
-export([start/8, stop/1, handle_query/2, start_listening/5]).

% Functions useful in qury handler modules
-export([answer_record/3, authority_record/3, additional_record/3, authoritative_answer_flag/1]).

% Server configuration (in runtime)
-export([set_handler_module/1, get_handler_module/0, set_dns_response_ttl/1, get_dns_response_ttl/0, set_max_edns_udp_size/1, get_max_edns_udp_size/0]).

-export([test/1, test/2]).

test(Domain) ->
    test(Domain, {8, 8, 8, 8}).

test(Domain, NS) ->
    Query = inet_dns:encode(
        #dns_rec{
            header = #dns_header{
                id = crypto:rand_uniform(1, 16#FFFF),
                opcode = 'query',
                rd = true
            },
            qdlist = [#dns_query{
                domain = Domain,
                type = soa,
                class = in
            }],
            arlist = [{dns_rr_opt, ".", opt, 1280, 0, 0, 0, <<>>}]
        }),
    {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
    gen_udp:send(Socket, NS, 53, Query),
    {ok, {NS, 53, Reply}} = gen_udp:recv(Socket, 65535),
    inet_dns:decode(Reply).


%% start/8
%% ====================================================================
%% @doc Starts a DNS server. The server will listen on chosen port (UDP and TCP).
%% QueryHandlerModule must conform to dns_query_handler_behaviour.
%% The server is started in a new process.
%% OnFailureFun is evalueated by the process if the server fails to start.
%% @end
%% ====================================================================
-spec start(SupervisorName :: atom(), DNSPort :: integer(), QueryHandlerModule :: atom(), DNSResponseTTL :: integer(), EdnsMaxUdpSize :: integer(),
    TCPNumAcceptors :: integer(), TCPTimeout :: integer(), OnFailureFun :: function()) -> ok | {error, Reason :: term()}.
%% ====================================================================
start(SupervisorName, DNSPort, QueryHandlerModule, DNSResponseTTL, EdnsMaxUdpSize, TCPNumAcceptors, TCPTimeout, OnFailureFun) ->
    set_handler_module(QueryHandlerModule),
    set_dns_response_ttl(DNSResponseTTL),
    set_max_edns_udp_size(EdnsMaxUdpSize),
    % Listeners start is done in another process.
    % This is because this function is often called by the same supervisor process
    % that supervises the listeners - which causes a deadlock.
    case proc_lib:start(?MODULE, start_listening, [SupervisorName, DNSPort, TCPNumAcceptors, TCPTimeout, OnFailureFun], ?LISTENERS_START_TIMEOUT) of
        ok -> ok;
        {error, Reason} -> {error, Reason}
    end.


%% stop/1
%% ====================================================================
%% @doc Stops the DNS server and cleans up.
%% @end
%% ====================================================================
-spec stop(SupervisorName :: atom()) -> ok.
%% ====================================================================
stop(SupervisorName) ->
    % Cleaning up must be done in another process. This is because this function is often called by the same supervisor process
    % that supervises the listeners - which causes a deadlock.
    spawn(fun() -> clear_children_and_listeners(SupervisorName, true) end),
    ok.


%% handle_query/2
%% ====================================================================
%% @doc Handles a DNS request. This function is called from dns_upd_handler or dns_tcp_handler after
%% they have received a request. After evaluation, the response is sent back to the client.
%% @end
%% ====================================================================
-spec handle_query(Packet :: binary(), Transport :: udp | tcp) -> ok.
%% ====================================================================
handle_query(Packet, Transport) ->
    case inet_dns:decode(Packet) of
        {ok, #dns_rec{header = Header, qdlist = QDList, anlist = _AnList, nslist = _NSList, arlist = ARList} = DNSRecWithAdditionalSection} ->
            % Detach OPT RR from the DNS query record an proceed with processing it - the OPT RR will be added during answer generation
            DNSRec = DNSRecWithAdditionalSection#dns_rec{arlist = []},
            OPTRR = case ARList of
                        [] -> undefined;
                        [#dns_rr_opt{} = OptRR] -> OptRR
                    end,
            case validate_query(DNSRec) of
                form_error ->
                    generate_answer(DNSRec#dns_rec{header = inet_dns:make_header(Header, rcode, ?FORMERR)}, OPTRR, Transport);
                bad_version ->
                    generate_answer(DNSRec#dns_rec{header = inet_dns:make_header(Header, rcode, ?BADVERS)}, OPTRR, Transport);
                ok ->
                    HandlerModule = get_handler_module(),
                    [#dns_query{domain = Domain, type = Type, class = Class}] = QDList,
                    case call_handler_module(HandlerModule, string:to_lower(Domain), Type) of
                        serv_fail ->
                            generate_answer(DNSRec#dns_rec{header = inet_dns:make_header(Header, rcode, ?SERVFAIL)}, OPTRR, Transport);
                        nx_domain ->
                            generate_answer(DNSRec#dns_rec{header = inet_dns:make_header(Header, rcode, ?NXDOMAIN)}, OPTRR, Transport);
                        not_impl ->
                            generate_answer(DNSRec#dns_rec{header = inet_dns:make_header(Header, rcode, ?NOTIMP)}, OPTRR, Transport);
                        refused ->
                            generate_answer(DNSRec#dns_rec{header = inet_dns:make_header(Header, rcode, ?REFUSED)}, OPTRR, Transport);
                        {ok, ResponseList} ->
                            %% If there was an OPT RR, it will be concated at the end of the process
                            NewRec = lists:foldl(
                                fun(CurrentRecord, #dns_rec{header = CurrHeader, anlist = CurrAnList, nslist = CurrNSList, arlist = CurrArList} = CurrRec) ->
                                    case CurrentRecord of
                                        {aa, Flag} ->
                                            CurrRec#dns_rec{header = inet_dns:make_header(CurrHeader, aa, Flag)};
                                        {Section, CurrDomain, CurrType, CurrData} ->
                                            RR = inet_dns:make_rr([
                                                {data, CurrData},
                                                {domain, CurrDomain},
                                                {type, CurrType},
                                                {ttl, get_dns_response_ttl()},
                                                {class, Class}]),
                                            case Section of
                                                answer -> CurrRec#dns_rec{anlist = CurrAnList ++ [RR]};
                                                authority -> CurrRec#dns_rec{nslist = CurrNSList ++ [RR]};
                                                additional -> CurrRec#dns_rec{arlist = CurrArList ++ [RR]}
                                            end
                                    end
                                end, DNSRec#dns_rec{}, ResponseList),
                            generate_answer(NewRec#dns_rec{arlist = NewRec#dns_rec.arlist}, OPTRR, Transport)
                    end
            end;
        _ ->
            {error, uprocessable}
    end.


%% validate_query/1
%% ====================================================================
%% @doc Checks if a query is valid.
%% @end
%% ====================================================================
-spec validate_query(#dns_rec{}) -> ok | form_error | bad_version.
%% ====================================================================
validate_query(DNSRec) ->
    case DNSRec of
        #dns_rec{qdlist = [#dns_query{}], anlist = [], nslist = [], arlist = []} ->
            % The record includes a question section and no OPT RR section - ok
            ok;
        #dns_rec{qdlist = [#dns_query{}], anlist = [], nslist = [], arlist = [#dns_rr_opt{version = Version}]} ->
            % The record includes a question section and an OPT RR section - check EDNS version
            case Version of
                0 -> ok;
                _ -> bad_version
            end;
        #dns_rec{} ->
            % Other - not ok
            form_error
    end.


%% call_handler_module/3
%% ====================================================================
%% @doc Calls the handler module to handle the query or returns not_impl if
%% this kind of query is not accepted by the server.
%% @end
%% ====================================================================
-spec call_handler_module(HandlerModule :: atom(), Domain :: binary(), Type :: atom()) ->
    {ok, [binary()]} | serv_fail | nx_domain  | not_impl | refused.
%% ====================================================================
call_handler_module(HandlerModule, Domain, Type) ->
    case type_to_fun(Type) of
        not_impl ->
            not_impl;
        Fun ->
            erlang:apply(HandlerModule, Fun, [Domain])
    end.


%% type_to_fun/1
%% ====================================================================
%% @doc Returns a function that should be called to handle a query of given type.
%% Those functions are defined in dns_query_handler_behaviour.
%% @end
%% ====================================================================
-spec type_to_fun(QueryType :: atom()) -> atom().
%% ====================================================================
type_to_fun(?S_A) -> handle_a;
type_to_fun(?S_NS) -> handle_ns;
type_to_fun(?S_CNAME) -> handle_cname;
type_to_fun(?S_SOA) -> handle_soa;
type_to_fun(?S_WKS) -> handle_wks;
type_to_fun(?S_PTR) -> handle_ptr;
type_to_fun(?S_HINFO) -> handle_hinfo;
type_to_fun(?S_MINFO) -> handle_minfo;
type_to_fun(?S_MX) -> handle_mx;
type_to_fun(?S_TXT) -> handle_txt;
type_to_fun(_) -> not_impl.


%% generate_answer/3
%% ====================================================================
%% @doc Encodes a DNS record and returns a tuple accepted by dns_xxx_handler modules.
%% Modifies flags in header according to server's capabilities.
%% If there was an OPT RR record in request, it modifies it properly and concates to the ADDITIONAL section.
%% @end
%% ====================================================================
-spec generate_answer(DNSRec :: #dns_rec{}, OPTRR :: #dns_rr_opt{}, Transport :: atom()) -> {ok, binary()}.
%% ====================================================================
generate_answer(DNSRec, OPTRR, Transport) ->
    % Update the header - no recursion available, qr=true -> it's a response
    Header2 = inet_dns:make_header(DNSRec#dns_rec.header, ra, false),
    NewHeader = inet_dns:make_header(Header2, qr, true),
    DNSRecUpdatedHeader = DNSRec#dns_rec{header = NewHeader},
    % Check if OPT RR is present. If so, retrieve client's max upd payload size and set the value to server's max udp.
    {NewDnsRec, ClientMaxUDP} = case OPTRR of
                                    undefined ->
                                        {DNSRecUpdatedHeader, undefined};
                                    #dns_rr_opt{udp_payload_size = ClMaxUDP} = RROPT ->
                                        NewRROPT = RROPT#dns_rr_opt{udp_payload_size = get_max_edns_udp_size()},
                                        {DNSRecUpdatedHeader#dns_rec{arlist = DNSRecUpdatedHeader#dns_rec.arlist ++ [NewRROPT]}, ClMaxUDP}
                                end,
    ?dump(NewDnsRec),
    case Transport of
        udp -> {ok, encode_udp(NewDnsRec, ClientMaxUDP)};
        tcp -> {ok, inet_dns:encode(NewDnsRec)}
    end.


%% encode_udp/2
%% ====================================================================
%% @doc Encodes a DNS record and truncates it if required.
%% @end
%% ====================================================================
-spec encode_udp(DNSRec :: #dns_rec{}, ClientMaxUDP :: integer() | undefined) -> binary().
%% ====================================================================
encode_udp(#dns_rec{} = DNSRec, ClientMaxUDP) ->
    TruncationSize = case ClientMaxUDP of
                         undefined ->
                             ?NOEDNS_UDP_SIZE;
                         Value ->
                             % If the client advertised a value, accept it but don't exceed the range [512, MAX_UDP_SIZE]
                             max(?NOEDNS_UDP_SIZE, min(get_max_edns_udp_size(), Value))
                     end,
    Packet = inet_dns:encode(DNSRec),
    case size(Packet) > TruncationSize of
        true ->
            NumBits = (8 * TruncationSize),
            % Truncate the packet
            <<TruncatedPacket:NumBits, _/binary>> = Packet,
            % Set the TC (truncation) flag, which is the 23th bit in header
            <<Head:22, _TC:1, LastBit:1, Tail/binary>> = <<TruncatedPacket:NumBits>>,
            NewPacket = <<Head:22, 1:1, LastBit:1, Tail/binary>>,
            NewPacket;
        false ->
            Packet
    end.


%% start_listening/5
%% ====================================================================
%% @doc Starts dns listeners and terminates dns_worker process in case of error.
%% OnFailureFun is evalueated if the server fails to start.
%% @end
%% ====================================================================
-spec start_listening(SupervisorName :: atom(), DNSPort :: integer(), TCPNumAcceptors :: integer(), TCPTimeout :: integer(), OnFailureFun :: function()) -> ok.
%% ====================================================================
start_listening(SupervisorName, DNSPort, TCPNumAcceptors, TCPTimeout, OnFailureFun) ->
    try
        proc_lib:init_ack(ok),
        UDPChild = {?DNS_UDP_LISTENER, {dns_udp_handler, start_link, [DNSPort]}, permanent, 5000, worker, [dns_udp_handler]},
        TCPOptions = [{packet, 2}, {dns_tcp_timeout, TCPTimeout}, {keepalive, true}],

        % Start the UDP listener
        {ok, Pid} = supervisor:start_child(SupervisorName, UDPChild),
        % Start the TCP listener. In case of an error, stop the UDP listener
        try
            {ok, _} = ranch:start_listener(?DNS_TCP_LISTENER, TCPNumAcceptors, ranch_tcp, [{port, DNSPort}],
                dns_tcp_handler, TCPOptions)
        catch
            _:RanchError ->
                supervisor:terminate_child(SupervisorName, Pid),
                supervisor:delete_child(SupervisorName, Pid),
                ?error("Error while starting DNS TCP, ~p", [RanchError]),
                throw(RanchError)
        end
    catch
        _:Reason ->
            ?error("DNS Error during starting listeners, ~p", [Reason]),
            OnFailureFun()
    end,
    ok.


%% authoritative_answer_flag/1
%% ====================================================================
%% @doc Convenience function used from a dns query handler. Creates a term that will cause
%% the AA (authoritative answer) flag to be set to desired value in response.
%% The term should be put in list returned from handle_xxx function.
%% @end
%% ====================================================================
-spec authoritative_answer_flag(Flag :: boolean()) -> ok.
%% ====================================================================
authoritative_answer_flag(Flag) ->
    {aa, Flag}.


%% answer_record/3
%% ====================================================================
%% @doc Convenience function used from a dns query handler. Creates a term that will end up
%% as a record in ANSWER section of DNS reponse.
%% The term should be put in list returned from handle_xxx function.
%% @end
%% ====================================================================
-spec answer_record(Domain :: string(), Type :: dns_query_type(), Data :: term()) -> ok.
%% ====================================================================
answer_record(Domain, Type, Data) ->
    {answer, Domain, Type, Data}.


%% authority_record/3
%% ====================================================================
%% @doc Convenience function used from a dns query handler. Creates a term that will end up
%% as a record in AUTHORITY section of DNS reponse.
%% The term should be put in list returned from handle_xxx function.
%% @end
%% ====================================================================
-spec authority_record(Domain :: string(), Type :: dns_query_type(), Data :: term()) -> ok.
%% ====================================================================
authority_record(Domain, Type, Data) ->
    {authority, Domain, Type, Data}.


%% additional_record/3
%% ====================================================================
%% @doc Convenience function used from a dns query handler. Creates a term that will end up
%% as a record in ADDITIONAL section of DNS reponse.
%% The term should be put in list returned from handle_xxx function.
%% @end
%% ====================================================================
-spec additional_record(Domain :: string(), Type :: dns_query_type(), Data :: term()) -> ok.
%% ====================================================================
additional_record(Domain, Type, Data) ->
    {additional, Domain, Type, Data}.


%% set_handler_module/1
%% ====================================================================
%% @doc Saves query handler module in application env.
%% @end
%% ====================================================================
-spec set_handler_module(Module :: atom()) -> ok.
%% ====================================================================
set_handler_module(Module) ->
    ok = application:set_env(ctool, query_handler_module, Module).


%% get_handler_module/0
%% ====================================================================
%% @doc Retrieves query handler module from application env.
%% @end
%% ====================================================================
-spec get_handler_module() -> atom().
%% ====================================================================
get_handler_module() ->
    {ok, Module} = application:get_env(ctool, query_handler_module),
    Module.


%% set_dns_response_ttl/1
%% ====================================================================
%% @doc Saves DNS response TTL in application env.
%% @end
%% ====================================================================
-spec set_dns_response_ttl(TTLInt :: integer()) -> ok.
%% ====================================================================
set_dns_response_ttl(TTLInt) ->
    ok = application:set_env(ctool, dns_response_ttl, TTLInt).


%% get_dns_response_ttl/0
%% ====================================================================
%% @doc Retrieves DNS response TTL from application env.
%% @end
%% ====================================================================
-spec get_dns_response_ttl() -> integer().
%% ====================================================================
get_dns_response_ttl() ->
    {ok, TTLInt} = application:get_env(ctool, dns_response_ttl),
    TTLInt.


%% set_max_udp_size/1
%% ====================================================================
%% @doc Saves DNS response TTL in application env.
%% @end
%% ====================================================================
-spec set_max_edns_udp_size(Size :: integer()) -> ok.
%% ====================================================================
set_max_edns_udp_size(Size) ->
    ok = application:set_env(ctool, edns_max_udp_size, Size).


%% get_max_udp_size/0
%% ====================================================================
%% @doc Retrieves DNS response TTL from application env.
%% @end
%% ====================================================================
-spec get_max_edns_udp_size() -> integer().
%% ====================================================================
get_max_edns_udp_size() ->
    {ok, Size} = application:get_env(ctool, edns_max_udp_size),
    Size.


%% ===================================================================
%% Internal functions
%% ===================================================================

%% clear_children_and_listeners/2
%% ====================================================================
%% @doc Terminates listeners and created children, if they exist.
%% @end
%% ====================================================================
-spec clear_children_and_listeners(SupervisorName :: atom(), ReportErrors :: boolean()) -> ok.
%% ====================================================================
clear_children_and_listeners(SupervisorName, ReportErrors) ->
    try
        SupChildren = supervisor:which_children(SupervisorName),
        case lists:keyfind(?DNS_UDP_LISTENER, 1, SupChildren) of
            {?DNS_UDP_LISTENER, _, _, _} ->
                ok = supervisor:terminate_child(SupervisorName, ?DNS_UDP_LISTENER),
                supervisor:delete_child(SupervisorName, ?DNS_UDP_LISTENER),
                ?debug("DNS UDP child has exited");
            _ ->
                ok
        end
    catch
        _:Error1 ->
            case ReportErrors of
                true -> ?error_stacktrace("Error stopping dns udp listener, status ~p", [Error1]);
                false -> ok
            end
    end,

    try
        ok = ranch:stop_listener(?DNS_TCP_LISTENER),
        ?debug("dns_tcp_listener has exited")
    catch
        _:Error2 ->
            case ReportErrors of
                true -> ?error_stacktrace("Error stopping dns tcp listener, status ~p", [Error2]);
                false -> ok
            end
    end,
    ok.