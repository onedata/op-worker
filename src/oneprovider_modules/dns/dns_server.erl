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

-include_lib("kernel/src/inet_dns.hrl").
-include_lib("ctool/include/logging.hrl").
-include("oneprovider_modules/dns/dns_worker.hrl").
-include("registered_names.hrl").
-include("supervision_macros.hrl").


% Maximum size of UDP DNS reply (without EDNS) as in RFC1035.
-define(NOEDNS_UDP_SIZE, 512).


%% ====================================================================
%% API
%% ====================================================================
-export([start/6, stop/0, handle_query/2, start_listening/3]).

% Server configuration (in runtime)
-export([set_handler_module/1, get_handler_module/0, set_dns_response_ttl/1, get_dns_response_ttl/0, set_max_edns_udp_size/1, get_max_edns_udp_size/0]).

% TODO
-export([test/1, test/2]).


%% start/6
%% ====================================================================
%% @doc Starts a DNS server. The server will listen on chosen port (UDP and TCP).
%% QueryHandlerModule must conform to dns_query_handler_behaviour.
%% @end
%% ====================================================================
-spec start(DNSPort :: integer(), QueryHandlerModule :: atom(), DNSResponseTTL :: integer(), EdnsMaxUdpSize :: integer(),
    TCPNumAcceptors :: integer(), TCPTimeout :: integer()) -> ok | {error, Reason :: term()}.
%% ====================================================================
start(DNSPort, QueryHandlerModule, DNSResponseTTL, EdnsMaxUdpSize, TCPNumAcceptors, TCPTimeout) ->
    % TODO supervisor do argsow
    set_handler_module(QueryHandlerModule),
    set_dns_response_ttl(DNSResponseTTL),
    set_max_edns_udp_size(EdnsMaxUdpSize),
    case proc_lib:start(?MODULE, start_listening, [DNSPort, TCPNumAcceptors, TCPTimeout], 500) of
        {ok, _Pid} ->
            ok;
        Error ->
            {error, {cannot_start_server, Error}}
    end.


stop() ->
    spawn(fun() -> clear_children_and_listeners() end),
    ok.


handle_query(Packet, Transport) ->
    case inet_dns:decode(Packet) of
        {ok, #dns_rec{header = Header, qdlist = QDList, anlist = _AnList, nslist = _NSList, arlist = _ARList} = DNSRec} ->
            case validate_query(DNSRec) of
                form_error ->
                    generate_answer(DNSRec#dns_rec{header = inet_dns:make_header(Header, rcode, ?FORMERR)}, Transport);
                bad_version ->
                    generate_answer(DNSRec#dns_rec{header = inet_dns:make_header(Header, rcode, ?BADVERS)}, Transport);
                ok ->
                    ?dump(DNSRec),
                    HandlerModule = get_handler_module(),
                    [#dns_query{domain = Domain, type = Type, class = Class}] = QDList,
                    case call_handler_module(HandlerModule, list_to_binary(Domain), Type) of
                        serv_fail ->
                            generate_answer(DNSRec#dns_rec{header = inet_dns:make_header(Header, rcode, ?SERVFAIL)}, Transport);
                        nx_domain ->
                            generate_answer(DNSRec#dns_rec{header = inet_dns:make_header(Header, rcode, ?NXDOMAIN)}, Transport);
                        not_impl ->
                            generate_answer(DNSRec#dns_rec{header = inet_dns:make_header(Header, rcode, ?NOTIMP)}, Transport);
                        refused ->
                            generate_answer(DNSRec#dns_rec{header = inet_dns:make_header(Header, rcode, ?REFUSED)}, Transport);
                        {ok, ResponseList} ->
                            AnList = lists:map(
                                fun(Data) ->
                                    inet_dns:make_rr([
                                        {data, Data},
                                        {domain, Domain},
                                        {type, Type},
                                        {ttl, get_dns_response_ttl()},
                                        {class, Class}])
                                end, ResponseList),
                            %% Set AA flag (Authoritative Answer) if the server can answer - this DNS server can handle only queries
                            %% concerning its domain, so all valid answers are authoritative.
                            generate_answer(DNSRec#dns_rec{header = inet_dns:make_header(Header, aa, AnList /= []), anlist = AnList}, Transport)
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
%% @doc Checks if a query is valid.
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
%% @doc Returns function name, that should be called to handle a query of given type.
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


%% form_answer/2
%% ====================================================================
%% @doc Encodes a DNS record and returns a tuple accepted by dns_xxx_handler modules.
%% Modifies flags in header according to server's capabilities.
%% @end
%% ====================================================================
-spec generate_answer(DNSRec :: #dns_rec{}, Transport :: atom()) -> {ok, binary()}.
%% ====================================================================
generate_answer(DNSRec, Transport) ->
    % Update the header - no recursion available, qr=true -> it's a response
    Header2 = inet_dns:make_header(DNSRec#dns_rec.header, ra, false),
    NewHeader = inet_dns:make_header(Header2, qr, true),
    DNSRecUpdatedHeader = DNSRec#dns_rec{header = NewHeader},
    % Check if OPT RR is present. If so, check client's max upd payload size and set the value to server's max udp.
    {NewDnsRec, ClientMaxUDP} = case DNSRecUpdatedHeader#dns_rec.arlist of
                                    [#dns_rr_opt{udp_payload_size = ClMaxUDP} = RROPT] ->
                                        {DNSRecUpdatedHeader#dns_rec{arlist = [RROPT#dns_rr_opt{udp_payload_size = get_max_edns_udp_size()}]}, ClMaxUDP};
                                    [] ->
                                        {DNSRecUpdatedHeader, undefined}
                                end,
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
encode_udp(#dns_rec{arlist = [#dns_rr_opt{} = RROPT]} = DNSRec, ClientMaxUDP) ->
    TruncationSize = case ClientMaxUDP of
                         undefined ->
                             ?NOEDNS_UDP_SIZE;
                         Value ->
                             % If the client advertised a value, accept it but don't exceed the range:
                             % [512, MAX_UDP_SIZE]
                             max(?NOEDNS_UDP_SIZE, min(get_max_edns_udp_size(), Value))
                     end,
    Packet = inet_dns:encode(DNSRec#dns_rec{arlist = [RROPT#dns_rr_opt{udp_payload_size = 1234}]}),
    case size(Packet) > TruncationSize of
        true ->
            ?dump(Packet),
            Packet2 = inet_dns:encode(DNSRec#dns_rec{header = inet_dns:make_header(DNSRec#dns_rec.header, tc, true), arlist = [RROPT#dns_rr_opt{udp_payload_size = 1234}]}),
            ?dump(Packet2);
        false ->
            ?dump(ok)
    end.


%% start_listening/3
%% ====================================================================
%% @doc Starts dns listeners and terminates dns_worker process in case of error.
%% @end
%% ====================================================================
-spec start_listening(DNSPort :: integer(), TCPNumAcceptors :: integer(), TCPTimeout :: integer()) -> ok.
%% ====================================================================
start_listening(DNSPort, TCPNumAcceptors, TCPTimeout) ->
    try
        UDPChild = ?Sup_Child(?DNS_UDP, dns_udp_handler, permanent, [DNSPort]),
        TCPOptions = [{packet, 2}, {dns_tcp_timeout, TCPTimeout}, {keepalive, true}],

        proc_lib:init_ack({ok, self()}),

        try
            supervisor:delete_child(?Supervisor_Name, ?DNS_UDP),
            ?debug("DNS UDP child has exited")
        catch
            _:_ -> ok
        end,

        try
            ranch:stop_listener(dns_tcp_listener),
            ?debug("dns_tcp_listener has exited")
        catch
            _:_ -> ok
        end,

        {ok, Pid} = supervisor:start_child(?Supervisor_Name, UDPChild),
        start_tcp_listener(TCPNumAcceptors, DNSPort, TCPOptions, Pid)
    catch
        _:Reason -> ?error("DNS Error during starting listeners, ~p", [Reason]),
            gen_server:cast({global, ?CCM}, {stop_worker, node(), ?MODULE}),
            ?info("Terminating ~p", [?MODULE])
    end,
    ok.


%% start_tcp_listener/4
%% ====================================================================
%% @doc Starts tcp listener and handles case when udp listener has already started, but
%% tcp listener is unable to do so.
%% @end
%% ====================================================================
-spec start_tcp_listener(AcceptorPool, Port, TransportOpts, Pid) -> ok when
    AcceptorPool :: pos_integer(),
    Port :: pos_integer(),
    TransportOpts :: list(),
    Pid :: pid().
%% ====================================================================
start_tcp_listener(AcceptorPool, Port, TransportOpts, Pid) ->
    try
        {ok, _} = ranch:start_listener(dns_tcp_listener, AcceptorPool, ranch_tcp, [{port, Port}],
            dns_tcp_handler, TransportOpts)
    catch
        _:RanchError -> ok = supervisor:terminate_child(?Supervisor_Name, Pid),
            supervisor:delete_child(?Supervisor_Name, Pid),
            ?error("Start DNS TCP listener error, ~p", [RanchError]),
            throw(RanchError)
    end,
    ok.


%% clear_children_and_listeners/0
%% ====================================================================
%% @doc Clears listeners and created children
%% @end
%% ====================================================================
-spec clear_children_and_listeners() -> ok.
%% ====================================================================
clear_children_and_listeners() ->
    SafelyExecute = fun(Invoke, Log) ->
        try
            Invoke()
        catch
            _:Error -> ?error(Log, [Error])
        end
    end,

    SafelyExecute(fun() -> ok = supervisor:terminate_child(?Supervisor_Name, ?DNS_UDP),
        supervisor:delete_child(?Supervisor_Name, ?DNS_UDP)
    end,
        "Error stopping dns udp listener, status ~p"),

    SafelyExecute(fun() -> ok = ranch:stop_listener(dns_tcp_listener)
    end,
        "Error stopping dns tcp listener, status ~p"),
    ok.


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
                type = a,
                class = in
            }],
            arlist = [{dns_rr_opt, ".", opt, 1280, 0, 0, 0, <<>>}]
        }),
    {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
    gen_udp:send(Socket, NS, 53, Query),
    {ok, {NS, 53, Reply}} = gen_udp:recv(Socket, 65535),
    inet_dns:decode(Reply).