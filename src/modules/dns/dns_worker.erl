%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements {@link worker_plugin_behaviour} and
%%% manages a DNS server module.
%%% In addition, it implements {@link dns_handler_behaviour} -
%%% DNS query handling logic.
%%% @end
%%%-------------------------------------------------------------------
-module(dns_worker).
-author("Lukasz Opiola").

-behaviour(worker_plugin_behaviour).
-behaviour(dns_handler_behaviour).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("kernel/src/inet_dns.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% dns_handler_behaviour callbacks
-export([handle_a/1, handle_ns/1, handle_cname/1, handle_soa/1, handle_wks/1,
    handle_ptr/1, handle_hinfo/1, handle_minfo/1, handle_mx/1, handle_txt/1]).

%% export for unit tests
-ifdef(TEST).
-export([parse_domain/1]).
-endif.

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init([]) ->
    {ok, #{}};

init(InitialState) when is_map(InitialState) ->
    {ok, InitialState};

init(test) ->
    {ok, #{}};

init(_) ->
    throw(unknown_initial_state).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck,
    Result :: nagios_handler:healthcheck_response() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: [inet:ip4_address()],
    Reason :: term().

handle(ping) ->
    pong;

handle(healthcheck) ->
    _Reply = healthcheck();

handle({update_lb_advice, LBAdvice}) ->
    ?debug("DNS update of load_balancing advice: ~p", [LBAdvice]),
    ok = worker_host:state_put(?MODULE, last_update, now()),
    ok = worker_host:state_put(?MODULE, lb_advice, LBAdvice);

handle({handle_a, Domain}) ->
    LBAdvice = worker_host:state_get(?MODULE, lb_advice),
    ?debug("DNS A request: ~s, current advice: ~p", [Domain, LBAdvice]),
    case LBAdvice of
        undefined ->
            % The DNS server is still out of sync, return serv fail
            serv_fail;
        _ ->
            case parse_domain(Domain) of
                unknown_domain ->
                    % Unrecognizable domain
                    refused;
                {Prefix, _Suffix} ->
                    % Accept all prefixes that consist of one part
                    case string:str(Prefix, ".") =:= 0 andalso length(Prefix) > 0 of
                        true ->
                            % Prefix OK, return nodes to connect to
                            Nodes = load_balancing:choose_nodes_for_dns(LBAdvice),
                            {ok, TTL} = application:get_env(?APP_NAME, dns_a_response_ttl),
                            {ok,
                                    [dns_server:answer_record(Domain, TTL, ?S_A, IP) || IP <- Nodes] ++
                                    [dns_server:authoritative_answer_flag(true)]
                            };
                        false ->
                            % Return NX domain for other prefixes
                            nx_domain
                    end
            end
    end;

handle({handle_ns, Domain}) ->
    LBAdvice = worker_host:state_get(?MODULE, lb_advice),
    ?debug("DNS NS request: ~s, current advice: ~p", [Domain, LBAdvice]),
    case LBAdvice of
        undefined ->
            % The DNS server is still out of sync, return serv fail
            serv_fail;
        _ ->
            case parse_domain(Domain) of
                unknown_domain ->
                    % Unrecognizable domain
                    refused;
                {Prefix, _Suffix} ->
                    % Accept all prefixes that consist of one part
                    case string:str(Prefix, ".") =:= 0 andalso length(Prefix) > 0 of
                        true ->
                            % Prefix OK, return NS nodes of the cluster
                            Nodes = load_balancing:choose_ns_nodes_for_dns(LBAdvice),
                            {ok, TTL} = application:get_env(?APP_NAME, dns_ns_response_ttl),
                            {ok,
                                    [dns_server:answer_record(Domain, TTL, ?S_NS, inet_parse:ntoa(IP)) || IP <- Nodes] ++
                                    [dns_server:authoritative_answer_flag(true)]
                            };
                        false ->
                            nx_domain
                    end
            end
    end;

handle(_Request) ->
    ?log_bad_request(_Request),
    throw({unsupported_request, _Request}).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok.
cleanup() ->
    dns_server:stop(?APPLICATION_SUPERVISOR_NAME).


%%%===================================================================
%%% dns_handler_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type A.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_a(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_a(Domain) ->
    worker_proxy:call(dns_worker, {handle_a, Domain}).


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type NS.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_ns(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_ns(Domain) ->
    worker_proxy:call(dns_worker, {handle_ns, Domain}).


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type CNAME.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_cname(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_cname(_Domain) ->
    not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type MX.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_mx(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_mx(_Domain) ->
    not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type SOA.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_soa(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_soa(_Domain) ->
    not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type WKS.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_wks(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_wks(_Domain) ->
    not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type PTR.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_ptr(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_ptr(_Domain) ->
    not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type HINFO.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_hinfo(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_hinfo(_Domain) ->
    not_impl.

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type MINFO.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_minfo(Domain :: string()) -> dns_handler_behaviour:handler_reply().
%% ====================================================================
handle_minfo(_Domain) ->
    not_impl.

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type TXT.
%% See {@link dns_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_txt(Domain :: string()) -> dns_handler_behaviour:handler_reply().
handle_txt(_Domain) ->
    not_impl.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Split the domain name into prefix and suffix, where suffix matches the
%% canonical globalregistry hostname (retrieved from env). The split is made on the dot between prefix and suffix.
%% If that's not possible, returns unknown_domain.
%% @end
%%--------------------------------------------------------------------
-spec parse_domain(Domain :: string()) -> {Prefix :: string(), Suffix :: string()} | unknown_domain.
parse_domain(DomainArg) ->
    % If requested domain starts with 'www.', ignore it
    Domain = case DomainArg of
                 "www." ++ Rest -> Rest;
                 Other -> Other
             end,
    {ok, ProviderHostnameWithoutDot} = application:get_env(?APP_NAME, global_registry_hostname),
    case ProviderHostnameWithoutDot =:= Domain of
        true ->
            {"", ProviderHostnameWithoutDot};
        false ->
            ProviderHostname = "." ++ ProviderHostnameWithoutDot,
            HostNamePos = string:rstr(Domain, ProviderHostname),
            % If hostname is at this position, it's a suffix (the string ends with it)
            ValidHostNamePos = length(Domain) - length(ProviderHostname) + 1,
            case HostNamePos =:= ValidHostNamePos of
                false ->
                    unknown_domain;
                true ->
                    {string:sub_string(Domain, 1, HostNamePos - 1), ProviderHostnameWithoutDot}
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% healthcheck dns endpoint
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, Reason :: atom()}.
healthcheck() ->
    LastUpdate = worker_host:state_get(?MODULE, last_update),
    LBAdvice = worker_host:state_get(?MODULE, lb_advice),
    case LBAdvice of
        undefined ->
            {error, no_lb_advice_received};
        _ ->
            {ok, Threshold} = application:get_env(?APP_NAME, dns_disp_out_of_sync_threshold),
            % Threshold is in millisecs, now_diff is in microsecs
            case timer:now_diff(now(), LastUpdate) > Threshold * 1000 of
                true ->
                    % DNS is out of sync
                    out_of_sync;
                false ->
                    % DNS is synced, check if it responds to requests.
                    check_dns_connectivity()
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks if DNS server responds to requests.
%% @end
%%--------------------------------------------------------------------
-spec check_dns_connectivity() -> ok | {error, server_not_responding}.
check_dns_connectivity() ->
    {ok, HealthcheckTimeout} = application:get_env(?APP_NAME, nagios_healthcheck_timeout),
    {ok, DNSPort} = application:get_env(?APP_NAME, dns_port),
    Query = inet_dns:encode(
        #dns_rec{
            header = #dns_header{
                id = crypto:rand_uniform(1, 16#FFFF),
                opcode = 'query',
                rd = true
            },
            qdlist = [#dns_query{
                domain = "localhost",
                type = soa,
                class = in
            }],
            arlist = [{dns_rr_opt, ".", opt, 1280, 0, 0, 0, <<>>}]
        }),
    {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
    gen_udp:send(Socket, "127.0.0.1", DNSPort, Query),
    case gen_udp:recv(Socket, 65535, HealthcheckTimeout) of
        {ok, _} ->
            % DNS is working
            ok;
        _ ->
            % DNS is not working
            {error, server_not_responding}
    end.