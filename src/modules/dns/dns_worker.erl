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
%%% In addition, it implements {@link dns_query_handler_behaviour} -
%%% DNS query handling logic.
%%% @end
%%%-------------------------------------------------------------------
-module(dns_worker).
-author("Lukasz Opiola").

-behaviour(worker_plugin_behaviour).
-behaviour(dns_query_handler_behaviour).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("kernel/src/inet_dns.hrl").

%% This record is used by dns_worker (it contains its state).
-record(state, {
    %% This record is usually used in read-only mode to get a list of
    %% nodes to return as DNS response. It is updated periodically by node manager.
    lb_advice = undefined :: load_balancing:dns_lb_advice(),
    % Time of last ld advice update received from dispatcher
    last_update = {0, 0, 0} :: {integer(), integer(), integer()}
}).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/2, cleanup/0]).

%% dns_query_handler_behaviour callbacks
-export([handle_a/1, handle_ns/1, handle_cname/1, handle_soa/1, handle_wks/1,
    handle_ptr/1, handle_hinfo/1, handle_minfo/1, handle_mx/1, handle_txt/1]).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, #state{}} | {error, Reason :: term()}.
init([]) ->
    {ok, #state{}};

init(InitialState) when is_record(InitialState, state) ->
    {ok, InitialState};

init(test) ->
    {ok, #state{}};

init(_) ->
    throw(unknown_initial_state).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request, State :: term()) -> Result when
    Request :: ping | healthcheck,
    Result :: nagios_handler:healthcheck_reponse() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: [inet:ip4_address()],
    Reason :: term().

handle(ping, _) ->
    pong;

handle(healthcheck, State) ->
    _Reply = healthcheck(State);

handle({update_lb_advice, LBAdvice}, State) ->
    ?debug("DNS update of load_balancing advice: ~p", [LBAdvice]),
    NewState = State#state{last_update = now(), lb_advice = LBAdvice},
    gen_server:call(?MODULE, {update_plugin_state, NewState});

handle({handle_a, Domain}, #state{lb_advice = LBAdvice}) ->
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

handle({handle_ns, Domain}, #state{lb_advice = LBAdvice}) ->
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

handle(_Request, _) ->
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
%%% dns_query_handler_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type A.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_a(Domain :: string()) ->
    {dns_query_handler_behaviour:reply_type(), dns_query_handler_behaviour:dns_query_handler_reponse()} |
    dns_query_handler_behaviour:reply_type().
handle_a(Domain) ->
    worker_proxy:call(dns_worker, {handle_a, Domain}).


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type NS.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_ns(Domain :: string()) ->
    {dns_query_handler_behaviour:reply_type(), dns_query_handler_behaviour:dns_query_handler_reponse()} |
    dns_query_handler_behaviour:reply_type().
handle_ns(Domain) ->
    worker_proxy:call(dns_worker, {handle_ns, Domain}).


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type CNAME.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_cname(Domain :: string()) ->
    {dns_query_handler_behaviour:reply_type(), dns_query_handler_behaviour:dns_query_handler_reponse()} |
    dns_query_handler_behaviour:reply_type().
handle_cname(_Domain) ->
    not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type MX.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_mx(Domain :: string()) ->
    {dns_query_handler_behaviour:reply_type(), dns_query_handler_behaviour:dns_query_handler_reponse()} |
    dns_query_handler_behaviour:reply_type().
handle_mx(_Domain) ->
    not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type SOA.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_soa(Domain :: string()) ->
    {dns_query_handler_behaviour:reply_type(), dns_query_handler_behaviour:dns_query_handler_reponse()} |
    dns_query_handler_behaviour:reply_type().
handle_soa(_Domain) ->
    not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type WKS.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_wks(Domain :: string()) ->
    {dns_query_handler_behaviour:reply_type(), dns_query_handler_behaviour:dns_query_handler_reponse()} |
    dns_query_handler_behaviour:reply_type().
handle_wks(_Domain) ->
    not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type PTR.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_ptr(Domain :: string()) ->
    {dns_query_handler_behaviour:reply_type(), dns_query_handler_behaviour:dns_query_handler_reponse()} |
    dns_query_handler_behaviour:reply_type().
handle_ptr(_Domain) ->
    not_impl.


%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type HINFO.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_hinfo(Domain :: string()) ->
    {dns_query_handler_behaviour:reply_type(), dns_query_handler_behaviour:dns_query_handler_reponse()} |
    dns_query_handler_behaviour:reply_type().
handle_hinfo(_Domain) ->
    not_impl.

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type MINFO.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_minfo(Domain :: string()) ->
    {dns_query_handler_behaviour:reply_type(), dns_query_handler_behaviour:dns_query_handler_reponse()} |
    dns_query_handler_behaviour:reply_type().
%% ====================================================================
handle_minfo(_Domain) ->
    not_impl.

%%--------------------------------------------------------------------
%% @doc
%% Handles DNS queries of type TXT.
%% See {@link dns_query_handler_behaviour} for reference.
%% @end
%%--------------------------------------------------------------------
-spec handle_txt(Domain :: string()) ->
    {dns_query_handler_behaviour:reply_type(), dns_query_handler_behaviour:dns_query_handler_reponse()} |
    dns_query_handler_behaviour:reply_type().
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
-spec healthcheck(State :: #state{}) -> ok | {error, Reason :: atom()}.
healthcheck(#state{last_update = LastUpdate}) ->
    {ok, Threshold} = application:get_env(?APP_NAME, dns_disp_out_of_sync_threshold),
    {ok, HealthcheckTimeout} = application:get_env(?APP_NAME, nagios_healthcheck_timeout),
    % Threshold is in millisecs, now_diff is in microsecs
    case timer:now_diff(now(), LastUpdate) > Threshold * 1000 of
        true ->
            % DNS is out of sync
            out_of_sync;
        false ->
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
            end
    end.