%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Plugin for DNS worker. Accepts urls according to op-worker requirements.
%%% @end
%%%-------------------------------------------------------------------
-module(dns_worker_plugin).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("kernel/src/inet_dns.hrl").

-export([resolve/3]).

-ifdef(TEST).
-export([parse_domain/1]).
-endif.

%%--------------------------------------------------------------------
%% @doc
%% {@link dns_worker_plugin_behaviour} callback resolve/3.
%% @end
%%--------------------------------------------------------------------
-spec resolve(Method :: atom(), Domain :: string(), LbAdvice :: term()) ->
    dns_handler_behaviour:handler_reply().

resolve(Method, Domain, LBAdvice) ->
    case parse_domain(Domain) of
        ok -> do_resolve(Method, Domain, LBAdvice);
        Other -> Other
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolves queries according to load balancing advice.
%% @end
%%--------------------------------------------------------------------
-spec do_resolve(Method :: atom(), Domain :: string(), LbAdvice :: term()) ->
    dns_handler_behaviour:handler_reply().

do_resolve(handle_a, Domain, LBAdvice) ->
    Nodes = load_balancing:choose_nodes_for_dns(LBAdvice),
    {ok, TTL} = application:get_env(?CLUSTER_WORKER_APP_NAME, dns_a_response_ttl),
    {ok,
            [dns_server:answer_record(Domain, TTL, ?S_A, IP) || IP <- Nodes] ++
            [dns_server:authoritative_answer_flag(true)]
    };

do_resolve(handle_ns, Domain, LBAdvice) ->
    Nodes = load_balancing:choose_ns_nodes_for_dns(LBAdvice),
    {ok, TTL} = application:get_env(?CLUSTER_WORKER_APP_NAME, dns_ns_response_ttl),
    {ok,
            [dns_server:answer_record(Domain, TTL, ?S_NS, inet_parse:ntoa(IP)) || IP <- Nodes] ++
            [dns_server:authoritative_answer_flag(true)]
    };

do_resolve(_, _, _) ->
    serv_fail.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Verifies if provider should handle given domain.
%% @end
%%--------------------------------------------------------------------
-spec parse_domain(Domain :: string()) -> ok | refused | nx_domain.

parse_domain(DomainArg) ->
    ProviderDomain = oneprovider:get_provider_domain(),
    GRDomain = oneprovider:get_gr_domain(),
    % If requested domain starts with 'www.', ignore the suffix
    QueryDomain = case DomainArg of
        "www." ++ Rest -> Rest;
        Other -> Other
    end,

    % Check if queried domain ends with provider domain
    case string:rstr(QueryDomain, ProviderDomain) of
        0 ->
            % If not, check if following are true:
            % 1. GR domain: gr.domain
            % 2. provider domain: prov_subdomain.gr.domain
            % 3. queried domain: first_part.gr.domain
            % If not, return REFUSED
            QDTail = string:join(tl(string:tokens(QueryDomain, ".")), "."),
            PDTail = string:join(tl(string:tokens(ProviderDomain, ".")), "."),
            case QDTail =:= GRDomain andalso PDTail =:= GRDomain of
                true ->
                    ok;
                false ->
                    refused
            end;
        _ ->
            % Queried domain does end with provider domain
            case QueryDomain of
                ProviderDomain ->
                    ok;
                _ ->
                    % Check if queried domain is in form
                    % 'first_part.provider.domain' - strip out the
                    % first_part and compare. If not, return NXDOMAIN
                    case string:join(tl(string:tokens(QueryDomain, ".")), ".") of
                        ProviderDomain ->
                            ok;
                        _ ->
                            nx_domain
                    end
            end
    end.
