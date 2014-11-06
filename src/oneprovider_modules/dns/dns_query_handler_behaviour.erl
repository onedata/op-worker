%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This behaviour defines API for a DNS query handler.
%% @end
%% ===================================================================
-module(dns_query_handler_behaviour).

-include("dns.hrl").


%% handle_xxx/1
%% ====================================================================
%% @doc Callbacks below handle specific types od DNS queries, in accordance to RFC1035:
%% {@link https://tools.ietf.org/html/rfc1035#section-3.2.2}
%% The argument in every function is Domain that was queried for, as a lower-case string.
%% On success, the callback must return {ok, List}, where List consists of terms created with functions:
%% dns_server:answer_record/2, dns_server:authority_record/2, dns_server:additional_record/2, dns_server:authoritative_answer_flag/1.
%% Those terms will be put in proper sections of DNS response.
%% See {@link dns.hrl} for reference and data types that should be returned for specific types of queries.
%% @end
%% ====================================================================
-callback handle_a(     Domain :: string()) -> {ok, dns_query_handler_reponse()} | serv_fail | nx_domain | not_impl | refused.

-callback handle_ns(    Domain :: string()) -> {ok, dns_query_handler_reponse()} | serv_fail | nx_domain | not_impl | refused.

-callback handle_cname( Domain :: string()) -> {ok, dns_query_handler_reponse()} | serv_fail | nx_domain | not_impl | refused.

-callback handle_soa(   Domain :: string()) -> {ok, dns_query_handler_reponse()} | serv_fail | nx_domain | not_impl | refused.

-callback handle_wks(   Domain :: string()) -> {ok, dns_query_handler_reponse()} | serv_fail | nx_domain | not_impl | refused.

-callback handle_ptr(   Domain :: string()) -> {ok, dns_query_handler_reponse()} | serv_fail | nx_domain | not_impl | refused.

-callback handle_hinfo( Domain :: string()) -> {ok, dns_query_handler_reponse()} | serv_fail | nx_domain | not_impl | refused.

-callback handle_minfo( Domain :: string()) -> {ok, dns_query_handler_reponse()} | serv_fail | nx_domain | not_impl | refused.

-callback handle_mx(    Domain :: string()) -> {ok, dns_query_handler_reponse()} | serv_fail | nx_domain | not_impl | refused.

-callback handle_txt(   Domain :: string()) -> {ok, dns_query_handler_reponse()} | serv_fail | nx_domain | not_impl | refused.



