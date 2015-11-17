%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% todo
%%% @end
%%%-------------------------------------------------------------------
-module(dns_worker_plugin).
-author("Michal Zmuda").

-export([parse_domain/1]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% {@link dns_worker_plugin_behaviour} callback parse_domain/0.
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
