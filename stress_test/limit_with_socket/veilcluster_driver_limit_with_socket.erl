%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc:This test checks how many open/close operations per second may
%% be done. This test allows to check if socket opening may be a performance
%% problem (when we compare results to limit test results).
%% @end
%% ===================================================================

-module(veilcluster_driver_limit_with_socket).
-export([new/1, run/4]).

-include("basho_bench.hrl").

new(_Id) ->
    Hosts = basho_bench_config:get(cluster_hosts),
    CertFile = basho_bench_config:get(cert_file),
    {ok, {Hosts, CertFile}}.

run(_Action, KeyGen, _ValueGen, {Hosts, CertFile}) ->
  KG = KeyGen(),
  Host = lists:nth((KG rem length(Hosts)) + 1 , Hosts),
  NewState = {Hosts, CertFile},

  try
    case wss:connect(Host, 5555, [{certfile, CertFile}, {cacertfile, CertFile}, auto_handshake]) of
      {ok, Socket} ->
        wss:close(Socket),
        {ok, NewState};
      {error, Error} -> {error, {connect, Error}, NewState};
      Other -> {error, {unknown_error, Other}, NewState}
    end
  catch
    E1:E2 ->
      {error, {error_thrown, E1, E2}, NewState}
  end.