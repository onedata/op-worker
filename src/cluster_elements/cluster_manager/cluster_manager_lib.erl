%%%-------------------------------------------------------------------
%%% @author RoXeon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Jul 2014 15:36
%%%-------------------------------------------------------------------
-module(cluster_manager_lib).
-author("RoXeon").

-include_lib("public_key/include/public_key.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_provider_id/0]).

-spec get_provider_id() -> ProviderId :: binary().
get_provider_id() ->
    {ok, Bin} = file:read_file(global_registry:get_provider_cert_path()),
    [{_, PeerCertDer, _} | _] = public_key:pem_decode(Bin),
    PeerCert = public_key:pkix_decode_cert(PeerCertDer, otp),

    auth_handler:get_provider_id(PeerCert).

