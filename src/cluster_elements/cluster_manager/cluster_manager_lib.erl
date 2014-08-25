%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: @todo: write me !
%% @end
%% ===================================================================
-module(cluster_manager_lib).
-author("Rafal Slota").

-include_lib("public_key/include/public_key.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_provider_id/0]).

-spec get_provider_id() -> ProviderId :: binary().
get_provider_id() ->
    {ok, Bin} = file:read_file(gr_plugin:get_cert_path()),
    [{_, PeerCertDer, _} | _] = public_key:pem_decode(Bin),
    PeerCert = public_key:pkix_decode_cert(PeerCertDer, otp),

    auth_handler:get_provider_id(PeerCert).

