%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Functions for decoding certificates into different formats.
%%% @end
%%%--------------------------------------------------------------------
-module(cert_decoder).
-author("Tomasz Lichon").

-include_lib("ctool/include/logging.hrl").

%% API
-export([pem_to_der/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Converts certificate from pem to der format.
%% @end
%%--------------------------------------------------------------------
-spec pem_to_der(file_meta:path()) -> public_key:der_encoded().
pem_to_der(CertPem) ->
    PemEntries = public_key:pem_decode(CertPem),
    {'Certificate',CertDer, _} = lists:keyfind('Certificate', 1, PemEntries),
    CertDer.

%%%===================================================================
%%% Internal functions
%%%===================================================================