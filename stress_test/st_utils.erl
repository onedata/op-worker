%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module contains stress tests utility functions.
%% @end
%% ===================================================================
-module(st_utils).

%% API
-export([host_to_node/1, make_dir/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% host_to_node/1
%% ====================================================================
%% @doc Maps machine hostname to worker name
-spec host_to_node(Hostname :: string()) -> Result when
    Result :: atom() | no_return().
%% ====================================================================
host_to_node("149.156.10.162") -> 'worker@veil-d01.grid.cyf-kr.edu.pl';
host_to_node("149.156.10.163") -> 'worker@veil-d02.grid.cyf-kr.edu.pl';
host_to_node("149.156.10.164") -> 'worker@veil-d03.grid.cyf-kr.edu.pl';
host_to_node("149.156.10.165") -> 'worker@veil-d04.grid.cyf-kr.edu.pl';
host_to_node(Other) -> throw(io_lib:fwrite("Unknown hostname: ~p", [Other])).


%% make_dir/1
%% ====================================================================
%% @doc Creates directory with parent directories
-spec make_dir(Dir :: string()) -> Result when
    Result :: ok | {error, Reason :: term()}.
%% ====================================================================
make_dir(Dir) ->
    [Root | Leafs] = filename:split(Dir),
    case make_dir(Root, Leafs) of
        ok -> ok;
        {error, eexist} -> ok;
        Other -> Other
    end.


%% make_dir/1
%% ====================================================================
%% @doc Creates directory with parent directories.
%% Should not be used directly, use make_dir/1 instead.
-spec make_dir(Root :: string(), Leafs :: [string()]) -> Result when
    Result :: ok | {error, Reason :: term()}.
%% ====================================================================
make_dir(Root, []) ->
    file:make_dir(Root);
make_dir(Root, [Leaf | Leafs]) ->
    file:make_dir(Root),
    make_dir(filename:join(Root, Leaf), Leafs).


%% get_dn/1
%% ====================================================================
%% @doc Gets rDN list compatibile user_logic:create_user from PEM file
-spec get_dn(PEMFile :: string()) -> Result when
    Result :: {ok, DN :: string()}.
%% ====================================================================
get_dn(PEMFile) ->
    try
        {ok, PemBin} = file:read_file(PEMFile),
        Cert = public_key:pem_decode(PemBin),
        [Leaf | Chain] = [public_key:pkix_decode_cert(DerCert, otp) || {'Certificate', DerCert, _} <- Cert],
        {ok, EEC} = gsi_handler:find_eec_cert(Leaf, Chain, gsi_handler:is_proxy_certificate(Leaf)),
        {rdnSequence, Rdn} = gsi_handler:proxy_subject(EEC),
        user_logic:rdn_sequence_to_dn_string(Rdn)
    catch
        _:Error -> throw(io_lib:format("Can not get dn: ~p", [Error]))
    end.