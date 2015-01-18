%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module loads GSI NIF libs
%%% @end
%%%-------------------------------------------------------------------
-module(gsi_nif).
-author("Rafal Slota").

%% API
-export([verify_cert_c/4, start/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This method loads NIF library into erlang VM. This should be used <br/>
%% once before using any other method in this module.
%% @end
%%--------------------------------------------------------------------
-spec start(Prefix :: string()) -> ok | {error, Reason :: term()}.
start(Prefix) ->
    erlang:load_nif(filename:join(Prefix, "c_lib/gpv_drv"), 0).

%%--------------------------------------------------------------------
%% @doc
%% This method validates peer certificate. All input certificates should be DER encoded. <br/>
%% {ok, 1} result means that peer is valid, {ok, 0, Errno} means its not. Errno is a raw errno returned by openssl/globus library <br/>
%% {error, Reason} on the other hand is returned only when something went horribly wrong during validation.
%% @end
%%--------------------------------------------------------------------
-spec verify_cert_c(PeerCert :: binary(), PeerChain :: [binary()], CAChain :: [binary()], CRLs :: [binary()]) ->
    {ok, 1} | {ok, 0, Errno :: integer()} | {error, Reason :: term()}.
verify_cert_c(_user, _chain, _ca, _crl) ->
    {error, 'NIF_not_loaded'}.