%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Wrapper library to call CSR creation NIF.
%%% @end
%%%-------------------------------------------------------------------
-module(csr_creator).
-author("Lukasz Opiola").

%% API
-export([create_csr/3]).

-on_load(init/0).

%%%===================================================================
%%% API
%%%===================================================================

%% init/0
%% ====================================================================
%% @doc Initializes NIF library.
%% @end
-spec init() -> ok | no_return().
%% ====================================================================
init() ->
    ok = erlang:load_nif("c_lib/csr_creator_drv", 0).


%% create_csr/3
%% ====================================================================
%% @doc Call underlying NIF function. Creates private key and certificate
%% signing request and saves them at given paths.
%% Returns 0 in case of success and 1 in case of failure.
%% Can throw an exception if nif was not properly loaded.
%% @end
-spec create_csr(Password :: string(), KeyFile :: string(), CsrPath :: string()) -> 0 | 1 | no_return().
%% ====================================================================
create_csr(_Password, _KeyFile, _CsrPath) ->
    erlang:nif_error({error, not_loaded}).

