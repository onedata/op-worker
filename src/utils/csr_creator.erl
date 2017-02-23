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

-include("global_definitions.hrl").

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
-spec init() -> ok | {error, Reason :: term()}.
init() ->
    LibName = "csr_creator_drv",
    LibPath =
        case code:priv_dir(?APP_NAME) of
            {error, bad_name} ->
                case filelib:is_dir(filename:join(["..", "priv"])) of
                    true ->
                        filename:join(["..", "priv", LibName]);
                    _ ->
                        filename:join(["priv", LibName])
                end;

            Dir ->
                filename:join(Dir, LibName)
        end,

    case erlang:load_nif(LibPath, 0) of
        ok -> ok;
        {error, {reload, _}} -> ok;
        {error, {upgrade, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.


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

