%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules provides utility functions for token management in op.
%%% @end
%%%-------------------------------------------------------------------
-module(token_utils).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    assert_interface_allowed/2,
    verify_api_caveats/3
]).

-type interface() :: gui | rest | oneclient.


%%%===================================================================
%%% API
%%%===================================================================


-spec assert_interface_allowed([caveats:caveat()] | tokens:serialized(),
    interface()) -> ok | no_return().
assert_interface_allowed(Caveats, _Interface) when is_list(Caveats) ->
    % TODO VFS-5719 check interface caveats
    ok;
assert_interface_allowed(SerializedToken, Interface) ->
    case tokens:deserialize(SerializedToken) of
        {ok, Token} ->
            Caveats = tokens:get_caveats(Token),
            assert_interface_allowed(Caveats, Interface);
        {error, _} = Error ->
            throw(Error)
    end.


-spec verify_api_caveats([caveats:caveat()], op_logic:operation(), gri:gri()) ->
    ok | no_return().
verify_api_caveats(Caveats, Operation, GRI) ->
    lists:foreach(fun(ApiCaveat) ->
        case cv_api:verify(ApiCaveat, ?OP_WORKER, Operation, GRI) of
            true -> ok;
            false -> throw(?ERROR_TOKEN_CAVEAT_UNVERIFIED(ApiCaveat))
        end
    end, Caveats).
