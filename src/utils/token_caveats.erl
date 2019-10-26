%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules handles token caveats checks.
%%% @end
%%%-------------------------------------------------------------------
-module(token_caveats).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    is_interface_allowed/2,
    assert_none_data_caveats/1,
    verify_api_caveats/3
]).

-type interface() :: gui | rest | oneclient.
-type caveats_source() ::
    [caveats:caveat()] |
    tokens:serialized() |
    tokens:token().


%%%===================================================================
%%% API
%%%===================================================================


-spec is_interface_allowed(caveats_source(), interface()) -> boolean().
is_interface_allowed(Caveats, _Interface) when is_list(Caveats) ->
    % TODO VFS-5719 check interface caveats
    true;
is_interface_allowed(#token{} = Token, Interface) ->
    Caveats = tokens:get_caveats(Token),
    is_interface_allowed(Caveats, Interface);
is_interface_allowed(SerializedToken, Interface) ->
    case tokens:deserialize(SerializedToken) of
        {ok, Token} ->
            Caveats = tokens:get_caveats(Token),
            is_interface_allowed(Caveats, Interface);
        {error, _} ->
            false
    end.


-spec assert_none_data_caveats(caveats_source()) -> ok | no_return().
assert_none_data_caveats(Caveats) when is_list(Caveats) ->
    case caveats:filter([cv_data_path, cv_data_objectid], Caveats) of
        [] -> ok;
        _ -> throw(?ERROR_TOKEN_INVALID)
    end;
assert_none_data_caveats(#token{} = Token) ->
    Caveats = tokens:get_caveats(Token),
    assert_none_data_caveats(Caveats);
assert_none_data_caveats(SerializedToken) ->
    case tokens:deserialize(SerializedToken) of
        {ok, Token} ->
            Caveats = tokens:get_caveats(Token),
            assert_none_data_caveats(Caveats);
        {error, _} ->
            throw(?ERROR_TOKEN_INVALID)
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
