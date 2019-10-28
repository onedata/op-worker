%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for verification of data caveats.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_caveats).
-author("Bartosz Walkowicz").

-include("proto/common/credentials.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/aai/caveats.hrl").

%% API
-export([assert_none_data_caveats/1]).

-define(DATA_CAVEATS, [cv_data_path, cv_data_objectid]).


%%%===================================================================
%%% API
%%%===================================================================


-spec assert_none_data_caveats([caveats:caveat()]) -> ok | no_return().
assert_none_data_caveats(Caveats) when is_list(Caveats) ->
    case caveats:filter(?DATA_CAVEATS, Caveats) of
        [] -> ok;
        _ -> throw(?ERROR_TOKEN_INVALID)
    end;
assert_none_data_caveats(SerializedToken) ->
    case tokens:deserialize(SerializedToken) of
        {ok, Token} ->
            assert_none_data_caveats(tokens:get_caveats(Token));
        {error, _} ->
            throw(?ERROR_TOKEN_INVALID)
    end.
