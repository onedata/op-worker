%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(atm_test_docker_registry).
-author("Bartosz Walkowicz").

%% API
-export([exec/2]).


%%%===================================================================
%%% API
%%%===================================================================


exec(<<"test/echo">>, #{<<"argsBatch">> := ArgsBatch}) ->
    #{<<"resultsBatch">> => ArgsBatch}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

