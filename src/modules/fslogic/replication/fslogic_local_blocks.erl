%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module manages blocks that are not synchronized between providers.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_local_blocks).
-author("Michał Wrzeszcz").

%% API
-export([get/3, flush/3]).

%%%===================================================================
%%% API
%%%===================================================================

get(Key, Offset, Size) ->
    [].

flush(Blocks, Offset, Size) ->
    ok.