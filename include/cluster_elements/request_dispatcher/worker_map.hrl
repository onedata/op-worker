%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% worker_map external parameters, used to customize worker map queries
%%% @end
%%%-------------------------------------------------------------------

-ifndef(WORKER_MAP_HRL).
-define(WORKER_MAP_HRL, 1).

-define(DEFAULT_WORKER_SELECTION_TYPE, random).
-type selection_type() :: random | prefer_local.
-type worker_ref() :: atom() | {atom(), node()}.

-endif.

