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
-author("Tomasz Lichon").

-define(default_worker_selection_type, random).
-type(selection_type() :: random | prefere_local).
-type(worker_ref() :: atom() | {atom(), node()}).