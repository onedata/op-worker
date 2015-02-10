%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This file contains definitions of annotations used during
%%% performnce tests.
%%% @end
%%%--------------------------------------------------------------------
-module(perf_test).
-author("Michal Wrzeszcz").

-annotation('function').
-include_lib("annotations/include/types.hrl").

-compile(export_all).

around_advice(#annotation{data=do_test}, M, F, Inputs) ->
  ct:print("Before: ~p", [{M, F}]),
  annotation:call_advised(M, F, Inputs),
  ct:print("After: ~p", [{M, F}]).