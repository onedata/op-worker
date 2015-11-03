%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions for http_worker modules.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(HTTP_COMMON_HRL).
-define(HTTP_COMMON_HRL, 1).

-include("global_definitions.hrl").
-include_lib("n2o/include/wf.hrl").

%% Includes from cowboy
-type req() :: cowboy_req:req().

-endif.