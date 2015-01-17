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
-ifndef(CDMI_HRL).
-define(CDMI_HRL, 1).

%% Includes from cowboy
-type req() :: cowboy_req:req().
-export_type([req/0]).

-endif.