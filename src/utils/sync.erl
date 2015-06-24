%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Developer modules that allows for easy recompilation and reload of
%%% erlang modules, erlyDTL templates and static GUI files.
%%% @end
%%%-------------------------------------------------------------------
-module(sync).
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").

%% API
-export([update_erl_files/0]).



%%%===================================================================
%%% API
%%%===================================================================


