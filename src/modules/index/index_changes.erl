%%%-------------------------------------------------------------------
%%% @author cyfronet
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling changes on index documents. The
%%% callback is called for all changes - remote (dbsync) and local (posthook).
%%% @end
%%%-------------------------------------------------------------------
-module(index_changes).
-author("cyfronet").

%% API
-export([]).
