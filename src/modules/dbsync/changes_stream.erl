%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-module(changes_stream).
-author("Rafal Slota").

-behavior(gen_changes).

-include("modules/dbsync/common.hrl").
-include_lib("ctool/include/logging.hrl").
