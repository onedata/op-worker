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
-module(fslogic_storage).
-author("Rafal Slota").

-include("modules/datastore/datastore.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([select_helper/1, select_storage/1]).

%%%===================================================================
%%% API
%%%===================================================================

select_helper(Storage) ->
    #storage{helpers = [Helper | _]} = Storage,
    {ok, Helper}.

select_storage(#fslogic_ctx{}) ->
    [#document{} = Storage | _] = storage:list(),
    {ok, Storage}.

%%%===================================================================
%%% Internal functions
%%%===================================================================