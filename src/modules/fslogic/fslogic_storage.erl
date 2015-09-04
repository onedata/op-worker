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
-export([select_helper/1, select_storage/1, new_storage/2, new_helper_init/2]).

%%%===================================================================
%%% API
%%%===================================================================

select_helper(#document{value = Storage}) ->
    select_helper(Storage);
select_helper(Storage) ->
    #storage{helpers = [Helper | _]} = Storage,
    {ok, Helper}.

select_storage(#fslogic_ctx{}) ->
    {ok, [#document{} = Storage | _]} = storage:list(),
    {ok, Storage}.


new_helper_init(HelperName, HelperArgs) ->
    #helper_init{name = HelperName, args = HelperArgs}.


new_storage(Name, Helpers) ->
    #storage{name = Name, helpers = Helpers}.

%%%===================================================================
%%% Internal functions
%%%===================================================================