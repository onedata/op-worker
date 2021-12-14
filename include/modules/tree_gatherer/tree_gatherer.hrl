%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Protocol used by tree gatherer.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(TREE_GATHERER_HRL).
-define(TREE_GATHERER_HRL, 1).


%%%===================================================================
%%% Requests to tree gatherer.
%%%===================================================================


-record(tree_gatherer_update_request, {
    guid :: file_id:file_guid(),
    handler_module :: tree_gatherer_pes_callback:handler_module(),
    diff_map :: tree_gatherer_pes_callback:diff_map()
}).


-record(tree_gatherer_get_request, {
    guid :: file_id:file_guid(),
    handler_module :: tree_gatherer_pes_callback:handler_module(),
    parameters :: [tree_gatherer_pes_callback:parameter()]
}).


-endif.