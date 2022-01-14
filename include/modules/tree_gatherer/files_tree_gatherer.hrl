%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Protocol used by files tree gatherer.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FILES_TREE_GATHERER_HRL).
-define(FILES_TREE_GATHERER_HRL, 1).


%%%===================================================================
%%% Requests to tree gatherer.
%%%===================================================================


-record(ftg_update_request, {
    guid :: file_id:file_guid(),
    handler_module :: files_tree_gatherer_pes_executor:handler_module(),
    diff_map :: files_tree_gatherer_pes_executor:diff_map()
}).


-record(ftg_get_request, {
    guid :: file_id:file_guid(),
    handler_module :: files_tree_gatherer_pes_executor:handler_module(),
    parameters :: [files_tree_gatherer_pes_executor:parameter()]
}).


-endif.