%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains record definitions used by tree traverse (see tree_traverse.erl).
%%% @end
%%%-------------------------------------------------------------------
-ifndef(TREE_TRAVERSE_HRL).
-define(TREE_TRAVERSE_HRL, 1).

% Record that defines master job
-record(tree_traverse, {
    % File or directory processed by job
    doc :: file_meta:doc(),
    % Fields used for directory listing
    token :: datastore_links_iter:token() | undefined,
    last_name = <<>> :: file_meta:name(),
    last_tree = <<>> :: od_provider:id(),
    batch_size :: tree_traverse:batch_size(),
    % Traverse config
    execute_slave_on_dir :: tree_traverse:execute_slave_on_dir(), % generate slave jobs also for directories
    traverse_info :: tree_traverse:traverse_info() % info passed to every slave job
}).

-record(tree_traverse_slave, {
    doc :: file_meta:doc(),
    traverse_info :: tree_traverse:traverse_info()
}).

-endif.