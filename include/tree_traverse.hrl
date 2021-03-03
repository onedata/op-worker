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

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").


-define(NEW_JOBS_DEFAULT_PREPROCESSOR, fun(_, _, _, _) -> ok end).
-define(DEFAULT_BATCH_SIZE, 1000).
-define(DEFAULT_CHILDREN_DIRS_HANDLING_MODE, generate_master_jobs).
-define(DEFAULT_CHILDREN_MASTER_JOBS_MODE, sync).
-define(DEFAULT_TRACK_SUBTREE_STATUS, false).

% Record that defines master job
-record(tree_traverse, {
    % File or directory processed by job
    file_ctx :: file_ctx:ctx(),
    % User who scheduled the traverse
    user_desc :: tree_traverse:user_desc(),

    % Fields used for directory listing
    token = ?INITIAL_LS_TOKEN :: file_meta:list_token(),
    last_name = <<>> :: file_meta:list_last_name(),
    last_tree = <<>> :: file_meta:list_last_tree(),
    batch_size :: tree_traverse:batch_size(),

    % Traverse config
    % generate slave jobs also for directories
    children_dirs_handling_mode :: tree_traverse:children_dirs_handling_mode(),
    % flag determining whether children master jobs are scheduled before slave jobs are processed
    children_master_jobs_mode = ?DEFAULT_CHILDREN_MASTER_JOBS_MODE :: tree_traverse:children_master_jobs_mode(),
    track_subtree_status = ?DEFAULT_TRACK_SUBTREE_STATUS :: boolean(),

    % info passed to every slave job
    traverse_info :: tree_traverse:traverse_info()
}).

% Record that defines slave job
-record(tree_traverse_slave, {
    file_ctx :: file_ctx:ctx(),
    % User who scheduled the traverse
    user_desc :: tree_traverse:user_desc(),
    traverse_info :: tree_traverse:traverse_info(),
    track_subtree_status = ?DEFAULT_TRACK_SUBTREE_STATUS :: boolean()
}).


-define(SUBTREE_PROCESSED, subtree_processed).
-define(SUBTREE_NOT_PROCESSED, subtree_not_processed).

-endif.