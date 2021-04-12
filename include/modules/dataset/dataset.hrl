%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros used in modules that implement datasets functionality.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASET_HRL).
-define(DATASET_HRL, 1).

% Macros defining possible dataset state values
-define(ATTACHED_DATASET, attached).
-define(DETACHED_DATASET, detached).

% Macros defining types of dataset membership
-define(NONE_DATASET_MEMBERSHIP, none).
-define(DIRECT_DATASET_MEMBERSHIP, direct).
-define(ANCESTOR_DATASET_MEMBERSHIP, ancestor).

% Macros defining types of datasets structures
-define(ATTACHED_DATASETS_STRUCTURE, <<"ATTACHED">>).
-define(DETACHED_DATASETS_STRUCTURE, <<"DETACHED">>).

-endif.