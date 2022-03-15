%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Callback defining plug-in to dir_stats_collector. It defines how
%%% statistics are persisted and consolidated/updated.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_stats_collection_behaviour).
-author("Michal Wrzeszcz").


%%%===================================================================
%%% Callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Gets collection from datastore.
%% @end
%%--------------------------------------------------------------------
-callback acquire(file_id:file_guid()) ->
    {dir_stats_collection:collection(), InitializationTraverseNum :: non_neg_integer()} | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Provides new value of statistic using old value and description of update (3rd argument).
%% 3rd argument can be diff or new value - its interpretation is collection internal
%% matter - collector does not process it.
%% NOTE: it does not save new value to datastore.
%% @end
%%--------------------------------------------------------------------
-callback consolidate(dir_stats_collection:stat_name(), OldValue :: dir_stats_collection:stat_value(),
    Update :: dir_stats_collection:stat_value()) -> dir_stats_collection:stat_value().


%%--------------------------------------------------------------------
%% @doc
%% Saves collection to datastore. If collection if saved during collections initialization traverse, traverse number
%% is the third argument, otherwise third argument is undefined.
%% NOTE: latest initialization traverse number is persisted to allow collector determine if stored collection is
%% outdated as a result of temporary disabling of statistics collecting.
%% @end
%%--------------------------------------------------------------------
-callback save(file_id:file_guid(), dir_stats_collection:collection(),
    InitializationTraverseNum :: non_neg_integer() | undefined) -> ok | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Deletes collection from datastore.
%% @end
%%--------------------------------------------------------------------
-callback delete(file_id:file_guid()) -> ok | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Init collection for directory not including statistics of children (for each child init_child will be called).
%% @end
%%--------------------------------------------------------------------
-callback init_dir(file_id:file_guid()) -> dir_stats_collection:collection() | no_return().


%%--------------------------------------------------------------------
%% @doc
%% When collection is being initialized, init_child/1 callback is called for each child to get
%% statistics connected with this child.
%% NOTE: if child is directory, returned statistics should not include statistics of this directory children.
%% @end
%%--------------------------------------------------------------------
-callback init_child(file_id:file_guid()) -> dir_stats_collection:collection() | no_return().