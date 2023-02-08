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
%%% Callbacks - persistence
%%%
%%% Following callbacks has to be defined to allow dir_stats_collector
%%% getting, saving and deleting collections from datastore.
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Gets collection from datastore. Returns collection and incarnation of collection when collection was saved.
%% Incarnation allows evaluation if acquired collection is outdated (see dir_stats_collector:update_in_cache/4).
%% @end
%%--------------------------------------------------------------------
-callback acquire(file_id:file_guid()) ->
    {dir_stats_collection:collection(), Incarnation :: non_neg_integer()} | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Saves collection to datastore. If collection is saved during collections initialization traverse, incarnation is the
%% third argument, otherwise third argument is `current` (`current` = previously saved incarnation has not changed).
%% NOTE: incarnation must be persisted to allow collector determine if stored collection is
%% outdated as a result of temporary disabling of statistics collecting.
%% @end
%%--------------------------------------------------------------------
-callback save(file_id:file_guid(), dir_stats_collection:collection(),
    Incarnation :: non_neg_integer() | current) -> ok | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Deletes collection from datastore.
%% @end
%%--------------------------------------------------------------------
-callback delete(file_id:file_guid()) -> ok | no_return().


%%%===================================================================
%%% Callbacks - collection in memory update
%%%===================================================================

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


-callback on_collection_move(dir_stats_collection:stat_name(), dir_stats_collection:stat_value()) ->
    {update_source_parent, dir_stats_collection:stat_value()} | ignore.


%%%===================================================================
%%% Callbacks - collections initialization
%%%
%%% Callbacks used to create collections for existing directories (when collecting state is changed to enabled
%%% for not empty space). For each directory only directory parameters (init_dir callback) and its direct children
%%% (init_child callback) are used to initialize collections. Statistics calculated using not direct descendants will
%%% be added to initialized statistics as a result of statistics flushing by dir_stats_collectors working on behalf
%%% of not direct descendants.
%%%
%%% NOTE: init_dir and init_child can be called with same guid (only for directories). However, context of call will
%%% be different. init_dir is called with directory guid when initializing directory identified by the guid.
%%% init_child is called with directory guid when initializing parent of directory identified by the guid.
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Init collection for directory not including statistics of children (for each child init_child will be called).
%% @end
%%--------------------------------------------------------------------
-callback init_dir(file_id:file_guid()) -> dir_stats_collection:collection() | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Get statistics connected with child identified by guid.
%% Second argument determines if stats should be counted if file is deleted (they are needed to handle races
%% between mv and delete as it is possible that between mv and handling mv by collector someone deletes file).
%% NOTE: if child is directory, returned statistics should not include statistics of this directory children.
%% @end
%%--------------------------------------------------------------------
-callback init_child(file_id:file_guid(), boolean()) -> dir_stats_collection:collection() | no_return().


%%%===================================================================
%%% Callbacks - file_meta_posthooks related
%%%
%%% Following callbacks has to be defined to allow dir_stats_collector
%%% passing collection as an argument to file_meta posthook function.
%%%===================================================================

-callback encode(dir_stats_collection:collection()) -> term().

-callback decode(term()) -> dir_stats_collection:collection().