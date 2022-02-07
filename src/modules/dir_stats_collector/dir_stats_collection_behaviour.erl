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
-callback acquire(file_id:file_guid()) -> dir_stats_collection:collection() | no_return().


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
%% Saves collection to datastore.
%% @end
%%--------------------------------------------------------------------
-callback save(file_id:file_guid(), dir_stats_collection:collection()) -> ok | no_return().


%%--------------------------------------------------------------------
%% @doc
%% Deletes collection from datastore.
%% @end
%%--------------------------------------------------------------------
-callback delete(file_id:file_guid()) -> ok | no_return().