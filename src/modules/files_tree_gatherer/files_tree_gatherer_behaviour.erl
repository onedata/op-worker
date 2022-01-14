%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Callback defining plug-in to files tree gatherer 
%%% (see files_tree_gatherer_pes_executor.erl).
%%% @end
%%%-------------------------------------------------------------------
-module(files_tree_gatherer_behaviour).
-author("Michal Wrzeszcz").

%%%===================================================================
%%% Callbacks
%%%===================================================================

-callback init_cache(file_id:file_guid()) -> {ok, files_tree_gatherer_pes_executor:values_map()} | {error, term()}.

-callback merge(files_tree_gatherer_pes_executor:parameter(), files_tree_gatherer_pes_executor:parameter_value(),
    files_tree_gatherer_pes_executor:parameter_value()) -> files_tree_gatherer_pes_executor:parameter_value().

-callback save(file_id:file_guid(), files_tree_gatherer_pes_executor:values_map()) -> ok | {error, term()}.