%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Callback defining plug-in to tree gatherer.
%%% @end
%%%-------------------------------------------------------------------
-module(tree_gatherer_behaviour).
-author("Michal Wrzeszcz").

%%%===================================================================
%%% Callbacks
%%%===================================================================

-callback init_cache(file_id:file_guid()) -> {ok, tree_gatherer_pes_callback:values_map()} | {error, term()}.

-callback merge(tree_gatherer_pes_callback:parameter(), tree_gatherer_pes_callback:parameter_value(),
    tree_gatherer_pes_callback:parameter_value()) -> tree_gatherer_pes_callback:parameter_value().

-callback save(file_id:file_guid(), tree_gatherer_pes_callback:values_map()) -> ok | {error, term()}.