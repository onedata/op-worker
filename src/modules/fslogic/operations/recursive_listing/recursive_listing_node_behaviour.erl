%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Behaviour of single node during recursive listing. For more details 
%%% consult `recursive_listing` module.
%%% @end
%%%--------------------------------------------------------------------
-module(recursive_listing_node_behaviour).
-author("Michal Stanisz").

-type tree_node() :: recursive_listing:tree_node().
-type node_id() :: recursive_listing:node_id().
-type node_name() :: recursive_listing:node_name().
-type node_iterator() :: recursive_listing:node_iterator().
-type limit() :: recursive_listing:limit().

%%%===================================================================
%%% Callbacks
%%%===================================================================

-callback is_branching_node(tree_node()) -> {boolean(), tree_node()}.

-callback get_node_id(tree_node()) -> node_id().

-callback get_node_name(tree_node(), user_ctx:ctx() | undefined) -> {node_name(), tree_node()}.

% NOTE: callback used only in listing initialization process.
-callback get_node_path_tokens(tree_node()) -> {[node_name()], tree_node()}.

-callback init_node_iterator(tree_node(), node_name() | undefined, limit()) -> 
    node_iterator().

-callback get_next_batch(node_iterator(), user_ctx:ctx()) ->
    {more | done, [tree_node()], node_iterator(), tree_node()} | no_access.
