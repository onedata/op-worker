%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2023, ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides definitions used in recursive listing.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(RECURSIVE_LISTING_HRL).
-define(RECURSIVE_LISTING_HRL, 1).

-record(recursive_listing_result, {
    entries :: [any()], % [recursive_listing:result_entry()] but dialyzer does not accept it
    inaccessible_paths :: [any()], % [recursive_listing:node_path()] but dialyzer does not accept it
    pagination_token :: undefined | recursive_listing:pagination_token()
}).

-endif.
