%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing store content browse options specialization for
%%% tree_forest store used in automation machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_tree_forest_store_content_browse_options).
-author("Bartosz Walkowicz").

-behaviour(atm_store_content_browse_options).

-include("modules/automation/atm_execution.hrl").

%% API
-export([sanitize/1]).


-type record() :: #atm_tree_forest_store_content_browse_options{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec sanitize(json_utils:json_map()) -> record() | no_return().
sanitize(#{<<"type">> := <<"treeForestStoreContentBrowseOptions">>} = Data) ->
    #atm_tree_forest_store_content_browse_options{
        listing_opts = atm_store_container_infinite_log_backend:sanitize_listing_opts(
            Data, timestamp_agnostic
        )
    }.
