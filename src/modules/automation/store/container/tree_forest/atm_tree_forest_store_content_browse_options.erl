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

-define(MAX_BROWSE_LIMIT, 1000).
-define(DEFAULT_BROWSE_LIMIT, 1000).

-type start_from() :: undefined | {index, binary()}.
-type offset() :: integer().
-type limit() :: 1..?MAX_BROWSE_LIMIT.

-type record() :: #atm_tree_forest_store_content_browse_options{}.

-export_type([start_from/0, offset/0, limit/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec sanitize(json_utils:json_map()) -> record() | no_return().
sanitize(#{<<"type">> := <<"treeForestStoreContentBrowseOptions">>} = Data) ->
    SanitizedData = middleware_sanitizer:sanitize_data(Data, #{optional => #{
        <<"index">> => {binary, fun atm_infinite_log_based_stores_common:assert_numerical_index/1},
        <<"offset">> => {integer, any},
        <<"limit">> => {integer, {between, 1, ?MAX_BROWSE_LIMIT}}
    }}),
    #atm_tree_forest_store_content_browse_options{
        start_from = infer_start_from(SanitizedData),
        offset = maps:get(<<"offset">>, SanitizedData, 0),
        limit = maps:get(<<"limit">>, SanitizedData, ?DEFAULT_BROWSE_LIMIT)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec infer_start_from(json_utils:json_map()) -> start_from().
infer_start_from(#{<<"index">> := Index}) -> {index, Index};
infer_start_from(_) -> undefined.
