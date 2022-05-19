%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing store content browse result specialization for
%%% time_series store used in automation machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_time_series_store_content_browse_result).
-author("Bartosz Walkowicz").

-behaviour(atm_store_content_browse_result).

-include("modules/automation/atm_execution.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([to_json/1]).

-type layout() :: #time_series_layout_result{}.
-type slice() :: #time_series_slice_result{}.

-type record() :: #atm_time_series_store_content_browse_result{}.

-export_type([layout/0, slice/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec to_json(record()) -> json_utils:json_term().
to_json(#atm_time_series_store_content_browse_result{
    result = #time_series_layout_result{} = Result
}) ->
    #{<<"layout">> => ts_browse_result:to_json(Result)};

to_json(#atm_time_series_store_content_browse_result{
    result = #time_series_slice_result{} = Result
}) ->
    #{<<"windows">> := Slice} = ts_browse_result:to_json(Result),
    #{<<"slice">> => Slice}.
