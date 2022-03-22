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

%% API
-export([to_json/1]).

-type layout() :: #atm_time_series_store_content_layout{}.
-type slice() :: #atm_time_series_store_content_slice{}.

-type record() :: #atm_time_series_store_content_browse_result{}.

-export_type([layout/0, slice/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec to_json(record()) -> json_utils:json_term().
to_json(#atm_time_series_store_content_browse_result{
    result = #atm_time_series_store_content_layout{layout = Layout}
}) ->
    #{<<"layout">> => Layout};

to_json(#atm_time_series_store_content_browse_result{
    result = #atm_time_series_store_content_slice{slice = SliceValue}
}) ->
    #{<<"slice">> => SliceValue}.
