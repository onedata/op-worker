%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing store content browse options specialization for
%%% time_series store used in automation machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_time_series_store_content_browse_options).
-author("Bartosz Walkowicz").

-behaviour(atm_store_content_browse_options).

-include("modules/automation/atm_execution.hrl").

%% API
-export([sanitize/1]).

-define(MAX_WINDOW_LIMIT, 1000).
-define(DEFAULT_WINDOW_LIMIT, 1000).

-type timestamp() :: time:millis().
-type window_limit() :: 1..?MAX_WINDOW_LIMIT.

-type get_layout() :: #atm_time_series_store_content_get_layout_req{}.
-type get_slice() :: #atm_time_series_store_content_get_slice_req{}.

-type record() :: #atm_time_series_store_content_browse_options{}.

-export_type([timestamp/0, window_limit/0, get_layout/0, get_slice/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec sanitize(json_utils:json_map()) -> record().
sanitize(Data) when not is_map_key(<<"mode">>, Data) ->
    % Default request
    #atm_time_series_store_content_browse_options{
        request = #atm_time_series_store_content_get_layout_req{}
    };

sanitize(#{<<"mode">> := <<"layout">>}) ->
    #atm_time_series_store_content_browse_options{
        request = #atm_time_series_store_content_get_layout_req{}
    };

sanitize(Data = #{<<"mode">> := <<"slice">>}) ->
    DataSpec = ts_browser_middleware:data_spec(Data),
    SanitizedData = middleware_sanitizer:sanitize_data(Data, DataSpec),
    
    #atm_time_series_store_content_browse_options{
        request = #atm_time_series_store_content_get_slice_req{
            layout = maps:get(<<"layout">>, SanitizedData),
            start_timestamp = maps:get(<<"startTimestamp">>, SanitizedData, undefined),
            window_limit = maps:get(<<"windowLimit">>, SanitizedData, ?DEFAULT_WINDOW_LIMIT)
        }
    };

sanitize(#{<<"mode">> := _InvalidMode}) ->
    throw(?ERROR_BAD_VALUE_NOT_ALLOWED(<<"mode">>, [<<"layout">>, <<"slice">>])).
