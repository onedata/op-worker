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

-define(MAX_WINDOWS_LIMIT, 1000).
-define(DEFAULT_WINDOWS_LIMIT, 1000).

-type timestamp() :: time:millis().
-type windows_limit() :: 1..?MAX_WINDOWS_LIMIT.

-type get_layout() :: #get_atm_time_series_store_content_layout{}.
-type get_slice() :: #get_atm_time_series_store_content_slice{}.

-type record() :: #atm_time_series_store_content_browse_options{}.

-export_type([timestamp/0, windows_limit/0, get_layout/0, get_slice/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec sanitize(json_utils:json_map()) -> record().
sanitize(Data) when not is_map_key(<<"mode">>, Data) ->
    % Default request
    #atm_time_series_store_content_browse_options{
        request = #get_atm_time_series_store_content_layout{}
    };

sanitize(#{<<"mode">> := <<"layout">>}) ->
    #atm_time_series_store_content_browse_options{
        request = #get_atm_time_series_store_content_layout{}
    };

sanitize(Data = #{<<"mode">> := <<"slice">>}) ->
    SanitizedData = middleware_sanitizer:sanitize_data(Data, #{
        required => #{
            <<"layout">> => {json, fun(RequestedLayout) ->
                try
                    maps:foreach(fun(TimeSeriesId, MetricIds) ->
                        true = is_binary(TimeSeriesId) andalso
                            is_list(MetricIds) andalso
                            lists:all(fun is_binary/1, MetricIds)
                    end, RequestedLayout),
                    true
                catch _:_ ->
                    false
                end
            end}
        },
        optional => #{
            <<"startTimestamp">> => {integer, {not_lower_than, 0}},
            <<"windowsLimit">> => {integer, {between, 1, ?MAX_WINDOWS_LIMIT}}
        }
    }),
    #atm_time_series_store_content_browse_options{
        request = #get_atm_time_series_store_content_slice{
            layout = maps:get(<<"layout">>, SanitizedData),
            start_timestamp = maps:get(<<"startTimestamp">>, SanitizedData, undefined),
            windows_limit = maps:get(<<"windowsLimit">>, SanitizedData, ?DEFAULT_WINDOWS_LIMIT)
        }
    };

sanitize(#{<<"mode">> := _InvalidMode}) ->
    throw(?ERROR_BAD_VALUE_NOT_ALLOWED(<<"mode">>, [<<"layout">>, <<"slice">>])).
