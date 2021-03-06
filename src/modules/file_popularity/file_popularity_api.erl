%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% API for file-popularity management
%%% @end
%%%-------------------------------------------------------------------
-module(file_popularity_api).
-author("Jakub Kudzia").

-include("modules/datastore/file_popularity_config.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([enable/1, disable/1, is_enabled/1, get_configuration/1, delete_config/1, configure/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec enable(file_popularity_config:id()) -> ok | {error, term()}.
enable(SpaceId) ->
    configure(SpaceId, #{
        enabled => true,
        last_open_hour_weight => ?DEFAULT_LAST_OPEN_HOUR_WEIGHT,
        avg_open_count_per_day_weight => ?DEFAULT_AVG_OPEN_COUNT_PER_DAY_WEIGHT,
        max_avg_open_count_per_day => ?DEFAULT_MAX_AVG_OPEN_COUNT_PER_DAY
    }).

-spec configure(file_popularity_config:id(), map()) -> ok | errors:error() | {error, term()}.
configure(SpaceId, #{enabled := false}) ->
    disable(SpaceId);
configure(SpaceId, NewConfiguration) ->
    case assert_types_and_values(NewConfiguration) of
        ok ->
            case file_popularity_config:maybe_create_or_update(SpaceId, NewConfiguration) of
                {error, not_changed} -> ok;
                {ok, NewFPCDoc} ->
                    NewLastOpenHourWeight = file_popularity_config:get_last_open_hour_weight(NewFPCDoc),
                    NewAvgOpenCountPerDayWeight = file_popularity_config:get_avg_open_count_per_day_weight(NewFPCDoc),
                    NewMaxAvgOpenCountPerDay = file_popularity_config:get_max_avg_open_count_per_day(NewFPCDoc),
                    file_popularity_view:modify(SpaceId, NewLastOpenHourWeight, NewAvgOpenCountPerDayWeight, NewMaxAvgOpenCountPerDay)
            end;
        Error -> Error
    end.

-spec disable(file_popularity_config:id()) -> ok | {error, term()}.
disable(SpaceId) ->
    autocleaning_api:disable(SpaceId),
    case file_popularity_config:maybe_create_or_update(SpaceId, #{enabled => false}) of
        {ok, _} -> ok;
        {error, _} = Error ->  Error
    end.

-spec delete_config(file_popularity_config:id()) -> ok.
delete_config(SpaceId) ->
    file_popularity_config:delete(SpaceId).

-spec is_enabled(file_popularity_config:id()) -> boolean().
is_enabled(SpaceId) ->
    file_popularity_config:is_enabled(SpaceId).

-spec get_configuration(file_popularity_config:id()) -> {ok, map()} | {error, term()}.
get_configuration(SpaceId) ->
    case file_popularity_config:get(SpaceId) of
        {ok, FPCDoc} ->
            {ok, #{
                enabled => file_popularity_config:is_enabled(FPCDoc),
                last_open_hour_weight => file_popularity_config:get_last_open_hour_weight(FPCDoc),
                avg_open_count_per_day_weight => file_popularity_config:get_avg_open_count_per_day_weight(FPCDoc),
                max_avg_open_count_per_day => file_popularity_config:get_max_avg_open_count_per_day(FPCDoc),
                example_query => file_popularity_view:example_query(SpaceId)
            }};
        {error, not_found} ->
            {ok, #{enabled => false}};
        Error ->
            Error
    end.

-spec assert_types_and_values(map()) -> ok | {error, term()}.
assert_types_and_values(Configuration) ->
    maps:fold(fun
        (_Key, _Value, Error = {error, _}) ->
            Error;
        (Key, Value, ok) ->
            assert_type_and_value(Key, Value)
    end, ok, Configuration).

-spec assert_type_and_value(atom(), term()) -> ok | {error, term()}.
assert_type_and_value(enabled, Value) when is_boolean(Value) ->
    ok;
assert_type_and_value(enabled, _Value) ->
    ?ERROR_BAD_VALUE_BOOLEAN(<<"enabled">>);
assert_type_and_value(last_open_hour_weight, Value) ->
    assert_non_negative_number(last_open_hour_weight, Value);
assert_type_and_value(avg_open_count_per_day_weight, Value) ->
    assert_non_negative_number(avg_open_count_per_day_weight, Value);
assert_type_and_value(max_avg_open_count_per_day, Value) ->
    assert_non_negative_number(max_avg_open_count_per_day, Value);
assert_type_and_value(Other, _Value) ->
    ?ERROR_BAD_DATA(str_utils:to_binary(Other)).


-spec assert_non_negative_number(atom(), term()) -> ok | errors:error().
assert_non_negative_number(Key, Value) when not is_number(Value) ->
    ?ERROR_BAD_VALUE_INTEGER(atom_to_binary(Key, utf8));
assert_non_negative_number(Key, Value) when Value < 0 ->
    ?ERROR_BAD_VALUE_TOO_LOW(atom_to_binary(Key, utf8), 0);
assert_non_negative_number(_Key, _Value) ->
    ok.
