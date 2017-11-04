%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for configuring autocleaning setup.
%%%-------------------------------------------------------------------
-module(autocleaning_config).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").

-type config() :: #autocleaning_config{}.

-export_type([config/0]).

%% API
-export([create_or_update/2, should_start_autoclean/2, get_target/1,
    get_lower_size_limit/1, get_upper_size_limit/1, should_stop_autoclean/2,
    get_max_inactive_limit/1, get_threshold/1]).

%%defaults
-define(DEFAULT_LOWER_SIZE_LIMIT, 1).
-define(DEFAULT_UPPER_SIZE_LIMIT, 1125899906842625). % 1 PiB + 1B
-define(DEFAULT_MAX_NOT_OPENED, 0).

%%-------------------------------------------------------------------
%% @doc
%% Creates or updates autocleaning config
%% @end
%%-------------------------------------------------------------------
-spec create_or_update(undefined | autocleaning:autocleaning(), maps:map()) ->
    autocleaning:autocleaning() | {error, term()}.
create_or_update(undefined, Settings) ->
    Target = maps:get(target, Settings, undefined),
    Threshold = maps:get(threshold, Settings, undefined),
    case {validate_target(Target), validate_threshold(Threshold)} of
        {ok, ok} ->
            Lower = maps:get(lower_file_size_limit, Settings, ?DEFAULT_LOWER_SIZE_LIMIT),
            Upper = maps:get(upper_file_size_limit, Settings, ?DEFAULT_UPPER_SIZE_LIMIT),
            MaxNotOpened = maps:get(max_file_not_opened_hours, Settings, ?DEFAULT_MAX_NOT_OPENED),
            #autocleaning_config{
                lower_file_size_limit = ensure_lower_limit_defined_and_non_negative(Lower, ?DEFAULT_LOWER_SIZE_LIMIT),
                upper_file_size_limit = utils:ensure_defined(Upper, undefined, ?DEFAULT_UPPER_SIZE_LIMIT),
                max_file_not_opened_hours = utils:ensure_defined(MaxNotOpened, undefined, ?DEFAULT_MAX_NOT_OPENED),
                target = Target,
                threshold = Threshold
            };
        {Error = {error, _}, _} -> Error;
        {_, Error = {error, _}} -> Error
    end;
create_or_update(#autocleaning_config{
    lower_file_size_limit = PreviousLower,
    upper_file_size_limit = PreviousUpper,
    max_file_not_opened_hours = PreviousMaxNotOpened,
    target = PreviousTarget,
    threshold = OldThreshold
}, Settings) ->
    Lower = maps:get(lower_file_size_limit, Settings, PreviousLower),
    Upper = maps:get(upper_file_size_limit, Settings, PreviousUpper),
    MaxNotOpened = maps:get(max_file_not_opened_hours, Settings, PreviousMaxNotOpened),
    Target = maps:get(target, Settings, PreviousTarget),
    Threshold = maps:get(threshold, Settings, OldThreshold),
    #autocleaning_config{
        lower_file_size_limit = ensure_lower_limit_defined_and_non_negative(Lower, PreviousLower),
        upper_file_size_limit = utils:ensure_defined(Upper, undefined, PreviousUpper),
        max_file_not_opened_hours = utils:ensure_defined(MaxNotOpened, undefined, PreviousMaxNotOpened),
        target = utils:ensure_defined(Target, undefined, PreviousTarget),
        threshold = utils:ensure_defined(Threshold, undefined, OldThreshold)
    }.

%%-------------------------------------------------------------------
%% @doc
%% Checks whether autocleaning should be started.
%% @end
%%-------------------------------------------------------------------
-spec should_start_autoclean(non_neg_integer(), autocleaning:autocleaning()) -> boolean().
should_start_autoclean(0, #autocleaning_config{}) ->
    false;
should_start_autoclean(CurrentSize, #autocleaning_config{threshold = Threshold}) ->
    CurrentSize >= Threshold.

%%-------------------------------------------------------------------
%% @doc
%% Checks whether autocleaning should be stopped.
%% @end
%%-------------------------------------------------------------------
-spec should_stop_autoclean(non_neg_integer(), autocleaning:autocleaning()) -> boolean().
should_stop_autoclean(CurrentSize, #autocleaning_config{threshold = Target}) ->
    CurrentSize =< Target.

%%-------------------------------------------------------------------
%% @doc
%% Getter for target field.
%% @end
%%-------------------------------------------------------------------
-spec get_target(autocleaning:autocleaning()) -> non_neg_integer().
get_target(#autocleaning_config{target = Target}) ->
    Target.

%%-------------------------------------------------------------------
%% @doc
%% Getter for threshold field.
%% @end
%%-------------------------------------------------------------------
-spec get_threshold(autocleaning:autocleaning()) -> non_neg_integer().
get_threshold(#autocleaning_config{threshold = Threshold}) ->
    Threshold.

%%-------------------------------------------------------------------
%% @doc
%% Getter for lower_file_size_limit field.
%% @end
%%-------------------------------------------------------------------
-spec get_lower_size_limit(autocleaning:autocleaning()) -> non_neg_integer().
get_lower_size_limit(#autocleaning_config{lower_file_size_limit = LowerLimit}) ->
    LowerLimit.

%%-------------------------------------------------------------------
%% @doc
%% Getter for upper_file_size_limit field.
%% @end
%%-------------------------------------------------------------------
-spec get_upper_size_limit(autocleaning:autocleaning()) -> non_neg_integer().
get_upper_size_limit(#autocleaning_config{upper_file_size_limit = UpperLimit}) ->
    UpperLimit.

%%-------------------------------------------------------------------
%% @doc
%% Getter for max_file_not_opened_hours field.
%% @end
%%-------------------------------------------------------------------
-spec get_max_inactive_limit(autocleaning:autocleaning()) -> non_neg_integer().
get_max_inactive_limit(#autocleaning_config{max_file_not_opened_hours = MaxNotOpened}) ->
    MaxNotOpened.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Validates whether target field is defined and if it's non_negative integer.
%% @end
%%-------------------------------------------------------------------
-spec validate_target(non_neg_integer() | undefined) -> ok | {error, term()}.
validate_target(Value) ->
    validate(Value, target).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Validates whether threshold field is defined and if it's non_negative integer.
%% @end
%%-------------------------------------------------------------------
-spec validate_threshold(non_neg_integer() | undefined) -> ok | {error, term()}.
validate_threshold(Value) ->
    validate(Value, threshold).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Validates whether given field is defined and if it's non_negative integer.
%% @end
%%-------------------------------------------------------------------
-spec validate(undefined | non_neg_integer(), atom()) -> ok | {error, term()}.
validate(undefined, Field) ->
    {error, {undefined_parameter, Field}};
validate(Value, Field) when Value < 0 ->
    {error, {negative_value_not_allowed, Field}};
validate(Value, _Field) when is_integer(Value) ->
    ok;
validate(Value, Field) when is_integer(Value) ->
    {error, {illegal_type, Field}}.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Ensures that lower file size limit is defined and if it's
%% non_negative_integer.
%% @end
%%-------------------------------------------------------------------
-spec ensure_lower_limit_defined_and_non_negative(undefined | non_neg_integer(),
    non_neg_integer()) -> non_neg_integer().
ensure_lower_limit_defined_and_non_negative(undefined, Default) ->
    Default;
ensure_lower_limit_defined_and_non_negative(Lower, Default) when Lower < 0 ->
    Default;
ensure_lower_limit_defined_and_non_negative(Lower, _Default) ->
    Lower.