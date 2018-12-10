%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Util functions that can be used to configure auto-cleaning mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_config).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").

-type config() :: #autocleaning_config{}.
-type rules() :: #autocleaning_rules{}.
-type rule_setting() :: autocleaning_rules:rule_setting().
-type error() :: {error, term()}.

-export_type([rule_setting/0, rules/0, config/0]).

%% API
-export([is_enabled/1, create_or_update/3, to_map/1,
    is_threshold_exceeded/2, is_target_reached/2,
    get_target/1, get_threshold/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec is_enabled(config() | undefined) -> boolean().
is_enabled(undefined) -> false;
is_enabled(#autocleaning_config{enabled = Enabled}) -> Enabled.

%%-------------------------------------------------------------------
%% @doc
%% This function creates or updates existing #autocleaning_config record
%% basing on NewConfiguration map.
%% @end
%%-------------------------------------------------------------------
-spec create_or_update(config() | undefined, maps:map(), non_neg_integer()) ->
    {ok, config()} | error().
create_or_update(undefined, NewConfiguration, SupportSize) ->
    create_or_update(default(SupportSize, SupportSize), NewConfiguration, SupportSize);
create_or_update(#autocleaning_config{
    enabled = CurrentEnabled,
    target = CurrentTarget,
    threshold = CurrentThreshold,
    rules = CurrentRules
}, NewConfiguration, SupportSize) ->
    try
        Enabled = autocleaning_utils:get_defined(enabled, NewConfiguration, CurrentEnabled),
        Target = autocleaning_utils:get_defined(target, NewConfiguration, CurrentTarget),
        Threshold = autocleaning_utils:get_defined(threshold, NewConfiguration, CurrentThreshold),
        autocleaning_utils:assert_boolean(Enabled, enabled),
        autocleaning_utils:assert_non_negative_integer(Target, target),
        autocleaning_utils:assert_non_negative_integer(Threshold, threshold),
        autocleaning_utils:assert_not_greater_then(Target, Threshold, target, threshold),
        autocleaning_utils:assert_not_greater_then(Threshold, SupportSize, threshold, support_size),

        RulesUpdateMap = autocleaning_utils:get_defined(rules, NewConfiguration, #{}),
        {ok, #autocleaning_config{
            enabled = Enabled,
            target = Target,
            threshold = Threshold,
            rules = autocleaning_rules:update(CurrentRules, RulesUpdateMap)
        }}
    catch
        throw:Error ->
            {error, Error}
    end.

-spec is_threshold_exceeded(non_neg_integer(), config()) -> boolean().
is_threshold_exceeded(0, #autocleaning_config{}) ->
    false;
is_threshold_exceeded(CurrentSize, #autocleaning_config{threshold = Threshold}) ->
    CurrentSize >= Threshold.

-spec is_target_reached(non_neg_integer(), config()) -> boolean().
is_target_reached(CurrentSize, #autocleaning_config{threshold = Target}) ->
    CurrentSize =< Target.

-spec get_target(config()) -> non_neg_integer().
get_target(#autocleaning_config{target = Target}) ->
    Target.

-spec get_threshold(config()) -> non_neg_integer().
get_threshold(#autocleaning_config{threshold = Threshold}) ->
    Threshold.

-spec to_map(config()) -> maps:map().
to_map(#autocleaning_config{
    enabled = Enabled,
    target = Target,
    threshold = Threshold,
    rules = Rules
}) ->
    #{
        enabled => Enabled,
        target => Target,
        threshold => Threshold,
        rules => autocleaning_rules:to_map(Rules)
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec default(non_neg_integer(), non_neg_integer()) -> config().
default(Target, Threshold) ->
    #autocleaning_config{
        target = Target,
        threshold = Threshold,
        rules = autocleaning_rules:default()
    }.

