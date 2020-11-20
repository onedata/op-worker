%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides ets cache for effective values. It is based on bounded_cache mechanism (see bounded_cache.erl
%%% in cluster_worker). Cache is cleaned automatically when defined size is exceeded (size is checked periodically).
%%% It allows calculation of value recursively (from file/dir to space) caching final and intermediate results for
%%% better performance.
%%% @end
%%%-------------------------------------------------------------------
-module(effective_value).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_or_calculate/3, get_or_calculate/4, get_or_calculate/5, get_or_calculate/6,
    get_or_calculate/7, invalidate/1]).

-type initial_calculation_info() :: term(). % Function that calculates value returns additional information
                                            % (CalculationInfo) that can be useful for further work
                                            % (e.g., calculating function can include datastore documents getting and
                                            % these documents can be used later without calling datastore).
                                            % Such returned value is provided to calculate function when processing
                                            % child in case of recursive value calculation.
                                            % This type represents initial value provided to function when processing
                                            % space directory (see get_or_calculate/7).
-type args() :: list().
-type in_critical_section() :: boolean() | parent. % parent = use section starting from parent directory

-define(CRITICAL_SECTION(Cache, Key), {effective_value_insert, Cache, Key}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_or_calculate(Cache, FileDoc, CalculateCallback, undefined)
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(bounded_cache:cache(), file_meta:doc(), bounded_cache:callback()) ->
    {ok, bounded_cache:value(), bounded_cache:additional_info()} | {error, term()}.
get_or_calculate(Cache, FileDoc, CalculateCallback) ->
    get_or_calculate(Cache, FileDoc, CalculateCallback, undefined).

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_or_calculate(Cache, FileDoc, CalculateCallback, InitialCalculationInfo, [])
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(bounded_cache:cache(), file_meta:doc(), bounded_cache:callback(),
    initial_calculation_info()) ->
    {ok, bounded_cache:value(), bounded_cache:additional_info()} | {error, term()}.
get_or_calculate(Cache, FileDoc, CalculateCallback, InitialCalculationInfo) ->
    get_or_calculate(Cache, FileDoc, CalculateCallback, InitialCalculationInfo, []).

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_or_calculate(Cache, FileDoc, CalculateCallback, InitialCalculationInfo,
%% Args, bounded_cache:get_timestamp())
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(bounded_cache:cache(), file_meta:doc(), bounded_cache:callback(),
    initial_calculation_info(), args()) ->
    {ok, bounded_cache:value(), bounded_cache:additional_info()} | {error, term()}.
get_or_calculate(Cache, FileDoc, CalculateCallback, InitialCalculationInfo, Args) ->
    get_or_calculate(Cache, FileDoc, CalculateCallback, InitialCalculationInfo, Args,
        bounded_cache:get_timestamp()).

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_or_calculate(Cache, Doc, CalculateCallback, InitialCalculationInfo, Args, Timestamp, false).
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(bounded_cache:cache(), file_meta:doc(), bounded_cache:callback(),
    initial_calculation_info(), args(), bounded_cache:timestamp()) ->
    {ok, bounded_cache:value(), bounded_cache:additional_info()} | {error, term()}.
get_or_calculate(Cache, Doc, CalculateCallback, InitialCalculationInfo, Args, Timestamp) ->
    get_or_calculate(Cache, Doc, CalculateCallback, InitialCalculationInfo, Args, Timestamp, false).

%%--------------------------------------------------------------------
%% @doc
%% Gets value from cache. If it is not found - uses callback to calculate it.
%% Calculated value is cached. Besides calculated value function returns additional information (CalculationInfo)
%% that is generated by calculate function and can be useful for further work
%% (e.g., calculating function can include datastore documents getting - see bounded_cache.erl in cluster_worker).
%% Calculate function processes single argument that is list [Doc, ParentValue, CalculationInfo | Args] where Doc is
%% file/directory file_meta document while ParentValue and CalculationInfo are results of calling this function on 
%% parent. Function is called recursively starting from space document. ParentValue and CalculationInfo are set to
%% undefined and InitialCalculationInfo for space document (it has no parent).
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(bounded_cache:cache(), file_meta:doc(), bounded_cache:callback(),
    initial_calculation_info(), args(), bounded_cache:timestamp(), in_critical_section()) ->
    {ok, bounded_cache:value(), bounded_cache:additional_info()} | {error, term()}.
get_or_calculate(Cache, #document{key = Key} = Doc, CalculateCallback, InitialCalculationInfo,
    Args, Timestamp, true) ->
    case bounded_cache:get(Cache, Key) of
        {ok, Value} ->
            {ok, Value, InitialCalculationInfo};
        {error, not_found} ->
            critical_section:run(?CRITICAL_SECTION(Cache, Key), fun() ->
                get_or_calculate(Cache, Doc, CalculateCallback, InitialCalculationInfo, Args,
                    Timestamp, parent)
            end)
    end;
get_or_calculate(Cache, #document{key = Key} = Doc, CalculateCallback, InitialCalculationInfo,
    Args, Timestamp, InCriticalSection) ->
    case bounded_cache:get(Cache, Key) of
        {ok, Value} ->
            {ok, Value, InitialCalculationInfo};
        {error, not_found} ->
            case {fslogic_uuid:is_space_dir_uuid(Key), fslogic_uuid:is_root_dir_uuid(Key)} of
                {false, false} ->
                    {ok, ParentUuid} = file_meta:get_parent_uuid(Doc),
                    case file_meta:get_including_deleted(ParentUuid) of
                        {ok, ParentDoc} ->
                            InCriticalSection2 = case InCriticalSection of
                                parent -> true;
                                _ -> InCriticalSection
                            end,
                            case get_or_calculate(Cache, ParentDoc, CalculateCallback, InitialCalculationInfo,
                                Args, Timestamp, InCriticalSection2) of
                                {ok, ParentValue, CalculationInfo} ->
                                    bounded_cache:calculate_and_cache(Cache, Key, CalculateCallback,
                                        [Doc, ParentValue, CalculationInfo | Args], Timestamp);
                                {error, _} = Error ->
                                    Error
                            end;
                        _ ->
                            {error, {file_meta_missing, ParentUuid}}
                    end;
                {false, true} ->
                    ?critical("Incorrect usage of effective_value cache ~p. Calculation has reached the global root directory."),
                    {error, root_dir_reached};
                {true, _} ->
                    bounded_cache:calculate_and_cache(Cache, Key, CalculateCallback,
                        [Doc, undefined, InitialCalculationInfo | Args], Timestamp)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv bounded_cache:invalidate(Cache)
%% @end
%%--------------------------------------------------------------------
-spec invalidate(bounded_cache:cache()) -> ok.
invalidate(Cache) ->
    bounded_cache:invalidate(Cache).
