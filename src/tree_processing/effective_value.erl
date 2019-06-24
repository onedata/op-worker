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

%% API
-export([get_or_calculate/5, get_or_calculate/6, get_or_calculate/7,
    invalidate/1]).

-type traverse_cache() :: term().
-type args() :: list().
-type in_critical_section() :: boolean().

-define(CRITICAL_SECTION(Cache, Key), {effective_value_insert, Cache, Key}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_or_calculate(Cache, FileDoc, CalculateCallback, InitialTraverseCache,
%% Args, bounded_cache:get_timestamp())
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(bounded_cache:cache(), file_meta:doc(), bounded_cache:callback(),
    traverse_cache(), args()) ->
    {ok, bounded_cache:value(), bounded_cache:additional_info()} | {error, term()}.
get_or_calculate(Cache, FileDoc, CalculateCallback, InitialTraverseCache, Args) ->
    get_or_calculate(Cache, FileDoc, CalculateCallback, InitialTraverseCache, Args,
        bounded_cache:get_timestamp()).

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_or_calculate(Cache, Doc, CalculateCallback, InitialTraverseCache, Args, Timestamp, false).
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(bounded_cache:cache(), file_meta:doc(), bounded_cache:callback(),
    traverse_cache(), args(), bounded_cache:timestamp()) ->
    {ok, bounded_cache:value(), bounded_cache:additional_info()} | {error, term()}.
get_or_calculate(Cache, Doc, CalculateCallback, InitialTraverseCache, Args, Timestamp) ->
    get_or_calculate(Cache, Doc, CalculateCallback, InitialTraverseCache, Args, Timestamp, false).

%%--------------------------------------------------------------------
%% @doc
%% Gets value from cache. If it is not found - uses callback to calculate it.
%% Calculated value is cached.
%% Calculate function processes single argument that is list [Doc, ParentValue, CalculationInfo | Args] where Doc is
%% file/directory file_meta document while ParentValue and CalculationInfo are results of calling this function on 
%% parent. Function is called recursively starting from space document. ParentValue and CalculationInfo are set to 
%% undefined and InitialTraverseCache for space document (it has no parent).
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(bounded_cache:cache(), file_meta:doc(), bounded_cache:callback(),
    traverse_cache(), args(), bounded_cache:timestamp(), in_critical_section()) ->
    {ok, bounded_cache:value(), bounded_cache:additional_info()} | {error, term()}.
get_or_calculate(Cache, #document{key = Key} = Doc, CalculateCallback, InitialTraverseCache, Args, Timestamp, false) ->
    case bounded_cache:get(Cache, Key) of
        {ok, Value} ->
            {ok, Value, InitialTraverseCache};
        {error, not_found} ->
            SpaceId = (catch fslogic_uuid:space_dir_uuid_to_spaceid(Key)),
            case is_binary(SpaceId) of % is space?
                true ->
                    bounded_cache:calculate_and_cache(Cache, Key, CalculateCallback,
                        [Doc, undefined, InitialTraverseCache | Args], Timestamp);
                _ ->
                    {ok, ParentDoc} = file_meta:get_parent(Doc),
                    {ok, ParentValue, CalculationInfo} = get_or_calculate(Cache, ParentDoc,
                        CalculateCallback, InitialTraverseCache, Args, Timestamp),
                    bounded_cache:calculate_and_cache(Cache, Key, CalculateCallback,
                        [Doc, ParentValue, CalculationInfo | Args], Timestamp)
            end
    end;
get_or_calculate(Cache, #document{key = Key} = Doc, CalculateCallback, InitialTraverseCache, Args, Timestamp, true) ->
    case bounded_cache:get(Cache, Key) of
        {ok, Value} ->
            {ok, Value, InitialTraverseCache};
        {error, not_found} ->
            critical_section:run(?CRITICAL_SECTION(Cache, Key), fun() ->
                get_or_calculate(Cache, Doc, CalculateCallback, InitialTraverseCache, Args, Timestamp, false)
            end)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv bounded_cache:invalidate(Cache)
%% @end
%%--------------------------------------------------------------------
-spec invalidate(bounded_cache:cache()) -> ok.
invalidate(Cache) ->
    bounded_cache:invalidate(Cache).
