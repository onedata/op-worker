%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides ets cache for effective values.
%%% @end
%%%-------------------------------------------------------------------
-module(effective_value).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_models.hrl").

%% API
-export([get_or_calculate/5, get_or_calculate/6, invalidate/1]).

-type traverse_cache() :: term().
-type args() :: list().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv get_or_calculate(Cache, FileDoc, CalculateCallback, TraverseCache,
%% Args, tmp_cache:get_timestamp())
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(tmp_cache:cache(), file_meta:doc(), tmp_cache:callback(),
    traverse_cache(), args()) ->
    {ok, tmp_cache:value(), tmp_cache:additional_info()} | {error, term()}.
get_or_calculate(Cache, FileDoc, CalculateCallback, TraverseCache, Args) ->
    get_or_calculate(Cache, FileDoc, CalculateCallback, TraverseCache, Args,
        tmp_cache:get_timestamp()).

%%--------------------------------------------------------------------
%% @doc
%% Gets value from cache. If it is not found - uses callback to calculate it.
%% Calculated value is cached.
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(tmp_cache:cache(), file_meta:doc(), tmp_cache:callback(),
    traverse_cache(), args(), tmp_cache:timestamp()) ->
    {ok, tmp_cache:value(), tmp_cache:additional_info()} | {error, term()}.
get_or_calculate(Cache, #document{key = Key} = Doc, CalculateCallback, TraverseCache, Args, Timestamp) ->
    case tmp_cache:get(Cache, Key) of
        {ok, Value} ->
            {ok, Value, TraverseCache};
        {error, not_found} ->
            SpaceId = (catch fslogic_uuid:space_dir_uuid_to_spaceid(Key)),
            case is_binary(SpaceId) of % is space?
                true ->
                    tmp_cache:calculate_and_cache(Cache, Key, CalculateCallback,
                        [Doc, undefined, TraverseCache | Args], Timestamp);
                _ ->
                    {ok, ParentDoc} = file_meta:get_parent(Doc),
                    {ok, ParentValue, CalculationInfo} = get_or_calculate(Cache, ParentDoc,
                        CalculateCallback, TraverseCache, Args, Timestamp),
                    tmp_cache:calculate_and_cache(Cache, Key, CalculateCallback,
                        [Doc, ParentValue, CalculationInfo | Args], Timestamp)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes all data in cache.
%% @end
%%--------------------------------------------------------------------
-spec invalidate(tmp_cache:cache()) -> ok.
invalidate(Cache) ->
    tmp_cache:invalidate(Cache).

%%%===================================================================
%%% Internal functions
%%%===================================================================
