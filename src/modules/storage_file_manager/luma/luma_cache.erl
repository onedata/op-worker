%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements generic, temporary cache behaviour for luma
%%% and reverse_luma models. Entries are valid only for specific period.
%%% After that period, callback is called to acquire current value.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_cache).
-author("Jakub Kudzia").

-include("global_definitions.hrl").

-type key() :: binary().
-type model() :: reverse_luma:model() | luma:model().
-type value() :: od_user:id() | luma:user_ctx().
-type timestamp() :: non_neg_integer().

-export_type([model/0, value/0, timestamp/0]).

%% API
-export([get/5, invalidate/1]).

%%-------------------------------------------------------------------
%% @doc
%% Returns value saved in given model. If entry was saved longer ago
%% than LumaCacheTimeout then QueryFun will be called and new value
%% will be saved.
%% @end
%%-------------------------------------------------------------------
-spec get(module(), key(), function(), [term()], luma_config:cache_timeout()) ->
    {ok, Value :: value()} | {error, Reason :: term()}.
get(Module, Key, QueryFun, QueryArgs, LumaCacheTimeout) ->
    CurrentTimestamp = erlang:system_time(milli_seconds),
    case Module:get(Key) of
        {error, {not_found, _}} ->
            query_and_cache_value(Module, Key, QueryFun, QueryArgs,
                CurrentTimestamp);
        {ok, #document{value = ModelRecord}} ->
            LastTimestamp = Module:last_timestamp(ModelRecord),
            case cache_should_be_renewed(CurrentTimestamp, LastTimestamp,
                LumaCacheTimeout)
            of
                false ->
                    Value = Module:get_value(ModelRecord),
                    {ok, Value};
                true ->
                    query_and_cache_value(Module, Key, QueryFun, QueryArgs,
                        CurrentTimestamp)
            end
    end.

%%-------------------------------------------------------------------
%% @doc
%% Deletes all cached entries.
%% @end
%%-------------------------------------------------------------------
-spec invalidate(module()) -> ok.
invalidate(Module) ->
    {ok, Docs} = Module:list(),
    lists:foreach(fun(#document{key = Key}) ->
        Module:delete(Key)
    end, Docs).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Calls QueryFun and saves acquired value with CurrentTimestamp.
%% @end
%%-------------------------------------------------------------------
-spec query_and_cache_value(module(), key(), function(), [term()], timestamp()) ->
    {ok, Value :: value()} | {error, Reason :: term()}.
query_and_cache_value(Module, Key, QueryFun, QueryArgs, CurrentTimestamp) ->
    case erlang:apply(QueryFun, QueryArgs) of
        {ok, Value} ->
            Doc = #document{
                key = Key,
                value = Module:new(Value, CurrentTimestamp)
            },
            {ok, Key} = Module:save(Doc),
            {ok, Value};
        Error ->
            Error
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether cache should be renewed.
%% @end
%%-------------------------------------------------------------------
-spec cache_should_be_renewed(timestamp(), timestamp(), luma_config:cache_timeout()) -> boolean().
cache_should_be_renewed(CurrentTimestamp, LastTimestamp, CacheTimeout) ->
    (CurrentTimestamp - LastTimestamp) > CacheTimeout.
