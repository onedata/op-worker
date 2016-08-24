%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for manipulation of custom metadata
%%% @end
%%%--------------------------------------------------------------------
-module(custom_meta_manipulation).
-author("Tomasz Lichon").

-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([find/2, insert/3, merge/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Find sub-json in json tree
%% @end
%%--------------------------------------------------------------------
-spec find(Json :: #{}, [binary()]) -> #{} | no_return().
find(Json, []) ->
    Json;
find(Json, [_Name | _Rest]) when not is_map(Json) ->
    throw({error, ?ENOATTR});
find(Json, [Name | Rest]) ->
    case maps:get(Name, Json, undefined) of
        undefined ->
            throw({error, ?ENOATTR});
        SubJson ->
            find(SubJson, Rest)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Insert sub-json to json tree
%% @end
%%--------------------------------------------------------------------
-spec insert(Json :: #{} | undefined, JsonToInsert :: #{}, [binary()]) -> #{} | no_return().
insert(_Json, JsonToInsert, []) ->
    JsonToInsert;
insert(undefined, JsonToInsert, [Name | Rest]) ->
    maps:put(Name, insert(undefined, JsonToInsert, Rest), #{});
insert(Json, JsonToInsert, [Name | Rest]) ->
    case maps:get(Name, Json, undefined) of
        undefined ->
            maps:put(Name, insert(undefined, JsonToInsert, Rest), Json);
        SubJson ->
            maps:put(Name, insert(SubJson, JsonToInsert, Rest), Json)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Merge given list of json so that the resulting json would contain inherited
%% entries.
%% @end
%%--------------------------------------------------------------------
-spec merge(Jsons :: [#{}]) -> #{}.
merge([]) ->
    #{};
merge([Json | _]) when not is_map(Json) ->
    Json;
merge([Json | Rest]) ->
    ParentJson = merge(Rest),
    case is_map(Json) andalso is_map(ParentJson) of
        true ->
            Keys = maps:keys(Json),
            ParentKeys = maps:keys(ParentJson),
            ParentOnlyKeys = ParentKeys -- Keys,
            CommonKeys = ParentKeys -- ParentOnlyKeys,
            JsonWithInheritedParentKeys = lists:foldl(
                fun(Key, Acc) ->
                    maps:put(Key, maps:get(Key, ParentJson), Acc)
                end, Json, ParentOnlyKeys),
            JsonWithMergedCommonKeys = lists:foldl(
                fun(Key, Acc) ->
                    Value = maps:get(Key, Acc),
                    ParentValue = maps:get(Key, ParentJson),
                    NewValue = merge([Value, ParentValue]),
                    maps:put(Key, NewValue, Acc)
                end, JsonWithInheritedParentKeys, CommonKeys),
            JsonWithMergedCommonKeys;
        false ->
            Json
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
