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
-spec find(Json :: maps:map(), [binary()]) -> maps:map() | no_return().
find(Json, []) ->
    Json;
find(Json, [Name | Rest]) ->
    NameSize = byte_size(Name),
    IndexSize = NameSize - 2,
    case Name of
        <<"[", Element:IndexSize/binary, "]">> when is_list(Json) ->
            Index = binary_to_integer(Element) + 1,
            case length(Json) < Index of
                true ->
                    throw({error, ?ENOATTR});
                false ->
                    SubJson = lists:nth(Index, Json),
                    find(SubJson, Rest)
            end;
        Name when is_map(Json) ->
            case maps:get(Name, Json, undefined) of
                undefined ->
                    throw({error, ?ENOATTR});
                SubJson ->
                    find(SubJson, Rest)
            end;
        _ ->
            throw({error, ?ENOATTR})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Insert sub-json to json tree
%% @end
%%--------------------------------------------------------------------
-spec insert(Json :: maps:map() | undefined, JsonToInsert :: maps:map(), [binary()]) -> maps:map() | no_return().
insert(_Json, JsonToInsert, []) ->
    JsonToInsert;
insert(undefined, JsonToInsert, [Name | Rest]) ->
    NameSize = byte_size(Name),
    IndexSize = NameSize - 2,
    case Name of
        <<"[", Element:IndexSize/binary, "]">> ->
            Index = binary_to_integer(Element) + 1,
            [null || _ <- lists:seq(1, Index - 1)] ++ [insert(undefined, JsonToInsert, Rest)];
        _ ->
            maps:put(Name, insert(undefined, JsonToInsert, Rest), #{})
    end;
insert(Json, JsonToInsert, [Name | Rest]) ->
    NameSize = byte_size(Name),
    IndexSize = NameSize - 2,
    case Name of
        <<"[", Element:IndexSize/binary, "]">> when is_list(Json) ->
            Index = binary_to_integer(Element) + 1,
            Length = length(Json),
            case Length < Index of
                true ->
                    Json ++ [null || _ <- lists:seq(Length + 1, Index - 1)] ++ [insert(undefined, JsonToInsert, Rest)];
                false ->
                    setnth(Index, Json, insert(undefined, JsonToInsert, Rest))
            end;
        _ when is_map(Json) ->
            case maps:get(Name, Json, undefined) of
                undefined ->
                    maps:put(Name, insert(undefined, JsonToInsert, Rest), Json);
                SubJson ->
                    maps:put(Name, insert(SubJson, JsonToInsert, Rest), Json)
            end;
        _ ->
            throw({error, ?ENOATTR})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Merge given list of json so that the resulting json would contain inherited entries
%% @end
%%--------------------------------------------------------------------
-spec merge(Jsons :: [maps:map()]) -> maps:map().
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

%%--------------------------------------------------------------------
%% @doc
%% Set nth element of list
%% @end
%%--------------------------------------------------------------------
-spec setnth(non_neg_integer(), list(), term()) -> list().
setnth(1, [_ | Rest], New) -> [New | Rest];
setnth(I, [E | Rest], New) -> [E | setnth(I - 1, Rest, New)].
