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

-include_lib("ctool/include/errors.hrl").

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
-spec find(Json :: map(), [binary()]) -> map() | no_return().
find(Json, []) ->
    Json;
find(Json, [Name | Rest]) ->
    IndexSize = get_index_size(Name),
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
            case maps:find(Name, Json) of
                error ->
                    throw({error, ?ENOATTR});
                {ok, SubJson} ->
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
-spec insert(Json :: map() | undefined, JsonToInsert :: map(), [binary()]) -> map() | no_return().
insert(_Json, JsonToInsert, []) ->
    JsonToInsert;
insert(undefined, JsonToInsert, [Name | Rest]) ->
    IndexSize = get_index_size(Name),
    case Name of
        <<"[", Element:IndexSize/binary, "]">> ->
            Index = binary_to_integer(Element) + 1,
            [null || _ <- lists:seq(1, Index - 1)] ++ [insert(undefined, JsonToInsert, Rest)];
        _ ->
            maps:put(Name, insert(undefined, JsonToInsert, Rest), #{})
    end;
insert(Json, JsonToInsert, [Name | Rest]) ->
    IndexSize = get_index_size(Name),
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
            SubJson = maps:get(Name, Json, undefined),
            maps:put(Name, insert(SubJson, JsonToInsert, Rest), Json);
        _ ->
            throw({error, ?ENOATTR})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Merge given list of json so that the resulting json would contain inherited entries
%% @end
%%--------------------------------------------------------------------
-spec merge(Jsons :: [map()]) -> map().
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
%% Get byte size of json array index stored in binary: e. g. "[12]" -> 2
%% @end
%%--------------------------------------------------------------------
-spec get_index_size(Name :: binary()) -> integer().
get_index_size(Name) ->
    byte_size(Name) - 2.

%%--------------------------------------------------------------------
%% @doc
%% Set nth element of list
%% @end
%%--------------------------------------------------------------------
-spec setnth(non_neg_integer(), list(), term()) -> list().
setnth(1, [_ | Rest], New) -> [New | Rest];
setnth(I, [E | Rest], New) -> [E | setnth(I - 1, Rest, New)].
