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
-export([find/2, insert/3]).

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
-spec insert(Json :: maps:map() | undefined, JsonToInsert :: maps:map(), [binary()]) -> maps:map() | no_return().
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

%%%===================================================================
%%% Internal functions
%%%===================================================================
