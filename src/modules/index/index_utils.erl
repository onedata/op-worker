%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utils functions for operating on index record.
%%% @end
%%%-------------------------------------------------------------------
-module(index_utils).
-author("Bartosz Walkowicz").

-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([sanitize_query_options/1, escape_js_function/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Convert Request parameters to couchdb view query options
%% @end
%%--------------------------------------------------------------------
-spec sanitize_query_options(maps:map() | list()) -> list().
sanitize_query_options(Map) when is_map(Map) ->
    sanitize_query_options(maps:to_list(Map), []);
sanitize_query_options(RawOptionsList) ->
    sanitize_query_options(RawOptionsList, []).

%%--------------------------------------------------------------------
%% @doc escapes characters: \ " ' \n \t \v \0 \f \r
%%--------------------------------------------------------------------
-spec escape_js_function(Function :: binary()) -> binary().
escape_js_function(undefined) ->
    undefined;
escape_js_function(Function) ->
    escape_js_function(Function, [{<<"\\\\">>, <<"\\\\\\\\">>}, {<<"'">>, <<"\\\\'">>},
        {<<"\"">>, <<"\\\\\"">>}, {<<"\\n">>, <<"\\\\n">>}, {<<"\\t">>, <<"\\\\t">>},
        {<<"\\v">>, <<"\\\\v">>}, {<<"\\0">>, <<"\\\\0">>}, {<<"\\f">>, <<"\\\\f">>},
        {<<"\\r">>, <<"\\\\r">>}]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Escapes characters given as proplists, in provided Function.
%%--------------------------------------------------------------------
-spec escape_js_function(Function :: binary(), [{binary(), binary()}]) -> binary().
escape_js_function(Function, []) ->
    Function;
escape_js_function(Function, [{Pattern, Replacement} | Rest]) ->
    EscapedFunction = re:replace(Function, Pattern, Replacement, [{return, binary}, global]),
    escape_js_function(EscapedFunction, Rest).


-spec sanitize_query_options(list(), list()) -> list().
sanitize_query_options([], Options) ->
    Options;

sanitize_query_options([{<<"bbox">>, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{<<"bbox">>, Val} | Rest], Options) ->
    Bbox = try
        case Val of
            undefined ->
                ok;
            _ ->
                [W, S, E, N] = binary:split(Val, <<",">>, [global]),
                true = is_float(catch binary_to_float(W)) orelse is_integer(catch binary_to_integer(W)),
                true = is_float(catch binary_to_float(S)) orelse is_integer(catch binary_to_integer(S)),
                true = is_float(catch binary_to_float(E)) orelse is_integer(catch binary_to_integer(E)),
                true = is_float(catch binary_to_float(N)) orelse is_integer(catch binary_to_integer(N))
        end
    catch
        _:_ ->
            throw(?ERROR_INVALID_BBOX)
    end,
    sanitize_query_options(Rest, [{bbox, Bbox} | Options]);

sanitize_query_options([{<<"descending">>, true} | Rest], Options) ->
    sanitize_query_options(Rest, [descending | Options]);
sanitize_query_options([{<<"descending">>, false} | Rest], Options) ->
    sanitize_query_options(Rest, Options);

sanitize_query_options([{<<"endkey">>, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{<<"endkey">>, Endkey} | Rest], Options) ->
    try
        sanitize_query_options(Rest, [{endkey, jiffy:decode(Endkey)} | Options])
    catch
        _:_ ->
            throw(?ERROR_BAD_VALUE_JSON(<<"endkey">>))
    end;

sanitize_query_options([{<<"startkey">>, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{<<"startkey">>, StartKey} | Rest], Options) ->
    try
        sanitize_query_options(Rest, [{startkey, jiffy:decode(StartKey)} | Options])
    catch
        _:_ ->
            throw(?ERROR_BAD_VALUE_JSON(<<"startkey">>))
    end;

sanitize_query_options([{<<"inclusive_end">>, true} | Rest], Options) ->
    sanitize_query_options(Rest, [inclusive_end | Options]);
sanitize_query_options([{<<"inclusive_end">>, false} | Rest], Options) ->
    sanitize_query_options(Rest, Options);

sanitize_query_options([{<<"key">>, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{<<"key">>, Key} | Rest], Options) ->
    try
        sanitize_query_options(Rest, [{key, jiffy:decode(Key)} | Options])
    catch
        _:_ ->
            throw(?ERROR_BAD_VALUE_JSON(<<"key">>))
    end;

sanitize_query_options([{<<"keys">>, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{<<"keys">>, Keys} | Rest], Options) ->
    try
        DecodedKeys = jiffy:decode(Keys),
        true = is_list(DecodedKeys),
        sanitize_query_options(Rest, [{keys, DecodedKeys} | Options])
    catch
        _:_ ->
            throw(?ERROR_BAD_VALUE_JSON(<<"keys">>))
    end;

sanitize_query_options([{<<"limit">>, Limit} | Rest], Options) ->
    sanitize_query_options(Rest, [{limit, Limit} | Options]);

sanitize_query_options([{<<"skip">>, Skip} | Rest], Options) ->
    sanitize_query_options(Rest, [{skip, Skip} | Options]);

sanitize_query_options([{<<"stale">>, <<"ok">>} | Rest], Options) ->
    sanitize_query_options(Rest, [{stale, ok} | Options]);
sanitize_query_options([{<<"stale">>, <<"update_after">>} | Rest], Options) ->
    sanitize_query_options(Rest, [{stale, update_after} | Options]);
sanitize_query_options([{<<"stale">>, <<"false">>} | Rest], Options) ->
    sanitize_query_options(Rest, [{stale, false} | Options]);

sanitize_query_options([{<<"spatial">>, true} | Rest], Options) ->
    sanitize_query_options(Rest, [{spatial, true} | Options]);
sanitize_query_options([{<<"spatial">>, false} | Rest], Options) ->
    sanitize_query_options(Rest, Options);

sanitize_query_options([{<<"start_range">>, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{<<"start_range">>, Endkey} | Rest], Options) ->
    StartRange = try
        {start_range, jiffy:decode(Endkey)}
    catch
        _:_ ->
            throw(?ERROR_BAD_VALUE_JSON(<<"start_range">>))
    end,
    sanitize_query_options(Rest, [StartRange | Options]);

sanitize_query_options([{<<"end_range">>, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{<<"end_range">>, Endkey} | Rest], Options) ->
    EndRange = try
        {end_range, jiffy:decode(Endkey)}
    catch
        _:_ ->
            throw(?ERROR_BAD_VALUE_JSON(<<"end_range">>))
    end,
    sanitize_query_options(Rest, [EndRange | Options]);

sanitize_query_options([_ | Rest], Options) ->
    sanitize_query_options(Rest, Options).
