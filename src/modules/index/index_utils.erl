%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utils functions fon operating on index record.
%%% @end
%%%-------------------------------------------------------------------
-module(index_utils).
-author("Bartosz Walkowicz").

-include("http/rest/rest_api/rest_errors.hrl").

%% API
-export([sanitize_query_options/1]).


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

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec sanitize_query_options(list(), list()) -> list().
sanitize_query_options([], Options) ->
    Options;

sanitize_query_options([{bbox, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{bbox, Bbox} | Rest], Options) ->
    sanitize_query_options(Rest, [{bbox, Bbox} | Options]);

sanitize_query_options([{descending, true} | Rest], Options) ->
    sanitize_query_options(Rest, [descending | Options]);
sanitize_query_options([{descending, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{descending, _} | _Rest], _Options) ->
    throw(?ERROR_INVALID_DESCENDING);

sanitize_query_options([{endkey, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{endkey, Endkey} | Rest], Options) ->
    try
        sanitize_query_options(Rest, [{endkey, jiffy:decode(Endkey)} | Options])
    catch
        _:_ ->
            throw(?ERROR_INVALID_ENDKEY)
    end;

sanitize_query_options([{startkey, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{startkey, StartKey} | Rest], Options) ->
    try
        sanitize_query_options(Rest, [{startkey, jiffy:decode(StartKey)} | Options])
    catch
        _:_ ->
            throw(?ERROR_INVALID_STARTKEY)
    end;

sanitize_query_options([{inclusive_end, true} | Rest], Options) ->
    sanitize_query_options(Rest, [inclusive_end | Options]);
sanitize_query_options([{inclusive_end, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{inclusive_end, _} | _Rest], _Options) ->
    throw(?ERROR_INVALID_INCLUSIVE_END);

sanitize_query_options([{key, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{key, Key} | Rest], Options) ->
    try
        sanitize_query_options(Rest, [{key, jiffy:decode(Key)} | Options])
    catch
        _:_ ->
            throw(?ERROR_INVALID_KEY)
    end;

sanitize_query_options([{keys, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{keys, Keys} | Rest], Options) ->
    try
        DecodedKeys = jiffy:decode(Keys),
        true = is_list(DecodedKeys),
        sanitize_query_options(Rest, [{keys, DecodedKeys} | Options])
    catch
        _:_ ->
            throw(?ERROR_INVALID_KEYS)
    end;

sanitize_query_options([{limit, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{limit, Limit} | Rest], Options) ->
    case catch binary_to_integer(Limit) of
        N when is_integer(N) ->
            sanitize_query_options(Rest, [{limit, N} | Options]);
        _Error ->
            throw(?ERROR_INVALID_LIMIT)
    end;

sanitize_query_options([{skip, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{skip, Skip} | Rest], Options) ->
    case catch binary_to_integer(Skip) of
        N when is_integer(N) ->
            sanitize_query_options(Rest, [{skip, N} | Options]);
        _Error ->
            throw(?ERROR_INVALID_SKIP)
    end;

sanitize_query_options([{stale, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{stale, <<"ok">>} | Rest], Options) ->
    sanitize_query_options(Rest, [{stale, ok} | Options]);
sanitize_query_options([{stale, <<"update_after">>} | Rest], Options) ->
    sanitize_query_options(Rest, [{stale, update_after} | Options]);
sanitize_query_options([{stale, <<"false">>} | Rest], Options) ->
    sanitize_query_options(Rest, [{stale, false} | Options]);
sanitize_query_options([{stale, _} | _], _Options) ->
    throw(?ERROR_INVALID_STALE);

sanitize_query_options([{spatial, true} | Rest], Options) ->
    sanitize_query_options(Rest, [{spatial, true} | Options]);
sanitize_query_options([{spatial, false} | Rest], Options) ->
    sanitize_query_options(Rest, Options);

sanitize_query_options([{start_range, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{start_range, Endkey} | Rest], Options) ->
    StartRange = try
        {start_range, jiffy:decode(Endkey)}
    catch
        _:_ ->
            throw(?ERROR_INVALID_START_RANGE)
    end,
    sanitize_query_options(Rest, [StartRange | Options]);

sanitize_query_options([{end_range, undefined} | Rest], Options) ->
    sanitize_query_options(Rest, Options);
sanitize_query_options([{end_range, Endkey} | Rest], Options) ->
    EndRange = try
        {end_range, jiffy:decode(Endkey)}
    catch
        _:_ ->
            throw(?ERROR_INVALID_END_RANGE)
    end,
    sanitize_query_options(Rest, [EndRange | Options]);

sanitize_query_options([_ | Rest], Options) ->
    sanitize_query_options(Rest, Options).
