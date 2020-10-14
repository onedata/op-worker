%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements some functions for parsing and processing parameters
%%% of http request (e.g. headers).
%%% @end
%%%--------------------------------------------------------------------
-module(http_parser).
-author("Bartosz Walkowicz").

%% API
-export([
    parse_range_header/2
]).

-type range() :: {RangeStart :: non_neg_integer(), RangeEnd :: non_neg_integer()}.

-export_type([range/0]).

-type threshold() :: non_neg_integer().


%%%===================================================================
%%% API
%%%===================================================================


-spec parse_range_header(cowboy_req:req(), threshold()) ->
    undefined | invalid | [range()].
parse_range_header(Req, Threshold) ->
    case cowboy_req:header(<<"range">>, Req) of
        undefined -> undefined;
        RawRange -> parse_bytes_ranges(RawRange, Threshold)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns byte ranges obtained from parsing http 'Range' header:
%%
%% >> parse_byte_range(<<"bytes=1-5,-3,2-">>, 10)
%% [{1, 5}, {7, 9}, {2, 9}]
%%
%% @end
%%--------------------------------------------------------------------
-spec parse_bytes_ranges(binary() | list(), threshold()) -> invalid | [range()].
parse_bytes_ranges(RangesBin, Threshold) when is_binary(RangesBin) ->
    case binary:split(RangesBin, <<"=">>, [global]) of
        [<<"bytes">>, RawRanges] ->
            parse_bytes_ranges(binary:split(RawRanges, <<",">>, [global]), Threshold);
        _ ->
            invalid
    end;
parse_bytes_ranges(RawRanges, Threshold) ->
    try
        lists:map(fun(RangeBin) -> parse_bytes_range(RangeBin, Threshold) end, RawRanges)
    catch _:_  ->
        invalid
    end.


%% @private
-spec parse_bytes_range(binary() | [binary()], threshold()) -> range() | no_return().
parse_bytes_range(RangeBin, Threshold) when is_binary(RangeBin) ->
    parse_bytes_range(binary:split(RangeBin, <<"-">>, [global]), Threshold);
parse_bytes_range([<<>>, FromEndBin], Threshold) ->
    validate_bytes_range({max(0, Threshold - binary_to_integer(FromEndBin)), Threshold - 1});
parse_bytes_range([RangeStartBin, <<>>], Threshold) ->
    validate_bytes_range({binary_to_integer(RangeStartBin), Threshold - 1});
parse_bytes_range([RangeStartBin, RangeEndBin], _Threshold) ->
    validate_bytes_range({binary_to_integer(RangeStartBin), binary_to_integer(RangeEndBin)});
parse_bytes_range(_InvalidRange, _Threshold) ->
    throw(invalid).


%% @private
-spec validate_bytes_range(range()) -> range() | no_return().
validate_bytes_range({RangeStart, RangeEnd}) when RangeStart > RangeEnd ->
    throw(invalid);
validate_bytes_range(Range) ->
    Range.
