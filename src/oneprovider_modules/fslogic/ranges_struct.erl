%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module implements data strunture that stores ranges with timestamps
%% @end
%% ===================================================================
-module(ranges_struct).
-include("oneprovider_modules/fslogic/ranges_struct.hrl").

%% API
-export([merge/2, truncate/2, minimize/1, subtract_newer/2, subtract/2, intersection/2, strip_timestamps/1]).

%% merge/3
%% ====================================================================
%% @doc merge two ranges_struct's, in case of conflicting ranges - highest timestamp wins
%% @end
-spec merge(Ranges1 :: ranges_struct(), Ranges2 :: ranges_struct()) -> ranges_struct().
merge([], Ranges2) ->
    Ranges2;
merge(Ranges1, []) ->
    Ranges1;
merge([#range{from = From1, to = To1} | Rest1], Ranges2) % ranges non empty
    when From1 > To1 ->
    merge(Rest1, Ranges2);
merge(Ranges1, [#range{from = From2, to = To2} | Rest2])
    when From2 > To2 ->
    merge(Ranges1, Rest2);
merge([#range{from = From1} | _] = Ranges1, [#range{from = From2} | _] = Ranges2) % first range in both lists is valid
    when From1 > From2 ->
    merge(Ranges2, Ranges1);
merge([Range1 = #range{to = To1} | Rest1], [#range{from = From2} | _] = Ranges2) % From1 < From2
    when To1 < From2 ->
    [Range1 | merge(Rest1, Ranges2)];
merge([Range1 = #range{from = From1} | Rest1], [#range{from = From2} | _] = Ranges2) % From2 <- [From1, To1]
    when From1 =/= From2 ->
    merge([Range1#range{to = From2-1}, Range1#range{from = From2} | Rest1], Ranges2);
merge([#range{to = To1} | _] = Ranges1, [#range{to = To2} | _] = Ranges2) % From1 == From2
    when To1 > To2 ->
    merge(Ranges2, Ranges1);
merge([#range{to = To1, timestamp = Time1} = Range1 | Rest1], [#range{timestamp = Time2} = Range2 | Rest2]) % From1 == From2 && To1 < To2
    when Time1 > Time2 ->
    [Range1#range{to = To1} | merge(Rest1, [Range2#range{from = To1+1} | Rest2])];
merge([#range{to = To1, timestamp = Time1} | Rest1], [#range{timestamp = Time2} = Range2 | Rest2])
    when Time1 =< Time2 ->
    [Range2#range{to = To1} | merge(Rest1, [Range2#range{from = To1+1} | Rest2])].

%% truncate/3
%% ====================================================================
%% @doc truncates ranges_struct up to given range (delete all ranges that are higher)
%% @end
-spec truncate(Ranges :: ranges_struct(), Range :: #range{}) -> ranges_struct().
truncate(Ranges, #range{to = Size} = Range) ->
    ReversedRanges = lists:reverse(Ranges),
    TruncatedReversedRanges = lists:dropwhile(
        fun(#range{from = From}) ->
            From > Size
        end, ReversedRanges),
    case TruncatedReversedRanges == [] of
        true -> [Range#range{from = 0}];
        _ ->
            [Last | Rest] = TruncatedReversedRanges,
            LastTo = Last#range.to,
            LastFrom = Last#range.from,
            case LastTo > Size of
                true -> lists:reverse([Last#range{from = LastFrom, to = Size} | Rest]);
                false -> lists:reverse([Range#range{from = LastTo + 1, to = Size}, Last | Rest])
            end
    end.

%% minimize/1
%% ====================================================================
%% @doc minimize ranges struct representation
%% @end
-spec minimize(Ranges :: ranges_struct()) -> ranges_struct().
minimize([]) ->
    [];
minimize([#range{from = From1, to = To1} | Rest1]) when From1 > To1 ->
    minimize(Rest1);
minimize([El1]) ->
    [El1];
minimize([#range{to = To1, timestamp = Time1} = El1, #range{from = From2, to = To2, timestamp = Time2} = El2 | Rest]) ->
    case To1 + 1 == From2 andalso Time1 == Time2 of
        true -> minimize([El1#range{to = To2} | Rest]);
        false -> [El1 | minimize([El2 | Rest])]
    end.

%% subtract/1
%% ====================================================================
%% @doc subtract second ranges_struct from first one
%% @end
-spec subtract(Ranges1 :: ranges_struct(), Ranges2 :: ranges_struct()) -> ranges_struct().
subtract(Ranges1, Ranges2) ->
    subtract_newer(Ranges1, [Range#range{timestamp = ?infinity} || Range <- Ranges2]).

%% subtract_newer/1
%% ====================================================================
%% @doc subtract second ranges_struct from first one (only ranges with higher timestamp)
%% @end
-spec subtract_newer(Ranges1 :: ranges_struct(), Ranges2 :: ranges_struct()) -> ranges_struct().
subtract_newer([], _) ->
    [];
subtract_newer(Ranges1, []) ->
    Ranges1;
subtract_newer([#range{from = From1, to = To1} | Rest1], Ranges2) % ranges non empty
    when From1 > To1 ->
    subtract_newer(Rest1, Ranges2);
subtract_newer(Ranges1, [#range{from = From2, to = To2} | Rest2])
    when From2 > To2 ->
    subtract_newer(Ranges1, Rest2);
subtract_newer([Range1 = #range{to = To1} | Rest1], [#range{from = From2} | _] = Ranges2)  % first range in both lists is valid
    when To1 < From2 ->
    [Range1 | subtract_newer(Rest1, Ranges2)];
subtract_newer([#range{from = From1} | _] = Ranges1, [#range{to = To2} | Rest2])
    when To2 < From1 ->
    subtract_newer(Ranges1, Rest2);
subtract_newer([Range1 = #range{from = From1} | Rest1], [#range{from = From2} | _] = Ranges2) % ranges have common part
    when From1 < From2 ->
    subtract_newer([Range1#range{to = From2 - 1}, Range1#range{from = From2} | Rest1], Ranges2);
subtract_newer([#range{from = From1} | _] = Ranges1, [Range2 = #range{from = From2} | Rest2])
    when From1 > From2 ->
    subtract_newer(Ranges1, [Range2#range{from = From1} | Rest2]);
subtract_newer([Range1 = #range{to = To1, timestamp = Time1} | Rest1], [#range{to = To2, timestamp = Time2} | Rest2]) % From1 == From2
    when To1 > To2 andalso Time1 >= Time2 ->
    [Range1#range{to = To2} | subtract_newer([Range1#range{from = To2+1} | Rest1], Rest2)];
subtract_newer([Range1 = #range{to = To1, timestamp = Time1} | Rest1], [#range{to = To2, timestamp = Time2} | Rest2])
    when To1 > To2 andalso Time1 < Time2 ->
    subtract_newer([Range1#range{from = To2+1} | Rest1], Rest2);
subtract_newer([Range1 = #range{to = To1, timestamp = Time1} | Rest1], [Range2 = #range{to = To2, timestamp = Time2} | Rest2]) % From1 == From2 && To1 <= To2
    when To1 =< To2 andalso Time1 >= Time2 ->
    [Range1 | subtract_newer(Rest1, [Range2#range{from = To1 + 1} | Rest2])];
subtract_newer([Range1 = #range{to = To1, timestamp = Time1} | Rest1], [Range2 = #range{to = To2, timestamp = Time2} | Rest2])
    when To1 =< To2 andalso Time1 < Time2 ->
    subtract_newer([Range1#range{from = To1+1} | Rest1], [Range2#range{from = To1+1} | Rest2]).

%% intersection/1
%% ====================================================================
%% @doc returns intersection of two ranges_structs, highest timestamp wins
%% @end
-spec intersection(Ranges1 :: ranges_struct(), Ranges2 :: ranges_struct()) -> ranges_struct().
intersection([], _) ->
    [];
intersection(_, []) ->
    [];
intersection([#range{from = From1, to = To1} | Rest1], Ranges2) % ranges non empty
    when From1 > To1 ->
    intersection(Rest1, Ranges2);
intersection(Ranges1, [#range{from = From2, to = To2} | Rest2])
    when From2 > To2 ->
    intersection(Ranges1, Rest2);
intersection([#range{from = From1} | _] = Ranges1, [#range{from = From2} | _] = Ranges2) % first range in both lists is valid
    when From1 > From2 ->
    intersection(Ranges2, Ranges1);
intersection([#range{to = To1} | Rest1], [#range{from = From2} | _] = Ranges2) % From1 < From2
    when To1 < From2 ->
    intersection(Rest1, Ranges2);
intersection([Range1 = #range{from = From1} | Rest1], [#range{from = From2} | _] = Ranges2) % From2 <- [From1, To1]
    when From1 =/= From2 ->
    intersection([Range1#range{from = From2} | Rest1], Ranges2);
intersection([#range{to = To1} | _] = Ranges1, [#range{to = To2} | _] = Ranges2) % From1 == From2
    when To1 > To2 ->
    intersection(Ranges2, Ranges1);
intersection([#range{to = To1, timestamp = Time1} = Range1 | Rest1], [#range{timestamp = Time2} = Range2 | Rest2]) % From1 == From2 && To1 < To2
    when Time1 > Time2 ->
    [Range1#range{to = To1} | intersection(Rest1, [Range2#range{from = To1+1} | Rest2])];
intersection([#range{to = To1, timestamp = Time1} | Rest1], [#range{timestamp = Time2} = Range2 | Rest2])
    when Time1 =< Time2 ->
    [Range2#range{to = To1} | intersection(Rest1, [Range2#range{from = To1+1} | Rest2])].

%% strip_timestamps/1
%% ====================================================================
%% @doc returns minimazied struct with all timestamps set to 0
%% @end
-spec strip_timestamps(Ranges :: ranges_struct()) -> ranges_struct().
strip_timestamps(Ranges) ->
    minimize([#range{from = From, to = To} || #range{from = From, to = To} <- Ranges]).