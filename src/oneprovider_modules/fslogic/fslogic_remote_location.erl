%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module provides api which allowes to:
%% - check if selected part of file is in sync with other providers (and also find out which provider has the newest version)
%% - mark some file part as modified, so other providers could synchronize it
%% @end
%% ===================================================================
-module(fslogic_remote_location).

-include("oneprovider_modules/fslogic/fslogic_remote_location.hrl").

% API
-export([mark_as_modified/3, check_if_synchronized/3]).

% Test API
-ifdef(TEST).
-export([byte_to_block_range/1, minimize_remote_parts_list/1]).
-endif.

%% mark_as_modified/3
%% ====================================================================
%% @doc Marks given block/byte range as modified by provider(ProviderId).
%% It extends remote_file_part list if necessary, and return's this list in its minimal
%% representation.
%% e. g. provider1 and provider2 have whole 100 block file in sync
%%       so there is one remote: [#remote_file_part{#block_range{from=0,to=100}, providers=[pr1_id, pr2_id}], in short [{0,100,[pr1,pr2]}]
%%       the provider 1 makes changes in block 5-10 and marks them as modified, new remote parts are:
%%       [{0,4,[pr1,pr2]}, {5,10,[pr1]}, {11,100,[pr1,pr2]}]
%% @end
-spec mark_as_modified(Range :: #byte_range{} | #block_range{}, RemoteParts :: [#remote_file_part{}], ProviderId :: binary()) -> [#remote_file_part{}].
%% ====================================================================
mark_as_modified(#byte_range{} = ByteRange, RemoteParts, ProviderId) ->
    mark_as_modified(byte_to_block_range(ByteRange), RemoteParts, ProviderId);
mark_as_modified(#block_range{} = BlockRange, RemoteParts, ProviderId) ->
    NewRemoteParts = mark_as_modified_recursive(BlockRange, RemoteParts, ProviderId),
    minimize_remote_parts_list(NewRemoteParts).

%% check_if_synchronized/3
%% ====================================================================
%% @doc Checks if given range of bytes/blocks is in sync with other providers.
%% If so, the empty list is returned. If not, function returns list of unsynchronized parts.
%% Each remote_file_part contains information about providers that have it up-to-date.
%% e. g. the remote parts are: (see mark_as_modified/3 doc)
%%       [{0,4,[pr1,pr2]}, {5,10,[pr1]}, {11,100,[pr1,pr2]}]
%%       provider2 check blocks 3-7, function returns parts that needs to be synchronized with pr1:
%%       {5,7,[pr1]}
%% @end
-spec check_if_synchronized(Range :: #byte_range{} | #block_range{}, RemoteParts :: [#remote_file_part{}], ProviderId :: binary()) -> [#remote_file_part{}].
%% ====================================================================
check_if_synchronized(#byte_range{} = ByteRange, RemoteParts, ProviderId) ->
    check_if_synchronized(byte_to_block_range(ByteRange), RemoteParts, ProviderId);
check_if_synchronized(_, [] , _) ->
    [];
check_if_synchronized(#block_range{from = GivenFrom, to = GivenTo}, _ , _) when GivenFrom > GivenTo ->
    [];
check_if_synchronized(#block_range{from = GivenFrom, to = GivenTo}, [#remote_file_part{range = #block_range{from = From, to = To}, providers = Providers} | _], ProviderId)
        when GivenFrom >= From andalso GivenTo =< To ->
    case lists:member(ProviderId, Providers) of
        true -> [];
        false -> [#remote_file_part{range = #block_range{from = GivenFrom, to = GivenTo}, providers = Providers}]
    end;
check_if_synchronized(#block_range{from = GivenFrom} = Range, [#remote_file_part{range = #block_range{from = From, to = To}, providers = Providers} | Rest], ProviderId)
        when GivenFrom >= From andalso GivenFrom =< To ->
    case lists:member(ProviderId, Providers) of
        true -> check_if_synchronized(Range#block_range{from = To + 1}, Rest, ProviderId);
        false -> [#remote_file_part{range = #block_range{from = GivenFrom, to = To}, providers = Providers} | check_if_synchronized(Range#block_range{from = To + 1}, Rest, ProviderId)]
    end;
check_if_synchronized(#block_range{from = GivenFrom} = Range, [#remote_file_part{range = #block_range{to = To}} | Rest], ProviderId)
        when GivenFrom > To ->
    check_if_synchronized(Range, Rest, ProviderId).

%% ====================================================================
%% Internal functions
%% ====================================================================

%% mark_as_modified_recursive/3
%% ====================================================================
%% @doc The internal, recursive version of mark_as_modified/3
%% @end
-spec mark_as_modified_recursive(Range :: #byte_range{} | #block_range{}, RemoteParts :: [#remote_file_part{}], ProviderId :: binary()) -> [#remote_file_part{}].
%% ====================================================================
mark_as_modified_recursive(_, [], _)->
    [];
mark_as_modified_recursive(#block_range{from = GivenFrom, to = GivenTo}, RemoteParts, _) when GivenFrom > GivenTo ->
    RemoteParts;
mark_as_modified_recursive(#block_range{from = GivenFrom} = Range, [#remote_file_part{range = #block_range{from = From, to = To}, providers = Providers} = First | Rest], ProviderId)
        when Providers == [ProviderId] andalso GivenFrom >= From andalso GivenFrom =< To ->
    [First | mark_as_modified_recursive(Range#block_range{from = To + 1}, Rest, ProviderId)];
mark_as_modified_recursive(#block_range{from = GivenFrom, to = GivenTo}, [#remote_file_part{range = #block_range{from = From, to = To}, providers = Providers} | Rest], ProviderId)
        when GivenFrom >= From andalso GivenTo =< To ->
    FirstRange =
        case GivenFrom == From of
            true -> [];
            false -> [#remote_file_part{range = #block_range{from = From, to = GivenFrom-1}, providers = Providers}]
        end,
    SecondRange =[#remote_file_part{range = #block_range{from = GivenFrom, to = GivenTo}, providers = [ProviderId]}],
    ThirdRange =
        case GivenTo == To of
            true -> [];
            false -> [#remote_file_part{range = #block_range{from = GivenTo+1, to = To}, providers = Providers}]
        end,
    FirstRange ++ SecondRange ++ ThirdRange ++ Rest;
mark_as_modified_recursive(#block_range{from = GivenFrom, to = GivenTo} = Range, [#remote_file_part{range = #block_range{from = From, to = To}, providers = Providers} | Rest], ProviderId)
        when GivenFrom >= From andalso GivenFrom =< To ->
    FirstRange =
        case GivenFrom == From of
            true -> [];
            false -> [#remote_file_part{range = #block_range{from = From, to = GivenFrom-1}, providers = Providers}]
        end,
    SecondRange = [#remote_file_part{range = #block_range{from = GivenFrom, to = To}, providers = [ProviderId]}],
    case Rest of
        [] -> FirstRange ++ SecondRange ++ [#remote_file_part{range = #block_range{from = To + 1, to = GivenTo}, providers = [ProviderId]}];
        _ -> FirstRange ++ SecondRange ++ mark_as_modified_recursive(Range#block_range{from = To + 1}, Rest, ProviderId)
    end;
mark_as_modified_recursive(#block_range{from = GivenFrom, to = GivenTo} = Range, [#remote_file_part{range = #block_range{to = To}} = First | Rest], ProviderId)
        when GivenFrom > To ->
    case Rest of
        [] -> [First, #remote_file_part{range = #block_range{from = To + 1, to = GivenTo}, providers = [ProviderId]}];
        _ -> [First | mark_as_modified_recursive(Range, Rest, ProviderId)]
    end.


%% byte_to_block_range/1
%% ====================================================================
%% @doc Converts byte range to block range, according to 'remote_block_size'
%% @end
-spec byte_to_block_range(#byte_range{}) -> #block_range{}.
%% ====================================================================
byte_to_block_range(#byte_range{from = From, to = To}) ->
    #block_range{from = From div ?remote_block_size, to = To div ?remote_block_size}.

%% minimize_remote_parts_list/3
%% ====================================================================
%% @doc The internal, recursive version of mark_as_modified/3
%% @end
-spec minimize_remote_parts_list(RemoteParts :: [#remote_file_part{}]) -> [#remote_file_part{}].
%% ====================================================================
minimize_remote_parts_list([]) ->
    [];
minimize_remote_parts_list([El1]) ->
    [El1];
minimize_remote_parts_list([
        #remote_file_part{range = #block_range{from = From1, to = To1}, providers = Providers1} = El1,
        #remote_file_part{range = #block_range{from = From2, to = To2}, providers = Providers2} = El2
        | Rest]) ->
    case To1 + 1 == From2 andalso Providers1 == Providers2 of
        true -> minimize_remote_parts_list([#remote_file_part{range = #block_range{from = From1, to = To2}, providers = Providers1} | Rest]);
        false -> [El1 | minimize_remote_parts_list([El2 | minimize_remote_parts_list(Rest)])]
    end.

