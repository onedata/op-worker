%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module provides api which allowes to:
%% - check_if_synchronized - check if selected part of file is in sync with other providers (and also find out which provider has the newest version)
%% - mark_as_modified - mark some file part as modified, so other providers could fetch this data later
%% - mark_as_available - mark some file part as available, it means that method caller has newest version of file block, on local storage
%% - truncate - inform that file was truncated, the remote_parts ranges would fit to that new size
%% @end
%% ===================================================================
-module(fslogic_available_blocks).

-include("oneprovider_modules/fslogic/fslogic_available_blocks.hrl").
-include("oneprovider_modules/fslogic/ranges_struct.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").


% API
-export([mark_as_modified/2, mark_as_available/2, check_if_synchronized/3, truncate/2, mark_other_provider_changes/2]).

% Test API
-ifdef(TEST).
-export([byte_to_block_range/1]).
-endif.

%% mark_as_modified/2
%% ====================================================================
%% @doc @equiv mark_as_modified(Range, Mydoc, get_timestamp())
%% @end
-spec mark_as_modified(Range :: #byte_range{} | #offset_range{} | #block_range{}, available_blocks_doc()) -> available_blocks_doc().
%% ====================================================================
mark_as_modified(Range, #db_document{} = Mydoc) ->
    mark_as_modified(Range, Mydoc, get_timestamp()).

%% mark_as_modified/3
%% ====================================================================
%% @doc Marks given block/byte range as modified.
%% It extends available block list if necessary, and returns updated doc
%% @end
-spec mark_as_modified(Range :: #byte_range{} | #offset_range{} | #block_range{}, available_blocks_doc(), Timestamp :: non_neg_integer()) -> available_blocks_doc().
%% ====================================================================
mark_as_modified(#byte_range{} = ByteRange, #db_document{} = Mydoc, Timestamp) ->
    mark_as_modified(byte_to_block_range(ByteRange), Mydoc, Timestamp);
mark_as_modified(#offset_range{} = OffsetRange, #db_document{} = Mydoc, Timestamp) ->
    mark_as_modified(offset_to_block_range(OffsetRange), Mydoc, Timestamp);
mark_as_modified(#block_range{from = From, to = To}, #db_document{record = #available_blocks{file_parts = Parts} = RemoteLocation} = MyDoc, Timestamp) ->
    NewRemoteParts = ranges_struct:minimize(ranges_struct:merge(Parts, [#range{from = From, to = To, timestamp = Timestamp}])),
    MyDoc#db_document{record = RemoteLocation#available_blocks{file_parts = NewRemoteParts}}.

%% truncate/2
%% ====================================================================
%% @doc @equiv truncate(Size, MyDoc, get_timestamp())
%% @end
-spec truncate(Range :: {bytes, integer()} | integer(), MyDoc :: available_blocks_doc()) -> available_blocks_doc().
%% ====================================================================
truncate(Size, #db_document{} =  MyDoc) ->
    truncate(Size, MyDoc, get_timestamp()).

%% truncate/3
%% ====================================================================
%% @doc Truncates given list of ranges to given size
%% It extends available block list if necessary and returns updated document.
%% @end
-spec truncate(Range :: {bytes, integer()} | integer(), MyDoc :: available_blocks_doc(), Timestamp :: non_neg_integer()) -> available_blocks_doc().
%% ====================================================================
truncate({bytes, ByteSize}, #db_document{} =  MyDoc, Timestamp) ->
    truncate(byte_to_block(ByteSize), MyDoc, Timestamp);
truncate(BlockSize, #db_document{record = #available_blocks{file_parts = Parts} = RemoteLocation} = MyDoc, Timestamp) ->
    NewRemoteParts = ranges_struct:minimize(ranges_struct:truncate(Parts, #range{to = BlockSize-1, timestamp = Timestamp})),
    MyDoc#db_document{record = RemoteLocation#available_blocks{file_parts = NewRemoteParts}}.

%% mark_as_available/3
%% ====================================================================
%% @doc Marks given block range as available for provider.
%% It extends available block list if necessary, and returns updated document.
%% @end
-spec mark_as_available(Blocks :: [#range{}], MyDoc :: available_blocks_doc()) -> available_blocks_doc().
%% ====================================================================
mark_as_available(Blocks, #db_document{record = #available_blocks{file_parts = Parts} = RemoteLocation} = MyDoc) ->
    NewRemoteParts = ranges_struct:minimize(ranges_struct:merge(Parts, Blocks)),
    MyDoc#db_document{record = RemoteLocation#available_blocks{file_parts = NewRemoteParts}}.

%% check_if_synchronized/3
%% ====================================================================
%% @doc Checks if given range of bytes/blocks is in sync with other providers.
%% If so, the empty list is returned. If not, function returns list of unsynchronized parts.
%% Each range contains information about providers that have it up-to-date.
%% @end
-spec check_if_synchronized(Range :: #byte_range{} | #offset_range{} | #block_range{}, MyDoc :: available_blocks_doc(), OtherDocs :: [available_blocks_doc()]) ->
    [{ProviderId :: string(), AvailableBlocks :: [#range{}]}].
%% ====================================================================
check_if_synchronized(#byte_range{} = ByteRange, MyDoc, OtherDocs) ->
    check_if_synchronized(byte_to_block_range(ByteRange), MyDoc, OtherDocs);
check_if_synchronized(#offset_range{} = OffsetRange, MyDoc, OtherDocs) ->
    check_if_synchronized(offset_to_block_range(OffsetRange), MyDoc, OtherDocs);
check_if_synchronized(#block_range{from = From, to = To}, #db_document{record = #available_blocks{file_parts = Parts}}, OtherDocs) ->
    PartsOutOfSync = ranges_struct:minimize(ranges_struct:subtract([#range{from = From, to = To}], Parts)),
    lists:map(
        fun(#db_document{record = #available_blocks{file_parts = Parts_, provider_id = Id}}) ->
            {Id, ranges_struct:minimize(ranges_struct:subtract(PartsOutOfSync, ranges_struct:subtract(PartsOutOfSync, Parts_)))}
        end, OtherDocs).

%% mark_other_provider_changes/2
%% ====================================================================
%% @doc Deletes blocks changed by other provider, from local available_blocks map
%% @end
-spec mark_other_provider_changes(MyDoc :: available_blocks_doc(), OtherDoc :: available_blocks_doc()) -> available_blocks_doc().
%% ====================================================================
mark_other_provider_changes(MyDoc = #db_document{record = #available_blocks{file_parts = MyParts} = Location}, #db_document{record = #available_blocks{file_parts = OtherParts}}) ->
    NewParts = ranges_struct:minimize(ranges_struct:subtract_newer(MyParts, OtherParts)),
    MyDoc#db_document{record = Location#available_blocks{file_parts = NewParts}}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% byte_to_block_range/1
%% ====================================================================
%% @doc Converts byte range to block range, according to 'remote_block_size'
%% @end
-spec byte_to_block_range(#byte_range{}) -> #block_range{}.
%% ====================================================================
byte_to_block_range(#byte_range{from = From, to = To}) ->
    #block_range{from = From div ?remote_block_size, to = To div ?remote_block_size}.

%% offset_to_block_range/1
%% ====================================================================
%% @doc Converts offset to block range, according to 'remote_block_size'
%% @end
-spec offset_to_block_range(#offset_range{}) -> #block_range{}.
%% ====================================================================
offset_to_block_range(#offset_range{offset = Offset, size = Size}) ->
    byte_to_block_range(#byte_range{from = Offset, to = Offset + Size -1}).

%% byte_to_block/1
%% ====================================================================
%% @doc Converts bytes to blocks
%% @end
-spec byte_to_block(integer()) -> integer().
%% ====================================================================
byte_to_block(Byte) ->
    utils:ceil(Byte / ?remote_block_size).

%% get_timestamp/0
%% ====================================================================
%% @doc gets a timestamp in ms from the epoch
%% @end
-spec get_timestamp() -> non_neg_integer().
%% ====================================================================
get_timestamp() ->
    {Mega,Sec,Micro} = erlang:now(),
    (Mega*1000000+Sec)*1000000+Micro.