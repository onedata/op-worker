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

-include_lib("ctool/include/logging.hrl").
-include("registered_names.hrl").
-include("oneprovider_modules/fslogic/fslogic_available_blocks.hrl").
-include("oneprovider_modules/fslogic/ranges_struct.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").

% API
-export([registered_requests/0, save_available_blocks/3, get_available_blocks/3, list_all_available_blocks/3, get_file_size/3]).
-export([mark_as_modified/2, mark_as_available/2, check_if_synchronized/3, truncate/2, mark_other_provider_changes/2]).
-export([db_sync_hook/0]).
-export([cast/1, call/1]).

-export([block_to_byte_range/2, byte_to_offset_range/1, ranges_to_offset_tuples/1]).
% Test API
-ifdef(TEST).
-export([byte_to_block_range/1]).
-endif.

%% ====================================================================
%% Available blocks cache functions
%% ====================================================================
%% Cache is registered in fslogic init function, request concerning the same document
%% Are always handled by one process. This functions should not be used directly, send
%% request to fslogic_worker instead

registered_requests() ->
    fun
        (ProtocolVersion, {save_available_blocks, Doc}, CacheName) -> fslogic_available_blocks:save_available_blocks(ProtocolVersion, CacheName, Doc);
        (ProtocolVersion, {get_available_blocks, FileId}, CacheName) -> fslogic_available_blocks:get_available_blocks(ProtocolVersion, CacheName, FileId);
        (ProtocolVersion, {list_all_available_blocks, FileId}, CacheName) -> fslogic_available_blocks:list_all_available_blocks(ProtocolVersion, CacheName, FileId);
        (ProtocolVersion, {get_file_size, FileId}, CacheName) -> fslogic_available_blocks:get_file_size(ProtocolVersion, CacheName, FileId)
    end.

save_available_blocks(ProtocolVersion, _CacheName, Doc) ->
    Pid = self(),
    ct:print("save begin ~p", [Pid]),
    {ok, _} = dao_lib:apply(dao_vfs, save_available_blocks, [Doc], ProtocolVersion),
    ct:print("save end ~p", [Pid]).

get_available_blocks(ProtocolVersion, _CacheName, FileId) ->
    {ok, AllDocs} = list_all_available_blocks(ProtocolVersion, _CacheName, FileId),
    ProviderId = cluster_manager_lib:get_provider_id(),
    [MyDoc] = lists:filter(fun(#db_document{record = #available_blocks{provider_id = Id}}) -> Id == ProviderId end, AllDocs),
    {ok,MyDoc}.

list_all_available_blocks(ProtocolVersion, _CacheName, FileId) ->
    {ok, AllDocs} = dao_lib:apply(dao_vfs, available_blocks_by_file_id, [FileId], ProtocolVersion),
    ProviderId = cluster_manager_lib:get_provider_id(),
    CreatedDocs = case lists:filter(fun(#db_document{record = #available_blocks{provider_id = Id}}) -> Id == ProviderId end, AllDocs) of
                      [] ->
                          {ok, Uuid} = dao_lib:apply(dao_vfs, save_available_blocks, [#available_blocks{file_id = FileId, provider_id = ProviderId}], ProtocolVersion),
                          {ok, Doc} = dao_lib:apply(dao_vfs, get_available_blocks, [Uuid], ProtocolVersion),
                          [Doc];
                      _ -> []
                  end,
    {ok, CreatedDocs ++ AllDocs}.

get_file_size(_ProtocolVersion, _CacheName, FileId) ->
    case logical_files_manager:getfileattr({uuid, FileId}) of
        {ok, #fileattributes{size = Size}} -> {ok, Size};
        Error -> Error
    end.

%% ====================================================================
%% functions for available_blocks documents modification
%% ====================================================================

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
            {Id, ranges_struct:minimize(ranges_struct:intersection(PartsOutOfSync, Parts_))}
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
%% Db sync hook, for tracking changes
%% ====================================================================

db_sync_hook() ->
    MyProviderId = cluster_manager_lib:get_provider_id(),
    fun
        (?FILES_DB_NAME, _, Uuid, #db_document{record = #available_blocks{provider_id = Id, file_id = FileId}}) when Id =/= MyProviderId ->
            {ok, Docs} = dao_lib:apply(dao_vfs, available_blocks_by_file_id, [FileId], 1),
            MyDocs = lists:filter(fun(#db_document{record = #available_blocks{provider_id = Id}}) -> Id == MyProviderId end, Docs),
            case MyDocs of
                [MyDoc] ->
                    [ChangedDoc] = lists:filter(fun(#db_document{uuid = Uuid_}) -> utils:ensure_binary(Uuid_) == utils:ensure_binary(Uuid) end, Docs),
                    #db_document{record = #available_blocks{file_parts = Ranges}} = NewDoc = fslogic_available_blocks:mark_other_provider_changes(MyDoc, ChangedDoc),

                    case NewDoc == MyDoc of
                        true -> ok;
                        _ ->
                            {ok, FullFileName} = logical_files_manager:get_file_full_name_by_uuid(FileId),
                            {ok, _} = fslogic_req_regular:update_file_block_map(FullFileName, fslogic_available_blocks:ranges_to_offset_tuples(Ranges), true),

                            fslogic_available_blocks:cast({save_available_blocks, NewDoc})
                    end;
                _ -> ok
            end;
        (_, _, _, _) -> ok
    end.

%% ====================================================================
%% Utility functions
%% ====================================================================

cast(Req) ->
    gen_server:call(?Dispatcher_Name, {fslogic, 1, Req}, ?CACHE_REQUEST_TIMEOUT).

call(Req) ->
    MsgId = make_ref(),
    gen_server:call(?Dispatcher_Name, {fslogic, 1, self(), MsgId, Req}, ?CACHE_REQUEST_TIMEOUT),
    receive
        {worker_answer, MsgId, Resp} -> Resp
    after ?CACHE_REQUEST_TIMEOUT ->
        ?error("Timeout in call to available_blocks process tree, req: ~p",[Req]),
        {error, timeout}
    end.

ranges_to_offset_tuples([]) -> [];
ranges_to_offset_tuples([#range{} = H | T]) ->
    #offset_range{offset = Offset, size = Size} = byte_to_offset_range(block_to_byte_range(H)),
    [{Offset, Size} | ranges_to_offset_tuples(T)].

%% ====================================================================
%% Internal functions
%% ====================================================================

block_to_byte_range(#range{from = From, to = To}) ->
    block_to_byte_range(#block_range{from = From, to = To});
block_to_byte_range(#block_range{from = From, to = To}) ->
    #byte_range{from = From * ?remote_block_size, to = (To+1) * ?remote_block_size}.

%% block_to_byte_range/2
%% ====================================================================
%% @doc Converts block range to byte range, according to 'remote_block_size'
%% @end
-spec block_to_byte_range(#block_range{}, FileByteSize :: integer()) -> #byte_range{}.
%% ====================================================================
block_to_byte_range(#range{from = From, to = To}, FileByteSize) ->
    block_to_byte_range(#block_range{from = From, to = To}, FileByteSize);
block_to_byte_range(#block_range{from = From, to = To}, FileByteSize) ->
    #byte_range{from = From * ?remote_block_size, to = min((To+1) * ?remote_block_size, FileByteSize-1)}.

%% byte_to_block_range/1
%% ====================================================================
%% @doc Converts byte range to block range, according to 'remote_block_size'
%% @end
-spec byte_to_block_range(#byte_range{}) -> #block_range{}.
%% ====================================================================
byte_to_block_range(#byte_range{from = From, to = To}) ->
    #block_range{from = From div ?remote_block_size, to = To div ?remote_block_size}.

byte_to_offset_range(#byte_range{from = From, to = To}) ->
    #offset_range{offset = From, size = To-From+1}.

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