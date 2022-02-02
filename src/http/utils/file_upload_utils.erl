%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for handling file upload using http and cowboy.
%%% Depending on underlying storage file can be uploaded using one of the
%%% following modes:
%%% - block/buffered - request body will be read and buffered. When the buffer
%%%                    accumulates a full block, it is flushed and the excess
%%%                    bytes stay in buffer.
%%%                    The exception may be the first block, which may be of smaller
%%%                    size so that offset (for next writes) will be aligned to
%%%                    smallest multiple of block size (accessing blocks at well
%%%                    defined boundaries improves write performance on object
%%%                    storages as it minimizes blocks access).
%%%                    When the end of data stream is reached, the remaining data
%%%                    in the buffer is flushed.
%%% - stream - request body will be read and received bytes will be immediately
%%%            written without buffering until entire request has been processed.
%%% @end
%%%--------------------------------------------------------------------
-module(file_upload_utils).
-author("Bartosz Walkowicz").

-include("modules/logical_file_manager/lfm.hrl").

%% API
-export([get_preferable_write_block_size/1, upload_file/5]).


-type offset() :: non_neg_integer().


-type read_req_body_fun() :: fun((cowboy_req:req(), cowboy_req:read_body_opts()) ->
    {ok, binary(), cowboy_req:req()} | {more, binary(), cowboy_req:req()}
).


% default write block size for storages without defined block size
-define(DEFAULT_WRITE_BLOCK_SIZE, 10485760). % 10 MB


% how many blocks should be written at once (on storages with defined block size)
% the value was decided upon experimentally
-define(PREFERRED_STORAGE_WRITE_BLOCK_MULTIPLE, 3).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_preferable_write_block_size(od_space:id()) ->
    undefined | non_neg_integer().
get_preferable_write_block_size(SpaceId) ->
    {ok, StorageId} = space_logic:get_local_supporting_storage(SpaceId),
    get_preferable_storage_write_block_size(StorageId).


-spec upload_file(
    lfm:handle(),
    Offset :: non_neg_integer(),
    cowboy_req:req(),
    ReadReqBodyFun :: read_req_body_fun(),
    ReadReqBodyOpts :: cowboy_req:read_body_opts()
) ->
    {ok, cowboy_req:req()}.
upload_file(FileHandle, Offset, Req, ReadReqBodyFun, ReadReqBodyOpts) ->
    StorageId = lfm_context:get_storage_id(FileHandle),

    case get_preferable_storage_write_block_size(StorageId) of
        undefined ->
            write_req_body_to_file_in_stream(
                FileHandle, Offset, Req,
                ReadReqBodyFun, ReadReqBodyOpts
            );
        BlockSize ->
            write_req_body_to_file_in_blocks(
                FileHandle, Offset, Req, <<>>,
                BlockSize, ReadReqBodyFun, ReadReqBodyOpts
            )
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_preferable_storage_write_block_size(storage:id()) ->
    undefined | non_neg_integer().
get_preferable_storage_write_block_size(StorageId) ->
    case storage:get_block_size(StorageId) of
        0 ->
            ?DEFAULT_WRITE_BLOCK_SIZE;
        undefined ->
            undefined;
        BlockSize ->
            BlockSize * ?PREFERRED_STORAGE_WRITE_BLOCK_MULTIPLE
    end.


%% @private
-spec write_req_body_to_file_in_stream(
    lfm:handle(),
    Offset :: non_neg_integer(),
    cowboy_req:req(),
    ReadReqBodyFun :: read_req_body_fun(),
    ReadReqBodyOpts :: cowboy_req:read_body_opts()
) ->
    {ok, cowboy_req:req()}.
write_req_body_to_file_in_stream(
    FileHandle, Offset, Req0,
    ReadReqBodyFun, ReadReqBodyOpts
) ->
    {Status, Data, Req1} = ReadReqBodyFun(Req0, ReadReqBodyOpts#{length => ?DEFAULT_WRITE_BLOCK_SIZE}),
    {ok, NewHandle, Bytes} = ?lfm_check(lfm:write(FileHandle, Offset, Data)),
    case Status of
        more ->
            write_req_body_to_file_in_stream(
                NewHandle, Offset + Bytes, Req1,
                ReadReqBodyFun, ReadReqBodyOpts
            );
        ok ->
            {ok, Req1}
    end.


%% @private
-spec write_req_body_to_file_in_blocks(
    lfm:handle(),
    offset(),
    cowboy_req:req(),
    Buffer :: binary(),
    MaxBlockSize :: non_neg_integer(),
    ReadReqBodyFun :: read_req_body_fun(),
    ReadReqBodyOpts :: cowboy_req:read_body_opts()
) ->
    {ok, cowboy_req:req()}.
write_req_body_to_file_in_blocks(
    FileHandle, Offset, Req0, Buffer, BlockSize, ReadReqBodyFun, ReadReqBodyOpts
) ->
    % if the offset is not aligned with a multiple of MaxBlockSize, write a smaller chunk to make it so
    BytesRemainingInCurrentBlock = BlockSize - (Offset rem BlockSize),
    PreferredChunkSize = BytesRemainingInCurrentBlock - byte_size(Buffer),
    case ReadReqBodyFun(Req0, ReadReqBodyOpts#{length => PreferredChunkSize}) of
        {ok, ReceivedChunk, Req1} ->
            ?lfm_check(lfm:write(FileHandle, Offset, <<Buffer/binary, ReceivedChunk/binary>>)),
            {ok, Req1};
        {more, ReceivedChunk, Req1} ->
            ReceivedChunkSize = byte_size(ReceivedChunk),
            case ReceivedChunkSize >= PreferredChunkSize of
                false ->
                    write_req_body_to_file_in_blocks(
                        FileHandle, Offset, Req1, <<Buffer/binary, ReceivedChunk/binary>>, BlockSize,
                        ReadReqBodyFun, ReadReqBodyOpts
                    );
                true ->
                    ExcessSize = ReceivedChunkSize - PreferredChunkSize,
                    <<ChunkToWrite:PreferredChunkSize/binary, ExcessBytes:ExcessSize/binary>> = ReceivedChunk,
                    {ok, NewHandle, _} = ?lfm_check(lfm:write(
                        FileHandle, Offset, <<Buffer/binary, ChunkToWrite/binary>>
                    )),
                    write_req_body_to_file_in_blocks(
                        NewHandle, Offset + BytesRemainingInCurrentBlock, Req1, ExcessBytes, BlockSize,
                        ReadReqBodyFun, ReadReqBodyOpts
                    )
            end
    end.
