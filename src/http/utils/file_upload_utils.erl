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
%%%                    accumulates one or more full blocks, they are flushed
%%%                    and the excess bytes stay in buffer.
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


-define(DEFAULT_WRITE_BLOCK_SIZE, 10485760). % 10 MB


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
            write_next_block_to_file(
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
            3 * BlockSize
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
    {ok, NewHandle, Bytes} = ?check(lfm:write(FileHandle, Offset, Data)),
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
-spec write_next_block_to_file(
    lfm:handle(),
    offset(),
    cowboy_req:req(),
    Buffer :: binary(),
    MaxBlockSize :: non_neg_integer(),
    ReadReqBodyFun :: read_req_body_fun(),
    ReadReqBodyOpts :: cowboy_req:read_body_opts()
) ->
    {ok, cowboy_req:req()}.
write_next_block_to_file(
    FileHandle, Offset, Req0, Buffer, MaxBlockSize, ReadReqBodyFun, ReadReqBodyOpts
) ->
    ActualBlockSize = MaxBlockSize - (Offset rem MaxBlockSize),
    ChunkSize = ActualBlockSize - byte_size(Buffer),
    case ReadReqBodyFun(Req0, ReadReqBodyOpts#{length => ChunkSize}) of
        {ok, Body, Req1} ->
            ?check(lfm:write(FileHandle, Offset, <<Buffer/binary, Body/binary>>)),
            {ok, Req1};
        {more, Body, Req1} ->
            ExcessSize = byte_size(Body) - ChunkSize,
            <<Chunk:ChunkSize/binary, ExcessBytes:ExcessSize/binary>> = Body,
            DataToWrite = <<Buffer/binary, Chunk/binary>>,

            DataToWriteSize = byte_size(DataToWrite),
            case DataToWriteSize >= ActualBlockSize of
                true ->
                    {ok, NewHandle, _} = ?check(lfm:write(
                        FileHandle, Offset, Chunk
                    )),
                    write_next_block_to_file(
                        NewHandle, Offset + ChunkSize, Req1, ExcessBytes, MaxBlockSize,
                        ReadReqBodyFun, ReadReqBodyOpts
                    );
                false ->
                    write_next_block_to_file(
                        FileHandle, Offset, Req1, DataToWrite, MaxBlockSize,
                        ReadReqBodyFun, ReadReqBodyOpts
                    )
            end
    end.
