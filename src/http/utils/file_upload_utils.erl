%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for handling file upload. Depending on underlying storage
%%% file can be uploaded using one of the following modes:
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
%% Exported for tests - TODO rm after switching to use onenv and real storages in tests
-export([get_storage_preferable_write_block_size/1]).


-type offset() :: non_neg_integer().
-type block_multiples_policy() :: allow_block_multiples | disallow_block_multiples.

-type read_req_body_fun() :: fun((cowboy_req:req(), cowboy_req:read_body_opts()) ->
    {ok, binary(), cowboy_req:req()} | {more, binary(), cowboy_req:req()}
).


%%%===================================================================
%%% API
%%%===================================================================


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
    case ?MODULE:get_storage_preferable_write_block_size(StorageId) of
        undefined ->
            write_req_body_to_file_in_stream(
                FileHandle, Offset, Req,
                ReadReqBodyFun, ReadReqBodyOpts
            );
        MaxBlockSize ->
            write_req_body_to_file_in_blocks(
                FileHandle, Offset, Req, MaxBlockSize,
                ReadReqBodyFun, ReadReqBodyOpts
            )
    end.


-spec get_preferable_write_block_size(od_space:id()) ->
    undefined | non_neg_integer().
get_preferable_write_block_size(SpaceId) ->
    {ok, StorageId} = space_logic:get_local_storage_id(SpaceId),
    ?MODULE:get_storage_preferable_write_block_size(StorageId).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_storage_preferable_write_block_size(storage:id()) ->
    undefined | non_neg_integer().
get_storage_preferable_write_block_size(StorageId) ->
    Helper = storage:get_helper(StorageId),
    helper:get_block_size(Helper).


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
    {Status, Data, Req1} = ReadReqBodyFun(Req0, ReadReqBodyOpts),
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
-spec write_req_body_to_file_in_blocks(
    lfm:handle(),
    offset(),
    cowboy_req:req(),
    MaxBlockSize :: non_neg_integer(),
    ReadReqBodyFun :: read_req_body_fun(),
    ReadReqBodyOpts :: cowboy_req:read_body_opts()
) ->
    {ok, cowboy_req:req()}.
write_req_body_to_file_in_blocks(
    FileHandle, Offset, Req, MaxBlockSize,
    ReadReqBodyFun, ReadReqBodyOpts
) ->
    AlignmentResult = write_first_block_if_partial(
        FileHandle, Offset, Req, <<>>, MaxBlockSize,
        ReadReqBodyFun, ReadReqBodyOpts
    ),
    case AlignmentResult of
        {ok, _} ->
            AlignmentResult;
        {more, NewHandle, NewOffset, Req1, Buffer} ->
            write_remaining_blocks_to_file(
                NewHandle, NewOffset, Req1, Buffer, MaxBlockSize,
                ReadReqBodyFun, ReadReqBodyOpts
            )
    end.


%% @private
-spec write_first_block_if_partial(
    lfm:handle(),
    offset(),
    cowboy_req:req(),
    Buffer :: binary(),
    MaxBlockSize :: non_neg_integer(),
    ReadReqBodyFun :: read_req_body_fun(),
    ReadReqBodyOpts :: cowboy_req:read_body_opts()
) ->
    {ok, cowboy_req:req()} |
    {more, lfm:handle(), offset(), cowboy_req:req(), ExcessBytes :: binary()}.
write_first_block_if_partial(
    FileHandle, Offset, Req, Buffer, MaxBlockSize,
    ReadReqBodyFun, ReadReqBodyOpts
) ->
    case Offset rem MaxBlockSize of
        0 ->
            {more, FileHandle, Offset, Req, Buffer};
        FirstBlockCurrentSize ->
            write_next_block_to_file(
                FileHandle, Offset, Req, Buffer,
                MaxBlockSize - FirstBlockCurrentSize, disallow_block_multiples,
                ReadReqBodyFun, ReadReqBodyOpts
            )
    end.


%% @private
-spec write_remaining_blocks_to_file(
    lfm:handle(),
    offset(),
    cowboy_req:req(),
    Buffer :: binary(),
    MaxBlockSize :: non_neg_integer(),
    ReadReqBodyFun :: read_req_body_fun(),
    ReadReqBodyOpts :: cowboy_req:read_body_opts()
) ->
    {ok, cowboy_req:req()}.
write_remaining_blocks_to_file(
    FileHandle, Offset, Req0, Buffer, MaxBlockSize,
    ReadReqBodyFun, ReadReqBodyOpts
) ->
    NextBlockWriteResult = write_next_block_to_file(
        FileHandle, Offset, Req0, Buffer, MaxBlockSize, allow_block_multiples,
        ReadReqBodyFun, ReadReqBodyOpts
    ),
    case NextBlockWriteResult of
        {ok, _} ->
            NextBlockWriteResult;
        {more, NewHandle, NewOffset, Req1, NewBuffer} ->
            write_remaining_blocks_to_file(
                NewHandle, NewOffset, Req1, NewBuffer, MaxBlockSize,
                ReadReqBodyFun, ReadReqBodyOpts
            )
    end.


%% @private
-spec write_next_block_to_file(
    lfm:handle(),
    offset(),
    cowboy_req:req(),
    Buffer :: binary(),
    MaxBlockSize :: non_neg_integer(),
    block_multiples_policy(),
    ReadReqBodyFun :: read_req_body_fun(),
    ReadReqBodyOpts :: cowboy_req:read_body_opts()
) ->
    {ok, cowboy_req:req()} |
    {more, lfm:handle(), offset(), cowboy_req:req(), ExcessBytes :: binary()}.
write_next_block_to_file(
    FileHandle, Offset, Req0, Buffer, MaxBlockSize, BlockMultiplesPolicy,
    ReadReqBodyFun, ReadReqBodyOpts
) ->
    case ReadReqBodyFun(Req0, ReadReqBodyOpts) of
        {ok, Body, Req1} ->
            ?check(lfm:write(FileHandle, Offset, <<Buffer/binary, Body/binary>>)),
            {ok, Req1};
        {more, Body, Req1} ->
            DataToWrite = <<Buffer/binary, Body/binary>>,
            DataToWriteSize = byte_size(DataToWrite),

            case DataToWriteSize >= MaxBlockSize of
                true ->
                    ChunkSize = case BlockMultiplesPolicy of
                        allow_block_multiples ->
                            MaxBlockSize * (DataToWriteSize div MaxBlockSize);
                        disallow_block_multiples ->
                            MaxBlockSize
                    end,
                    <<Chunk:ChunkSize/binary, ExcessBytes/binary>> = DataToWrite,

                    {ok, NewHandle, ChunkSize} = ?check(lfm:write(
                        FileHandle, Offset, Chunk
                    )),
                    {more, NewHandle, Offset + ChunkSize, Req1, ExcessBytes};
                false ->
                    write_next_block_to_file(
                        FileHandle, Offset, Req1, DataToWrite, MaxBlockSize,
                        BlockMultiplesPolicy, ReadReqBodyFun, ReadReqBodyOpts
                    )
            end
    end.
