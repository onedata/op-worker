%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for handling file upload.
%%% @end
%%%--------------------------------------------------------------------
-module(file_upload_utils).
-author("Bartosz Walkowicz").

-include("modules/logical_file_manager/lfm.hrl").

%% API
-export([get_preferable_write_block_size/1, upload_file/5]).

-type read_req_body_fun() :: fun((cowboy_req:req(), cowboy_req:read_body_opts()) ->
    {ok, binary(), cowboy_req:req()} | {more, binary(), cowboy_req:req()}
).


%%%===================================================================
%%% API
%%%===================================================================


%% @private
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
    case get_storage_preferable_write_block_size(StorageId) of
        undefined ->
            write_req_body_to_file_in_stream(
                FileHandle, Offset, Req,
                ReadReqBodyFun, ReadReqBodyOpts
            );
        MaxBlockSize ->
            write_req_body_to_file_in_blocks(
                FileHandle, Offset, Req, <<>>, MaxBlockSize,
                ReadReqBodyFun, ReadReqBodyOpts
            )
    end.


-spec get_preferable_write_block_size(od_space:id()) ->
    undefined | non_neg_integer().
get_preferable_write_block_size(SpaceId) ->
    {ok, StorageId} = space_logic:get_local_storage_id(SpaceId),
    get_storage_preferable_write_block_size(StorageId).


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
    Offset :: non_neg_integer(),
    cowboy_req:req(),
    Buffer :: binary(),
    MaxBlockSize:: non_neg_integer(),
    ReadReqBodyFun :: read_req_body_fun(),
    ReadReqBodyOpts :: cowboy_req:read_body_opts()
) ->
    {ok, cowboy_req:req()}.
write_req_body_to_file_in_blocks(
    FileHandle, Offset, Req0, Buffer, MaxBlockSize,
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
                    ChunkSize = MaxBlockSize * (DataToWriteSize div MaxBlockSize),
                    <<Chunk:ChunkSize/binary, ExcessBytes/binary>> = DataToWrite,
                    {ok, NewHandle, Written} = ?check(lfm:write(FileHandle, Offset, Chunk)),

                    write_req_body_to_file_in_blocks(
                        NewHandle, Offset + Written, Req1, ExcessBytes, MaxBlockSize,
                        ReadReqBodyFun, ReadReqBodyOpts
                    );
                false ->
                    write_req_body_to_file_in_blocks(
                        FileHandle, Offset, Req1, DataToWrite, MaxBlockSize,
                        ReadReqBodyFun, ReadReqBodyOpts
                    )
            end
    end.
