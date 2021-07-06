%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for file_upload_utils module.
%%% @end
%%%--------------------------------------------------------------------
-module(file_upload_utils_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").


-record(req, {
    blocks_sizes :: [non_neg_integer()],
    read_blocks_count = 0 :: non_neg_integer()
}).
-record(chunk, {
    offset :: non_neg_integer(),
    size :: non_neg_integer()
}).

-define(PREFERABLE_STORAGE_WRITE_BLOCK_SIZE, 100).


%%%===================================================================
%%% Test generators
%%%===================================================================


get_blocks_for_sync_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun many_small_chunks_should_be_aggregated/1,
            fun chunks_should_be_written_in_multiple_of_block_size_if_possible/1,
            fun offset_should_be_adjusted_to_block_size_multiple/1,
            fun single_chunk_should_be_written_at_once_regardless_of_offset/1
        ]}.


%%%===================================================================
%%% Test functions
%%%===================================================================


many_small_chunks_should_be_aggregated(_) ->
    file_upload_utils:upload_file(
        file_handle,
        0,
        #req{blocks_sizes = [11, 12, 13, 14, 15, 16, 17, 18, 19]},
        fun read_req_body/2,
        #{}
    ),
    WrittenChunks = get_written_chunks([]),

    ?_assertEqual([
        #chunk{offset = 0, size = 100},
        #chunk{offset = 100, size = 35}
    ], WrittenChunks).


chunks_should_be_written_in_multiple_of_block_size_if_possible(_) ->
    file_upload_utils:upload_file(
        file_handle,
        0,
        #req{blocks_sizes = [540, 440, 333]},
        fun read_req_body/2,
        #{}
    ),
    WrittenChunks = get_written_chunks([]),

    ?_assertEqual([
        #chunk{offset = 0, size = 500},
        #chunk{offset = 500, size = 400},
        #chunk{offset = 900, size = 413}
    ], WrittenChunks).


offset_should_be_adjusted_to_block_size_multiple(_) ->
    file_upload_utils:upload_file(
        file_handle,
        23,
        #req{blocks_sizes = [456, 123, 27]},
        fun read_req_body/2,
        #{}
    ),
    WrittenChunks = get_written_chunks([]),

    ?_assertEqual([
        #chunk{offset = 23, size = 77},
        #chunk{offset = 100, size = 500},
        #chunk{offset = 600, size = 29}
    ], WrittenChunks).


single_chunk_should_be_written_at_once_regardless_of_offset(_) ->
    file_upload_utils:upload_file(
        file_handle,
        23,
        #req{blocks_sizes = [456]},
        fun read_req_body/2,
        #{}
    ),
    WrittenChunks = get_written_chunks([]),

    ?_assertEqual([
        #chunk{offset = 23, size = 456}
    ], WrittenChunks).


%%%===================================================================
%%% Test fixtures
%%%===================================================================


start() ->
    Self = self(),

    meck:new([storage, lfm, lfm_context], [passthrough]),
    meck:expect(storage, get_block_size, fun(_) ->
        ?PREFERABLE_STORAGE_WRITE_BLOCK_SIZE
    end),
    meck:expect(lfm, write, fun(FileHandle, Offset, Chunk) ->
        ChunkSize = byte_size(Chunk),
        Self ! {write, #chunk{offset = Offset, size = ChunkSize}},
        {ok, FileHandle, ChunkSize}
    end),
    meck:expect(lfm_context, get_storage_id, fun(_) ->
        storage_id
    end).


stop(_) ->
    ?assert(meck:validate([storage, lfm, lfm_context])),
    meck:unload().


%%%===================================================================
%%% Internal functions
%%%===================================================================


read_req_body(#req{blocks_sizes = BS, read_blocks_count = RBC} = Req, _) ->
    Status = case RBC == length(BS) - 1 of
        true -> ok;
        false -> more
    end,
    Data = crypto:strong_rand_bytes(lists:nth(RBC + 1, BS)),
    NewReq = Req#req{read_blocks_count = RBC + 1},

    {Status, Data, NewReq}.


get_written_chunks(Chunks) ->
    receive
        {write, Chunk} ->
            get_written_chunks([Chunk | Chunks])
    after 0 ->
        lists:reverse(Chunks)
    end.


-endif.
