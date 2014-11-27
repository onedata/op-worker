%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains functions for operating on file system. This is for page modules to
%% be able to use the file system API without knowledge of underlying structures.
%% @end
%% ===================================================================
-module(fs_interface).

-include("oneprovider_modules/dao/dao_vfs.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/fslogic/fslogic_available_blocks.hrl").
-include("oneprovider_modules/fslogic/ranges_struct.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([file_parts_mock/1, get_all_available_blocks/1, get_full_file_path/1, get_file_uuid/1]).

file_parts_mock(_FilePath) ->
    random:seed(now()),
    Size = random:uniform(123213) + 500,

    {Size, [
        {<<"provider1">>, random:uniform(Size), lists:sort([random:uniform(Size) || _ <- lists:seq(1, 2 * random:uniform(4))])},
        {<<"prdfsgsdfovider2">>, random:uniform(Size), lists:sort([random:uniform(Size) || _ <- lists:seq(1, 2 * random:uniform(4))])},
        {<<"provr3">>, random:uniform(Size), lists:sort([random:uniform(Size) || _ <- lists:seq(1, 2 * random:uniform(4))])},
        {<<"prosdfvider4">>, random:uniform(Size), lists:sort([random:uniform(Size) || _ <- lists:seq(1, 2 * random:uniform(4))])}
    ]}.


get_all_available_blocks(FileID) ->
    {ok, AvailableBlocks} = logical_files_manager:list_all_available_blocks(FileID),
    {_, FileSize} = lists:foldl(
        fun(#available_blocks{file_size = {Timestamp, Size}}, {AccTmstp, AccSize}) ->
            case Timestamp > AccTmstp of
                true -> {Timestamp, Size};
                false -> {AccTmstp, AccSize}
            end
        end, {-1, 0}, AvailableBlocks),
    Blocks = lists:map(
        fun(#available_blocks{provider_id = ProviderID, file_parts = FileParts}) ->
            {ProvBytes, BlockList} = lists:foldl(
                fun(#range{from = From, to = To}, {AccBytes, AccBlocks}) ->
                    FromBytes = From * ?remote_block_size,
                    ToBytes = min(FileSize - 1, To * ?remote_block_size + ?remote_block_size - 1),
                    {AccBytes + ToBytes - FromBytes + 1, AccBlocks ++ [FromBytes, ToBytes]}
                end, {0, []}, FileParts),
            {ProviderID, ProvBytes, BlockList}
        end, AvailableBlocks),

    {FileSize, lists:flatten(lists:duplicate(3, Blocks))}.


%% get_full_file_path/1
%% ====================================================================
%% @doc Converts file path in binary to string representing the full file name (one that starts with spaces dir).
%% @end
-spec get_full_file_path(FilePath :: binary()) -> string().
%% ====================================================================
get_full_file_path(FilePath) ->
    {ok, FullPath} = fslogic_path:get_full_file_name(gui_str:binary_to_unicode_list(FilePath)),
    FullPath.


%% get_file_uuid/1
%% ====================================================================
%% @doc Retrieves file document's UUID for given filepath.
%% @end
-spec get_file_uuid(FullFilePath :: string()) -> string().
%% ====================================================================
get_file_uuid(FullFilePath) ->
    {ok, #db_document{uuid = FileID}} = fslogic_objects:get_file(FullFilePath),
    FileID.