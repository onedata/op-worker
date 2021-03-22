%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions for GUI download related modules.
%%% @end
%%%-------------------------------------------------------------------

-include("global_definitions.hrl").

-ifndef(GUI_DOWNLOAD_HRL).
-define(GUI_DOWNLOAD_HRL, 1).

-define(DEFAULT_READ_BLOCK_SIZE, application:get_env(
    ?APP_NAME, default_download_read_block_size, 10485760) % 10 MB
).
-define(MAX_DOWNLOAD_BUFFER_SIZE, application:get_env(
    ?APP_NAME, max_download_buffer_size, 20971520) % 20 MB
).


-record(download_ctx, {
    file_size :: file_meta:size(),
    
    file_handle :: lfm:handle(),
    read_block_size :: http_streamer:read_block_size(),
    max_read_blocks_count :: non_neg_integer(),
    encoding_fun :: fun((Data :: binary()) -> EncodedData :: binary()),
    tar_stream = undefined :: undefined | tar_utils:stream(),
    
    on_success_callback :: fun(() -> ok)
}).
-endif.
