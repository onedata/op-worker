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

-ifndef(DOWNLOAD_HRL).
-define(DOWNLOAD_HRL, 1).

-define(DEFAULT_READ_BLOCK_SIZE, application:get_env(
    ?APP_NAME, default_download_read_block_size, 10485760) % 10 MB
).
-define(MAX_DOWNLOAD_BUFFER_SIZE, application:get_env(
    ?APP_NAME, max_download_buffer_size, 20971520) % 20 MB
).

% TODO VFS-6597 - update cowboy to at least ver 2.7 to fix streaming big files
% Due to lack of backpressure mechanism in cowboy when streaming files it must
% be additionally implemented. This module implementation checks cowboy process
% msg queue len to see if next data chunk can be queued. To account for
% differences in speed between network and storage a simple backoff is
% implemented with below boundaries.
-define(MIN_SEND_RETRY_DELAY, 100).
-define(MAX_SEND_RETRY_DELAY, 1000).

-record(download_ctx, {
    file_size :: file_meta:size(),
    
    file_handle :: lfm:handle(),
    read_block_size :: read_block_size(),
    max_read_blocks_count :: non_neg_integer(),
    encoding_fun :: fun((Data :: binary()) -> EncodedData :: binary()),
    tar_stream = undefined :: undefined | tar_utils:stream(),
    
    on_success_callback :: fun(() -> ok)
}).
-endif.
