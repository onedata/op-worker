%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions for http download.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(HTTP_DOWNLOAD_HRL).
-define(HTTP_DOWNLOAD_HRL, 1).


% TODO VFS-6597 - update cowboy to at least ver 2.7 to fix streaming big files
% Due to lack of backpressure mechanism in cowboy when streaming files it must
% be additionally implemented. This module implementation checks cowboy process
% msg queue len to see if next data chunk can be queued. To account for
% differences in speed between network and storage a simple backoff is
% implemented with below boundaries.
-define(MIN_HTTP_SEND_RETRY_DELAY, 100).
-define(MAX_HTTP_SEND_RETRY_DELAY, 1000).


-endif.
