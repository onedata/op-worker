%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros used in modules that implement bulk download functionality.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(BULK_DOWNLOAD_HRL).
-define(BULK_DOWNLOAD_HRL, 1).

% Macros representing messages sent between processes responsible for bulk download
-define(MSG_NEXT_FILE(__FileAttrs, __Pid), {next_file, __FileAttrs, __Pid}).
-define(MSG_DATA_CHUNK(__Chunk, __RetryDelay), {data_chunk, __Chunk, __RetryDelay}).
-define(MSG_RESUMED(__NewConn, __Offset), {resumed, __NewConn, __Offset}).
-define(MSG_CONTINUE(__RetryDelay), {continue, __RetryDelay}).
-define(MSG_DONE, done).
-define(MSG_ERROR, error).
-define(MSG_FINISH, finish).

-define(LOOP_TIMEOUT, timer:seconds(5)).

-endif.